import json
import subprocess
import os
import string
from typing import List
import datetime

import psycopg2
import psycopg2.extras

import config
import tools
from DriverManager import DbDriverMeta, GetDbTablesResult, GetMappedTablesResult, FieldMapResult
from db.Capture import CaptureManager
from db.mdatadb import ConfigEnv


class Driver(DbDriverMeta):
    last_table_name = None
    last_table_fields = None

    driver_type = "postgresql"

    def connect(self, dbenv: ConfigEnv):
        self.dbenv = dbenv
        dbport = dbenv.dbport if not dbenv.dbport is None and len(dbenv.dbport) > 2 else '5432'
        self.db = psycopg2.connect("dbname='{0}' user='{1}' password='{2}' host='{3}' port='{4}'".format(dbenv.dbname, dbenv.dbuser, dbenv.dbpass, dbenv.dbhost, dbport))
        self._bucket = 'db_' + dbenv.dbname
        self.storagedir = os.path.join(config.storagedir, 'db', self.dbenv.dbname)
        self.verify_db_setup()
        return True

    def exec_dml(self, dml):
        cur = self.db.cursor()
        cur.execute(dml)
        self.db.commit()
        cur.close()

    @property
    def dbhost(self):
        return self.dbenv.dbhost

    @property
    def dbport(self):
        return self.dbenv.dbport

    @property
    def dbname(self):
        return self.dbenv.dbname

    @property
    def new_map_cursor(self):
        return self.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    @property
    def cursor(self):
        return self.db.cursor()

    def verify_db_setup(self):
        if not self.table_exists('gf_mdata_sync_stats'):
            dml =   'create table gf_mdata_sync_stats (' +\
                    '  id         serial primary key, '+\
                    '  table_name varchar(50) not null, '+\
                    '  inserts    numeric(8) not null, '+\
                    '  updates    numeric(8) not null, '+\
                    '  sync_start timestamp not null default now(), '+\
                    '  sync_end   timestamp not null default now(), '+\
                    '  sync_since timestamp not null)'
            self.exec_dml(dml)

    def insert_sync_stats(self, table_name, sync_start, sync_end, sync_since, inserts, updates):
        cur = self.cursor
        if not sync_since:
            sync_since = datetime.date(year=1970, month=1, day=1)
        cur.execute('insert into gf_mdata_sync_stats (table_name, inserts, updates, sync_start, sync_end, sync_since) '+\
                    'values (%s,%s,%s,%s,%s,%s)', [table_name, inserts, updates, sync_start, sync_end, sync_since])
        self.db.commit()
        cur.close()

    def bulk_load(self, tablename):
        if not os.path.isfile('/usr/bin/psql'):
            raise Exception('psql not found. Please install postgresql client to use bulk loading')

        exportfile = os.path.join(self.storagedir, 'export', tablename + '.exp.gz')
        cmd = r"\copy {} from program 'gzip -dc < {}'".format('"' + tablename + '"', exportfile)
        cmdargs = ['/usr/bin/psql', '-h', self.dbhost, '-d', self.dbname, '-c', cmd]
        try:
            outputbytes = subprocess.check_output(cmdargs)
            result = outputbytes.decode('utf-8').strip()
            if result.startswith('COPY'):
                return int(result[5:])
        except Exception as ex:
            print(ex)
        return 0

    def upsert(self, cur, table_name, trec : dict, journal = None):
        assert('Id' in trec)

        cur.execute("select * from \"{}\" where id = '{}'".format(table_name, trec['Id']))
        tmp_rec = cur.fetchone()
        orig_rec = {}
        index = 0
        if not tmp_rec is None:
            for col in cur.description:
                orig_rec[col[0]] = tmp_rec[index]
                index += 1

        namelist = []
        data = []

        inserted = False
        updated = False

        if len(orig_rec) == 0:
            table_fields = self.get_table_fields(table_name)
            existing_field_names = table_fields.keys()
            for k, v in trec.items():
                k = k.lower()
                if k in existing_field_names:
                    namelist.append(k)
                    data.append(v)

            valueplaceholders = ','.join('%s' for i in range(len(data)))
            fieldnames = ','.join(namelist)
            sql = 'insert into "{0}" ({1}) values ({2});'.format(table_name, fieldnames, valueplaceholders)
            if journal:
                journal.write(bytes('i:{} --> {}\n'.format(sql, json.dumps(data, default=tools.json_serial)), 'utf-8'))
            cur.execute(sql, data)
            inserted = True
        else:
            #
            # use only the changed field values
            #
            pkey = None
            for k, v in trec.items():
                k = k.lower()
                if k == 'id':
                    pkey = v
                    continue
                if k in orig_rec and orig_rec[k] != v:
                    namelist.append(k)
                    data.append(v)
                #
                # !!!!! FIX DATE/DATETIME PROBLEM
                #

            assert(pkey != None)
            sql = 'update "{}" set '.format(table_name)
            sql = 'update "{}" set '.format(table_name)
            sets = []
            for name in namelist:
                sets.append(name + r'=%s')
            sql += ','.join(sets)
            sql += " where id = '{}'".format(pkey)
            if journal:
                journal.write(bytes('u:{} --> {}\n'.format(sql, json.dumps(data, default=tools.json_serial)), 'utf-8'))
            cur.execute(sql, data)
            updated = True
        return (inserted, updated)

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def get_table_fields(self, table_name):
        if table_name == self.last_table_name:
            return self.last_table_fields
        cur = self.new_map_cursor
        sql =   "select column_name, data_type, character_maximum_length, ordinal_position "+\
                "from information_schema.columns "+\
                "where table_name = '{}' "+\
                "order by ordinal_position"
        cur.execute(sql.format(table_name))
        columns = cur.fetchall()
        cur.close()
        self.last_table_name = table_name
        self.last_table_fields = dict()
        for c in columns:
            self.last_table_fields[c['column_name']] = { 'column_name': c['column_name'], 'data_type': c['data_type'], 'character_maximum_length': c['character_maximum_length'], 'ordinal_position': c['ordinal_position']}
        return self.last_table_fields

    def get_db_tables(self) -> List[GetDbTablesResult]:
        table_cursor = self.db.cursor()
        table_cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
        tables = table_cursor.fetchall()
        table_cursor.close()
        result = [GetDbTablesResult(row[0]) for row in tables]
        return result

    def table_exists(self, table_name):
        table_cursor = self.db.cursor()
        table_cursor.execute(
            "select count(*) from information_schema.tables where table_name = %s and table_schema='public'",
            (table_name,))
        val = table_cursor.fetchone()
        cnt, = val
        return cnt > 0

    def get_mapped_tables(self) -> List[GetMappedTablesResult]:
        cur = self.new_map_cursor
        cur.execute('select table_name, sobject_name from table_map order by sobject_name')
        tables = cur.fetchall()
        cur.close()
        result = [GetMappedTablesResult(table) for table in tables]
        return result

    def get_field_map(self, sobject_name : string):
        if CaptureManager.exists(self._bucket, sobject_name):
            stuff = CaptureManager.fetch(self._bucket, sobject_name)
            return stuff['table_name'], stuff['payload']
        cur = self.new_map_cursor
        cur.execute('select id, table_name from table_map where sobject_name = %s', (sobject_name,))
        table_map = cur.fetchone()
        if table_map is None: return None, None
        cur.execute('select * from field_map where table_map = %s', (table_map['id'],))
        fields = cur.fetchall()
        cur.close()
        CaptureManager.save(self._bucket, sobject_name, { 'table_name':table_map['table_name'], 'payload':fields})
        result = [FieldMapResult(field) for field in fields]
        return table_map['table_name'], result

    def is_table_mapped(self, sobject) -> bool:
        cur = self.new_map_cursor
        cur.execute('select table_name from table_map where sobject_name = %s', (sobject,))
        found = cur.fetchone() is None
        cur.close()
        return found

    def add_column(self, sobject_name:string, fielddef:dict):
        (sql, d) = self.make_column(sobject_name, fielddef)
        self.add_mapped_field(sobject_name, d['table_name'], d['sobject_field'], d['db_field'], d['fieldtype'])

    def add_mapped_field(self, sobject_name, table_name, sobject_field, db_field, fieldtype):
        stmt = "insert into map_drop (sobject_name, table_name, sobject_field, table_field, fieldtype) values ('{0}','{1}','{2}','{3}','{4}')"
        stmt = stmt.format(sobject_name, table_name, sobject_field, db_field, fieldtype)
        cur = self.db.cursor()
        cur.execute(stmt)
        self.db.commit()
        cur.close()

    def get_db_columns(self, table_name):
        col_cursor = self.new_map_cursor
        col_cursor.execute(
            "select * from information_schema.columns where table_name=%s " + \
            "and table_schema='public' " + \
            "order by column_name", (table_name,))
        cols = col_cursor.fetchall()
        col_cursor.close()
        return cols

    def drop_column(self, table_name, column_name):
        # drop the field mappings
        # drop the column
        print('TODO: drop_column')
        print('dropped ' + table_name + '.' + column_name)

    def drop_table(self, table_name):
        # drop the field mappings
        # drop the table
        print('TODO: drop_table')
        print('dropped ' + table_name)

    def make_column(self, sobject_name:str, field:dict) -> dict:
        """
            returns:
                list(dict(
                    fieldlen, dml, table_name, sobject_name, sobject_field, db_field, fieldtype
                ))
        """
        assert(sobject_name != None)
        assert(field != None)
#        if field is None: return None,None

        sql = ''
        fieldname = field['name']
        fieldtype = field['type']
        fieldlen = field['length']
        if fieldtype in (
        'picklist', 'multipicklist', 'string', 'textarea', 'email', 'phone', 'url', 'encryptedstring'):
            sql += 'varchar ({0}) '.format(fieldlen)
        elif fieldtype == 'combobox':
            sql += 'varchar (200) '
        elif fieldtype == 'datetime':
            sql += 'timestamp '
        elif fieldtype == 'date':
            sql += 'date '
        elif fieldtype == 'time':
            sql += 'time '
        elif fieldtype == 'id':
            sql += 'char(15) primary key '
            #fieldname = 'sfid'
        elif fieldtype == 'reference':
            # refto = field['referenceTo'][0]
            sql += 'char(15) '
            # if not refto in table_names and refto != sobject_name:
            #    self.create_table(refto)
            # self.refs[sobject_name].append('alter table {0} add column {1} char(15) references {2}(sfid)'.format(sobject_name, fieldname, refto))
            # continue
        elif fieldtype == 'boolean':
            sql += 'boolean '
        elif fieldtype == 'double':
            sql += 'numeric ({0},{1}) '.format(field['precision'], field['scale'])
            fieldlen = field['precision'] + field['scale'] + 1
        elif fieldtype == 'currency':
            sql += 'numeric (18,2) '
        elif fieldtype == 'int':
            sql += 'integer '
            fieldlen = 15
        elif fieldtype == 'percent':
            sql += 'numeric '
            fieldlen = 9
        elif fieldtype in ('base64', 'anyType'):  ##### not implemented yet <<<<<<
            return None
        elif fieldtype == 'address':
            return None
            #
            # this is a weird exception.  Handle differently
            #
            newfieldlist = []
            prefix = field['name']
            if field['name'].endswith('Address'):
                prefix = prefix[0:-7]
            newfieldlist.append({'fieldlen': fieldlen, 'sql': 'varchar(200) ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'city', 'db_field': prefix+'City', 'fieldtype': 'address'})
            newfieldlist.append({'fieldlen': fieldlen, 'sql': 'varchar(50) ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'country', 'db_field': prefix+'Country', 'fieldtype': 'address'})
            newfieldlist.append({'fieldlen': fieldlen, 'sql': 'varchar(20) ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'postalCode', 'db_field': prefix+'PostalCode', 'fieldtype': 'address'})
            newfieldlist.append({'fieldlen': fieldlen, 'sql': 'varchar(100) ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'state', 'db_field': prefix+'State', 'fieldtype': 'address'})
            newfieldlist.append({'fieldlen': fieldlen, 'sql': 'varchar(200) ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'street', 'db_field': prefix+'Street', 'fieldtype': 'address'})
            newfieldlist.append({'fieldlen': fieldlen, 'sql': 'varchar(20) ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'longitude', 'db_field': prefix+'Longitude', 'fieldtype': 'address'})
            newfieldlist.append({'fieldlen': fieldlen, 'sql': 'varchar(20) ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'latitude', 'db_field': prefix+'Latitude', 'fieldtype': 'address'})
            return newfieldlist
        else:
            print(field)
            raise Exception('field {0} unknown type {1} for sobject {2}'.format(fieldname, fieldtype, sobject_name))

        newfieldlist = [{'fieldlen': fieldlen, 'dml': sql, 'table_name': sobject_name, 'sobject_field': field['name'], 'db_field': fieldname, 'fieldtype': fieldtype}]
        return newfieldlist

    def alter_table_add_columns(self, new_field_defs, sobject_name):
        ddl_template = 'ALTER TABLE "{}" ADD COLUMN {} {}'
        cur = self.db.cursor()
        for field in new_field_defs:
            col_def = self.make_column(sobject_name, field)
            col = col_def[0]
            ddl = ddl_template.format(sobject_name, col['db_field'], col['dml'])
            cur.execute(ddl)
        self.db.commit()
        cur.close()

    def alter_table_drop_columns(self, drop_field_names, sobject_name):
        ddl_template = 'ALTER TABLE "{}" DROP COLUMN {}'
        cur = self.db.cursor()
        for field in drop_field_names:
            ddl = ddl_template.format(sobject_name, field)
            cur.execute(ddl)
        self.db.commit()
        cur.close()

    def maintain_indexes(self, sobject_name, field_defs):
        ddl_template = 'CREATE INDEX IF NOT EXISTS {}_{} ON "{}" ({})'
        cur = self.db.cursor()
        for field in field_defs:
            if field['externalId'] or field['idLookup']:
                ddl = ddl_template.format(sobject_name, field['name'], sobject_name, field['name'])
                cur.execute(ddl)
        self.db.commit()
        cur.close()


    def make_create_table(self, fields, sobject_name):
        #if self.table_exists(sobject_name) or sobject_name in self.createstack:
        #    return

        #self.createstack.append(sobject_name)
        sobject_name = sobject_name.lower()
        print('new sobject: ' + sobject_name)
        tablecols = []
        fieldlist = []
        #self.refs[sobject_name] = []

        for field in fields:
            m = self.make_column(sobject_name, field)
            if m is None:
                continue
            for column in m:
                fieldlist.append(column)
                tablecols.append('  ' + column['db_field'] + ' ' + column['dml'])
        sql = ',\n'.join(tablecols)
        return sobject_name, fieldlist, 'create table "{0}" ( \n{1} )\n'.format(sobject_name, sql)

    def make_select_statement(self, field_names, sobject_name):
        select = 'select ' + ','.join(field_names) + ' from ' + sobject_name
        return select

    def getMaxTimestamp(self, tablename):
        col_cursor = self.db.cursor()
        col_cursor.execute('select max(lastmodifieddate) from "' + tablename + '"')
        stamp, = col_cursor.fetchone()
        col_cursor.close()
        return stamp

    def make_transformer(self, sobject_name, table_name, fieldlist):

        parser = 'from transformutils import id, bl, db, dt, st, ts, inte\n\n'
        parser +=   'def parse(rec):\n' +\
                    '  result = dict()\n' +\
                    '  def push(name, value):\n' +\
                    '    result[name] = value\n\n'


        for fieldmap in fieldlist:
            fieldtype = fieldmap['fieldtype']
            fieldname = fieldmap['sobject_field']
            fieldlen = fieldmap['fieldlen']
            dbfield = fieldmap['db_field']
            p_parser = ''
            if fieldtype in ('picklist', 'multipicklist', 'string', 'textarea', 'email', 'phone', 'url', 'encryptedstring'):
                p_parser = 'push("{}", st(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'combobox':
                p_parser = 'push("{}", st(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'datetime':
                p_parser = 'push("{}", ts(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'date':
                p_parser = 'push("{}", dt(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'time':
                p_parser = 'push("{}", tm(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'id':
                p_parser = 'push("{}", id(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'reference':
                p_parser = 'push("{}", id(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'boolean':
                p_parser = 'push("{}", bl(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'double':
                p_parser = 'push("{}", db(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'currency':
                p_parser = 'push("{}", db(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'int':
                p_parser = 'push("{}", inte(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype == 'percent':
                p_parser = 'push("{}", db(rec, "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldlen)
            elif fieldtype in ('base64', 'anyType'):  ##### not implemented yet <<<<<<
                return None
            elif fieldtype == 'address':
                p_parser = 'push("{}", st(rec, "{}", "{}", fieldlen={}))\n'.format(dbfield, fieldname, fieldmap['subfield'], fieldlen)

            parser += '  ' + p_parser
        parser += '  return result'
        return parser
