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
    schema_name = None

    driver_type = "postgresql"

    def connect(self, dbenv: ConfigEnv):
        self.dbenv = dbenv
        dbport = dbenv.dbport if not dbenv.dbport is None and len(dbenv.dbport) > 2 else '5432'
        self.db = psycopg2.connect(
            "dbname='{0}' user='{1}' password='{2}' host='{3}' port='{4}'".format(dbenv.dbname, dbenv.dbuser,
                                                                                  dbenv.dbpass, dbenv.dbhost, dbport))
        self._bucket = 'db_' + dbenv.dbname
        self.storagedir = os.path.join(config.storagedir, 'db', self.dbenv.dbname)
        self.schema_name = dbenv.schema
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
            ddl = 'create table {}.gf_mdata_sync_stats (' + \
                  '  id         serial primary key, ' + \
                  '  table_name text not null, ' + \
                  '  inserts    numeric(8) not null, ' + \
                  '  updates    numeric(8) not null, ' + \
                  '  sync_start timestamp not null default now(), ' + \
                  '  sync_end   timestamp not null default now(), ' + \
                  '  sync_since timestamp not null)'
            ddl = ddl.format(self.schema_name)
            self.exec_dml(ddl)
        if not self.table_exists('gf_mdata_schema_chg'):
            ddl = 'create table {}.gf_mdata_schema_chg (' + \
                  '  id         serial primary key, ' + \
                  '  table_name text not null, ' + \
                  '  col_name   text not null, ' + \
                  '  operation  text not null, ' + \
                  '  date_added timestamp not null default now())'
            ddl = ddl.format(self.schema_name)
            self.exec_dml(ddl)

    def insert_sync_stats(self, table_name, sync_start, sync_end, sync_since, inserts, updates):
        cur = self.cursor
        if sync_since is None:
            sync_since = datetime.datetime(1970,1,1,0,0,0)
        dml = 'insert into {}.gf_mdata_sync_stats (table_name, inserts, updates, sync_start, sync_end, sync_since) ' + \
              'values (%s,%s,%s,%s,%s,%s)'
        cur.execute(dml.format(self.schema_name), [table_name, inserts, updates, sync_start, sync_end, sync_since])
        self.db.commit()
        cur.close()

    def insert_schema_change(self, table_name: string, col_name: string, operation: string):
        cur = self.cursor
        dml = 'insert into {}.gf_mdata_schema_chg (table_name, col_name, operation) values (%s,%s,%s)'
        cur.execute(dml.format(self.schema_name), [table_name, col_name, operation])
        self.db_commit()
        cur.close()

    def bulk_load(self, tablename):
        tablename = tablename.lower()
        if not os.path.isfile('/usr/bin/psql'):
            raise Exception('/usr/bin/psql not found. Please install postgresql client to use bulk loading')

        exportfile = os.path.join(self.storagedir, 'export', tablename + '.exp.gz')
        cmd = r"\copy {} from program 'gzip -dc < {}'".format(self.fq_table(tablename), exportfile)
        cmdargs = ['/usr/bin/psql', '-h', self.dbhost, '-d', self.dbname, '-c', cmd]
        try:
            outputbytes = subprocess.check_output(cmdargs)
            result = outputbytes.decode('utf-8').strip()
            if result.startswith('COPY'):
                return int(result[5:])
        except Exception as ex:
            print(ex)
        return 0

    def upsert(self, cur, table_name, trec: dict, journal=None):
        assert ('Id' in trec)

        cur.execute("select * from {} where id = '{}'".format(self.fq_table(table_name), trec['Id']))
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
            sql = 'insert into {0} ({1}) values ({2});'.format(self.fq_table(table_name), fieldnames, valueplaceholders)
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
                if k in orig_rec:
                    if orig_rec[k] != v:
                        namelist.append(k)
                        data.append(v)
                #
                # !!!!! FIX DATE/DATETIME PROBLEM
                #

            if len(namelist) == 0:
                #
                # This is a legit case. It is probably due to overlapping lastmodifieddate in sync query where nothing
                # actually changed.
                return (False, False)

            assert(not pkey is None)
            sql = 'update {} set '.format(self.fq_table(table_name))
            sql = 'update {} set '.format(self.fq_table(table_name))
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
        sql = "select column_name, data_type, character_maximum_length, ordinal_position " + \
              "from information_schema.columns " + \
              "where table_name = '{}' " + \
              "order by ordinal_position"
        cur.execute(sql.format(table_name))
        columns = cur.fetchall()
        cur.close()
        self.last_table_name = table_name
        self.last_table_fields = dict()
        for c in columns:
            self.last_table_fields[c['column_name']] = {'column_name': c['column_name'], 'data_type': c['data_type'],
                                                        'character_maximum_length': c['character_maximum_length'],
                                                        'ordinal_position': c['ordinal_position']}
        return self.last_table_fields

    def record_count(self, table_name):
        table_cursor = self.db.cursor()
        table_cursor.execute('SELECT count(*) FROM {}.{}'.format(self.schema_name,table_name))
        records = table_cursor.fetchone()
        table_cursor.close()
        return records

    def get_db_tables(self) -> List[GetDbTablesResult]:
        table_cursor = self.db.cursor()
        table_cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=%s ORDER BY table_name",
            (self.schema_name,))
        tables = table_cursor.fetchall()
        table_cursor.close()
        result = [GetDbTablesResult(row[0]) for row in tables]
        return result

    def table_exists(self, table_name):
        table_cursor = self.db.cursor()
        table_cursor.execute(
            "select count(*) from information_schema.tables where table_name = %s and table_schema=%s",
            (table_name, self.schema_name))
        val = table_cursor.fetchone()
        cnt, = val
        return cnt > 0

    def get_db_columns(self, table_name):
        col_cursor = self.new_map_cursor
        col_cursor.execute(
            "select * from information_schema.columns where table_name=%s " + \
            "and table_schema=%s" + \
            "order by column_name", (table_name, self.schema_name))
        cols = col_cursor.fetchall()
        col_cursor.close()
        return cols

    def make_column(self, sobject_name: str, field: dict) -> list:
        """
            returns:
                list(dict(
                    fieldlen, dml, table_name, sobject_name, sobject_field, db_field, fieldtype
                ))
        """
        assert (not sobject_name is None)
        assert (not field is None)
        #        if field is None: return None,None

        sql = ''
        fieldname = field['name']
        fieldtype = field['type']
        fieldlen = field['length']
        if fieldtype in ('picklist', 'multipicklist', 'email', 'phone', 'url'):
            sql += 'varchar ({0}) '.format(fieldlen)
        elif fieldtype in ('string', 'encryptedstring', 'textarea', 'combobox'):
            sql += 'text '
        elif fieldtype == 'datetime':
            sql += 'timestamp '
        elif fieldtype == 'date':
            sql += 'date '
        elif fieldtype == 'time':
            sql += 'time '
        elif fieldtype == 'id':
            sql += 'char(15) primary key '
            # fieldname = 'sfid'
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
            # newfieldlist = []
            # prefix = field['name']
            # if field['name'].endswith('Address'):
            #     prefix = prefix[0:-7]
            # newfieldlist.append({'fieldlen': fieldlen, 'sql': 'text ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'city', 'db_field': prefix+'City', 'fieldtype': 'address'})
            # newfieldlist.append({'fieldlen': fieldlen, 'sql': 'text ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'country', 'db_field': prefix+'Country', 'fieldtype': 'address'})
            # newfieldlist.append({'fieldlen': fieldlen, 'sql': 'text ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'postalCode', 'db_field': prefix+'PostalCode', 'fieldtype': 'address'})
            # newfieldlist.append({'fieldlen': fieldlen, 'sql': 'text ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'state', 'db_field': prefix+'State', 'fieldtype': 'address'})
            # newfieldlist.append({'fieldlen': fieldlen, 'sql': 'text ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'street', 'db_field': prefix+'Street', 'fieldtype': 'address'})
            # newfieldlist.append({'fieldlen': fieldlen, 'sql': 'text ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'longitude', 'db_field': prefix+'Longitude', 'fieldtype': 'address'})
            # newfieldlist.append({'fieldlen': fieldlen, 'sql': 'text ', 'table_name': sobject_name, 'sobject_field': prefix, 'subfield':'latitude', 'db_field': prefix+'Latitude', 'fieldtype': 'address'})
            # return newfieldlist
        else:
            print(field)
            raise Exception('field {0} unknown type {1} for sobject {2}'.format(fieldname, fieldtype, sobject_name))

        newfieldlist = [{'fieldlen': fieldlen, 'dml': sql, 'table_name': sobject_name, 'sobject_field': field['name'],
                         'db_field': fieldname, 'fieldtype': fieldtype}]
        return newfieldlist

    def alter_table_add_columns(self, new_field_defs, sobject_name):
        ddl_template = 'ALTER TABLE {} ADD COLUMN {} {}'
        cur = self.db.cursor()
        newcols = []
        for field in new_field_defs:
            col_def = self.make_column(sobject_name, field)
            if col_def is None:
                #                print('unsupported column type for {} - skipped'.format(field['name']))
                continue
            col = col_def[0]
            ddl = ddl_template.format(self.fq_table(sobject_name), col['db_field'], col['dml'])
            print('adding column {} to {}'.format(col['db_field'], sobject_name))
            cur.execute(ddl)
            newcols.append(col)

            # record change to schema
            sql = 'insert into {}.gf_mdata_schema_chg (table_name, col_name, operation) values (%s,%s,%s)'
            cur.execute(sql.format(self.schema_name), [sobject_name, col['db_field'], 'create'])

        self.db.commit()
        cur.close()
        return newcols

    def alter_table_drop_columns(self, drop_field_names, sobject_name):
        ddl_template = 'ALTER TABLE {} DROP COLUMN {}'
        cur = self.db.cursor()
        for field in drop_field_names:
            ddl = ddl_template.format(self.fq_table(sobject_name), field)
            cur.execute(ddl)

            # record change to schema
            sql = 'insert into {}.gf_mdata_schema_chg (table_name, col_name, operation) values (%s,%s,%s)'
            cur.execute(sql.format(self.schema_name), [sobject_name, field, 'drop'])

        self.db.commit()
        cur.close()

    def maintain_indexes(self, sobject_name, field_defs):
        ddl_template = 'CREATE INDEX IF NOT EXISTS {}_{} ON {} ({})'
        cur = self.db.cursor()
        for field in field_defs:
            if field['externalId'] or field['idLookup']:
                if field['name'].lower() != 'id':  # Id is already set as the pkey
                    ddl = ddl_template.format(sobject_name, field['name'], self.fq_table(sobject_name), field['name'])
                    cur.execute(ddl)
        self.db.commit()
        cur.close()

    def make_create_table(self, fields, sobject_name):
        sobject_name = sobject_name.lower()
        print('new sobject: ' + sobject_name)
        tablecols = []
        fieldlist = []

        for field in fields:
            m = self.make_column(sobject_name, field)
            if m is None:
                continue
            for column in m:
                fieldlist.append(column)
                tablecols.append('  ' + column['db_field'] + ' ' + column['dml'])
        sql = ',\n'.join(tablecols)
        return sobject_name, fieldlist, 'create table {0} ( \n{1} )\n'.format(self.fq_table(sobject_name), sql)

    def make_select_statement(self, field_names, sobject_name):
        select = 'select ' + ','.join(field_names) + ' from ' + sobject_name
        return select

    def getMaxTimestamp(self, tablename):
        col_cursor = self.db.cursor()
        col_cursor.execute('select max(lastmodifieddate) from ' + self.fq_table(tablename))
        stamp, = col_cursor.fetchone()
        col_cursor.close()
        return stamp

    def make_transformer(self, sobject_name, table_name, fieldlist):

        parser = 'from transformutils import id, bl, db, dt, st, ts, inte\n\n'
        parser += 'def parse(rec):\n' + \
                  '  result = dict()\n' + \
                  '  def push(name, value):\n' + \
                  '    result[name] = value\n\n'

        for fieldmap in fieldlist:
            fieldtype = fieldmap['fieldtype']
            fieldname = fieldmap['sobject_field']
            fieldlen = fieldmap['fieldlen']
            dbfield = fieldmap['db_field']
            p_parser = ''
            if fieldtype in (
            'picklist', 'multipicklist', 'string', 'textarea', 'email', 'phone', 'url', 'encryptedstring'):
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
                p_parser = 'push("{}", stsub(rec, "{}", "{}", fieldlen={}))\n'.format(dbfield, fieldname,
                                                                                   fieldmap['subfield'], fieldlen)

            parser += '  ' + p_parser
        parser += '  return result'
        return parser

    def fq_table(self, tablename):
        return '"{}"."{}"'.format(self.schema_name, tablename)
