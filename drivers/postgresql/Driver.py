import datetime
import gzip
import json
import logging
import operator
import os
import string
import subprocess
import sys
from typing import List, Dict

import psycopg2
import psycopg2.extras
from fastcache import lru_cache

import config
import tools
from DriverManager import DbDriverMeta, GetDbTablesResult
from objects.connections import ConnectionConfig
from context import Context
from objects.sobject import SObjectField, SObjectFields, ColumnMap


class Driver(DbDriverMeta):

    def __init__(self):
        self.driver_type = "postgresql"
        self.dbenv = None
        self.db = None
        self.storagedir = None
        self.log = logging.getLogger('dbdriver')
        self.schema_name = None

    def connect(self, dbenv: ConnectionConfig):
        self.dbenv = dbenv
        dbport = dbenv.dbport if dbenv.dbport is not None and len(dbenv.dbport) > 2 else '5432'
        try:
            self.db = psycopg2.connect(
                "dbname='{0}' user='{1}' password='{2}' host='{3}' port='{4}'".format(dbenv.dbname, dbenv.dbuser,
                                                                                      dbenv.dbpass, dbenv.dbhost,
                                                                                      dbport))
            self.storagedir = os.path.join(config.storagedir, 'db', self.dbenv.id)
            self.schema_name = dbenv.schema
            self.verify_db_setup()
            return True
        except Exception as ex:
            self.log.fatal(f'Unable to log into {dbenv.dbname} at {dbenv.dbhost}:{dbport} for user {dbenv.dbuser}')
            self.log.fatal(ex)
            raise ex

    def exec_ddl(self, ddl: str):
        cur = self.db.cursor()
        cur.execute(ddl)
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
        self.exec_ddl(f'CREATE SCHEMA IF NOT EXISTS {self.schema_name}')
        if not self.table_exists('gf_mdata_sync_stats'):
            ddl = f'create table {self.schema_name}.gf_mdata_sync_stats (' + \
                  '  id         serial primary key, ' + \
                  '  jobid      integer, ' + \
                  '  table_name text not null, ' + \
                  '  inserts    numeric(8) not null, ' + \
                  '  updates    numeric(8) not null, ' + \
                  '  sync_start timestamp not null default now(), ' + \
                  '  sync_end   timestamp not null default now(), ' + \
                  '  sync_since timestamp not null)'
            self.exec_ddl(ddl)
        if not self.table_exists('gf_mdata_schema_chg'):
            ddl = f'create table {self.schema_name}.gf_mdata_schema_chg (' + \
                  '  id         serial primary key, ' + \
                  '  table_name text not null, ' + \
                  '  col_name   text not null, ' + \
                  '  operation  text not null, ' + \
                  '  date_added timestamp not null default now())'
            self.exec_ddl(ddl)
        if not self.table_exists('gf_mdata_sync_jobs'):
            ddl = f'create table {self.schema_name}.gf_mdata_sync_jobs (' + \
                  '  id         serial primary key, ' + \
                  '  date_start timestamp not null default now(),' + \
                  '  date_finish timestamp)'
            self.exec_ddl(ddl)
            self.exec_ddl(f'alter table {self.schema_name}.gf_mdata_sync_stats add constraint ' +
                          'gf_mdata_sync_stats_job_fk foreign key (jobid) references ' +
                          f'{self.schema_name}.gf_mdata_sync_jobs(id) on delete cascade')

    def start_sync_job(self):
        cur = self.cursor
        cur.execute(f'insert into {self.schema_name}.gf_mdata_sync_jobs (date_start) values (%s) returning id',
                    (datetime.datetime.now(),))
        rowid = cur.fetchone()[0]
        cur.close()
        return rowid

    def finish_sync_job(self, jobid):
        cur = self.cursor
        cur.execute(f'update {self.schema_name}.gf_mdata_sync_jobs set date_finish=%s where id=%s',
                    (datetime.datetime.now(), jobid))
        cur.close()

    def insert_sync_stats(self, jobid, table_name, sync_start, sync_end, sync_since, inserts, updates):
        cur = self.cursor
        if sync_since is None:
            sync_since = datetime.datetime(1970, 1, 1, 0, 0, 0)
        dml = f'insert into {self.schema_name}.gf_mdata_sync_stats ' + \
              '(jobid, table_name, inserts, updates, sync_start, ' + \
              'sync_end, sync_since) values (%s,%s,%s,%s,%s,%s,%s)'

        cur.execute(dml, (jobid, table_name, inserts, updates, sync_start, sync_end, sync_since))
        self.db.commit()

    def clean_house(self, date_constraint: datetime):
        cur = self.cursor
        dml = f'delete from {self.schema_name}.gf_mdata_sync_jobs where date_start < %s'
        cur.execute(dml, (date_constraint,))
        self.db.commit()
        cur.close()

    def insert_schema_change(self, table_name: string, col_name: string, operation: string):
        cur = self.cursor
        dml = f'insert into {self.schema_name}.gf_mdata_schema_chg (table_name, col_name, operation) values (%s,%s,%s)'
        cur.execute(dml, [table_name, col_name, operation])
        self.db.commit()
        cur.close()

    def import_native(self, tablename):
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
            self.log.fatal(ex)
        return 0

    def upsert(self, cur, table_name, trec: dict, journal=None):
        assert ('Id' in trec)

        cur.execute("select * from {} where id = '{}'".format(self.fq_table(table_name), trec['Id']))
        tmp_rec = cur.fetchone()
        orig_rec = {}
        index = 0
        if tmp_rec is not None:
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

            valueplaceholders = ','.join('%s' for _ in range(len(data)))
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
                return False, False

            assert pkey is not None
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
        return inserted, updated

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    @lru_cache(maxsize=5, typed=False)
    def get_table_fields(self, table_name: str) -> Dict:
        cur = self.new_map_cursor
        sql = "select column_name, data_type, character_maximum_length, ordinal_position " + \
              "from information_schema.columns " + \
              "where table_name = '{}' " + \
              "order by ordinal_position"
        cur.execute(sql.format(table_name))
        columns = cur.fetchall()
        cur.close()
        table_fields = dict()
        for c in columns:
            table_fields[c['column_name']] = {'column_name': c['column_name'], 'data_type': c['data_type'],
                                              'character_maximum_length': c['character_maximum_length'],
                                              'ordinal_position': c['ordinal_position']}
        return table_fields

    def record_count(self, table_name: str) -> int:
        table_cursor = self.db.cursor()
        table_cursor.execute('SELECT count(*) FROM {}.{}'.format(self.schema_name, table_name))
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

    def table_exists(self, table_name: str) -> bool:
        table_cursor = self.db.cursor()
        table_cursor.execute(
            "select count(*) from information_schema.tables where table_name = %s and table_schema=%s",
            (table_name, self.schema_name))
        val = table_cursor.fetchone()
        cnt, = val
        return cnt > 0

    def get_db_columns(self, table_name: str) -> List:
        col_cursor = self.new_map_cursor
        col_cursor.execute(
            "select * from information_schema.columns where table_name=%s " +
            "and table_schema=%s " +
            "order by column_name", (table_name, self.schema_name))
        cols = col_cursor.fetchall()
        col_cursor.close()
        return cols

    def _make_column(self, sobject_name: str, field: SObjectField) -> [ColumnMap]:
        """
            returns:
                list(dict(
                    fieldlen, dml, table_name, sobject_name, sobject_field, db_field, fieldtype
                ))
        """
        assert (sobject_name is not None)
        assert (field is not None)
        #        if field is None: return None,None

        sql = ''
        fieldname = field.name
        fieldtype = field.get_type
        fieldlen = field.length
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
        elif fieldtype == 'reference':
            sql += 'char(15) '
        elif fieldtype == 'boolean':
            sql += 'boolean '
        elif fieldtype == 'double':
            sql += 'numeric ({0},{1}) '.format(field.precision, field.scale)
            fieldlen = field.precision + field.scale + 1
        elif fieldtype == 'currency':
            sql += 'numeric (18,2) '
        elif fieldtype == 'int':
            sql += 'integer '
            fieldlen = 15
        elif fieldtype == 'percent':
            sql += 'numeric '
            fieldlen = 9
        elif fieldtype in ('base64', 'anyType'):  # not implemented yet <<<<<<
            return []
        elif fieldtype == 'address':
            return []
        else:
            self.log.error(f'field {fieldname} unknown type {fieldtype} for sobject {sobject_name}')
            raise Exception('field {0} unknown type {1} for sobject {2}'.format(fieldname, fieldtype, sobject_name))

        new_list = [ColumnMap.from_parts(fieldlen, sql, sobject_name, field.name, fieldname, fieldtype)]
        return new_list

    def alter_table_add_columns(self, new_field_defs, sobject_name: str) -> [ColumnMap]:
        ddl_template = "ALTER TABLE {} ADD COLUMN {} {}"
        cur = self.db.cursor()
        newcols = []
        for field in new_field_defs:
            col_def: [ColumnMap] = self._make_column(sobject_name, field)
            if len(col_def) == 0:
                continue
            col = col_def[0]
            ddl = ddl_template.format(self.fq_table(sobject_name), col.db_field, col.dml)
            print('    adding column {} to {}'.format(col.db_field, sobject_name))
            cur.execute(ddl)
            newcols.append(col)

            # record change to schema
            sql = f'insert into {self.schema_name}.gf_mdata_schema_chg ' +\
                  '(table_name, col_name, operation) values (%s,%s,%s)'
            cur.execute(sql, [sobject_name, col.db_field, 'create'])

        self.db.commit()
        cur.close()
        return newcols

    def alter_table_drop_columns(self, drop_field_names: [str], sobject_name: str):
        ddl_template = 'ALTER TABLE {} DROP COLUMN {}'
        cur = self.db.cursor()
        for field in drop_field_names:
            print('    dropping column {} from {}'.format(field, sobject_name))
            ddl = ddl_template.format(self.fq_table(sobject_name), field)
            cur.execute(ddl)

            # record change to schema
            sql = 'insert into {}.gf_mdata_schema_chg (table_name, col_name, operation) values (%s,%s,%s)'
            cur.execute(sql.format(self.schema_name), [sobject_name, field, 'drop'])

        self.db.commit()
        cur.close()

    def maintain_indexes(self, sobject_name, field_defs: SObjectFields):
        ddl_template = "CREATE INDEX IF NOT EXISTS {}_{} ON {} ({})"
        cur = self.db.cursor()
        for field in field_defs.values():
            if field.is_externalid or field.is_idlookup:
                if field.name != 'id':  # Id is already set as the pkey
                    ddl = ddl_template.format(sobject_name, field.name, self.fq_table(sobject_name), field.name)
                    cur.execute(ddl)
        self.db.commit()
        cur.close()

    def make_select_statement(self, field_names: [str], sobject_name: str) -> str:
        select = 'select ' + ','.join(field_names) + ' from ' + sobject_name
        return select

    def make_create_table(self, fields: SObjectFields, sobject_name: str) -> (str, [ColumnMap], str):
        sobject_name = sobject_name.lower()
        self.log.info('new sobject: ' + sobject_name)
        tablecols = []
        fieldlist: [ColumnMap] = []

        for field in fields.values():
            m: [ColumnMap] = self._make_column(sobject_name, field)
            if len(m) == 0:
                continue
            for column in m:
                fieldlist.append(column)
                tablecols.append('  ' + column.db_field + ' ' + column.dml)
        sql = ',\n'.join(tablecols)
        return sobject_name, fieldlist, 'create table {0} ( \n{1} )\n'.format(self.fq_table(sobject_name), sql)

    def max_timestamp(self, tablename: str):
        col_cursor = self.db.cursor()
        col_cursor.execute('select max(lastmodifieddate) from ' + self.fq_table(tablename))
        stamp, = col_cursor.fetchone()
        col_cursor.close()
        return stamp

    def make_transformer(self, sobject_name, table_name, fieldlist: [ColumnMap]):
        parser = 'from transformutils import id, bl, db, dt, st, ts, inte\n\n'
        parser += 'def parse(rec):\n' + \
                  '  result = dict()\n\n'
        #                  '  def push(name, value):\n' + \
        #                  '    result[name] = value\n\n'

        for field in fieldlist:
            fieldtype = field.field_type
            fieldname = field.sobject_field
            fieldlen = field.fieldlen
            dbfield = field.db_field
            p_parser = ''
            if fieldtype in ('picklist', 'multipicklist', 'string', 'textarea', 'email', 'phone',
                             'url', 'encryptedstring', 'combobox'):
                p_parser = f'result["{dbfield}"] = st(rec, "{fieldname}", fieldlen={fieldlen})\n'
            elif fieldtype == 'datetime':
                p_parser = f'result["{dbfield}"] = ts(rec, "{fieldname}", fieldlen={fieldlen})\n'
            elif fieldtype == 'date':
                p_parser = f'result["{dbfield}"] = dt(rec, "{fieldname}", fieldlen={fieldlen})\n'
            elif fieldtype == 'time':
                p_parser = f'result["{dbfield}"] = tm(rec, "{fieldname}", fieldlen={fieldlen})\n'
            elif fieldtype in ('id', 'reference'):
                p_parser = f'result["{dbfield}"] = id(rec, "{fieldname}", fieldlen={fieldlen})\n'
            elif fieldtype == 'boolean':
                p_parser = f'result["{dbfield}"] = bl(rec, "{fieldname}", fieldlen={fieldlen})\n'
            elif fieldtype in ('double', 'currency', 'percent'):
                p_parser = f'result["{dbfield}"] = db(rec, "{fieldname}", fieldlen={fieldlen})\n'
            elif fieldtype == 'int':
                p_parser = f'result["{dbfield}"] = inte(rec, "{fieldname}", fieldlen={fieldlen})\n'
            elif fieldtype in ('base64', 'anyType'):  # not implemented yet <<<<<<
                return None
            elif fieldtype == 'address':
                # p_parser = 'result["{}"] = stsub(rec, "{}", "{}", fieldlen={})\n'.format(dbfield, fieldname,
                #                                                                         field['subfield'], fieldlen)
                pass

            parser += '  ' + p_parser
        parser += '  return result'
        return parser

    @lru_cache(maxsize=10, typed=False)
    def fq_table(self, tablename):
        return '"{}"."{}"'.format(self.schema_name, tablename)

    @staticmethod
    def _escape(val):
        if '\\' in val or '\n' in val or '\r' in val or '\t' in val:
            val = val.replace('\\', '\\\\')
            val = val.replace('\n', '\\n')
            val = val.replace('\r', '\\r')
            val = val.replace('\t', '\\t')
        return val

    def format_for_export(self, trec: Dict, tablefields: [Dict], fieldmap: Dict[str, ColumnMap]):
        parts = []
        for tf in tablefields:
            n = tf['column_name']
            f = fieldmap[n]
            soqlf = f.sobject_field
            if soqlf in trec:
                val = trec[soqlf]
                if val is None:
                    parts.append('\\N')
                else:
                    if isinstance(val, bool):
                        parts.append('True' if val else 'False')
                    elif isinstance(val, datetime.datetime):
                        parts.append(val.isoformat())
                    elif isinstance(val, str):
                        parts.append(Driver._escape(val))
                    else:
                        parts.append(str(val))
            else:
                parts.append('\\N')
        return bytes('\t'.join(parts) + '\n', 'utf-8')

    def export_native(self, sobject_name: str, ctx: Context, just_sample=False,
                      timestamp=None):

        sobject_name = sobject_name.lower()

        xlate_handler = ctx.filemgr.load_translate_handler(sobject_name)

        fieldlist: [ColumnMap] = ctx.filemgr.get_sobject_map(sobject_name)
        fieldmap = dict((f.db_field.lower(), f) for f in fieldlist)

        tablefields: Dict = self.get_table_fields(sobject_name)
        tablefields: Dict = sorted(tablefields.values(), key=operator.itemgetter('ordinal_position'))
        soqlfields = [fm.sobject_field for fm in fieldmap.values()]

        soql = 'select {} from {}'.format(','.join(soqlfields), sobject_name)
        if timestamp is not None:
            soql += ' where SystemModStamp > {0}'.format(tools.sf_timestamp(timestamp))
        if just_sample:
            self.log.info('sampling 500 records max')
            soql += ' limit 500'
        counter = 0
        total_size = ctx.sfclient.record_count(sobject_name)
        if sys.stdout.isatty():
            print('{}: exporting {} records: 0%'.format(sobject_name, total_size), end='\r', flush=True)
        else:
            print(f'{sobject_name}: exporting {total_size} records')
        with gzip.open(os.path.join(self.storagedir, sobject_name + '.exp.gz'), 'wb', compresslevel=5) as export:
            for rec in ctx.sfclient.query(soql):
                trec: Dict = xlate_handler.parse(rec)
                record = self.format_for_export(trec, tablefields, fieldmap)
                export.write(record)
                counter += 1
                if counter % 2000 == 0 and sys.stdout.isatty():
                    print('{}: exporting {} records: {:.0f}%\r'.format(sobject_name, total_size,
                                                                       (counter / total_size) * 100), end='\r',
                          flush=True)
            export.close()
            if sys.stdout.isatty():
                print("\nexported {} records{}".format(counter, ' ' * 10))
