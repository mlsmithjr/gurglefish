#    Copyright 2018, 2019 Marshall L Smith Jr
#
#    This file is part of Gurglefish.
#
#    Gurglefish is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Gurglefish is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Gurglefish.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import gzip
import json
import logging
import operator
import os
import string
import sys
from typing import List, Dict

import psycopg2
import psycopg2.extras
from fastcache import lru_cache
from psycopg2._psycopg import connection, cursor

from gurglefish import FileManager
from gurglefish import config
from gurglefish import tools
from gurglefish.DriverManager import DbDriverMeta, GetDbTablesResult, DbNativeExporter
from gurglefish.objects.connections import ConnectionConfig
from gurglefish.context import Context
from gurglefish.objects.sobject import SObjectField, SObjectFields, ColumnMap


class NativeExporter(DbNativeExporter):

    def __init__(self, sobject: str, db: DbDriverMeta, filemgr: FileManager, just_sample=False, timestamp=None):
        self.sobject_name = sobject.lower()
        self.dbdriver = db
        self.query = None
        self.export_file = None
        self.tablefields = None
        self.xlate_handler = filemgr.load_translate_handler(self.sobject_name)
        self.log = logging.getLogger('exporter')

        fieldlist: [ColumnMap] = filemgr.get_sobject_map(self.sobject_name)
        self.fieldmap = dict((f.db_field.lower(), f) for f in fieldlist)

        self.tablefields: Dict = self.dbdriver.get_table_fields(self.sobject_name)
        self.tablefields: List = sorted(self.tablefields.values(), key=operator.itemgetter('ordinal_position'))
        soqlfields = [fm.sobject_field for fm in self.fieldmap.values()]

        self.query = 'select {} from {}'.format(','.join(soqlfields), self.sobject_name)
        if timestamp is not None:
            self.query += ' where SystemModStamp > {0}'.format(tools.sf_timestamp(timestamp))
        if just_sample:
            self.log.info('sampling 500 records max')
            self.query += ' limit 500'
        self.counter = 0
        self.export_file = gzip.open(os.path.join(filemgr.exportdir, self.sobject_name + '.exp.gz'), 'wb', compresslevel=6)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.export_file is not None:
            self.export_file.close()
            self.export_file = None

    def soql(self) -> str:
        return self.query

    def write(self, rec: Dict):
        transformed: Dict = self.xlate_handler.parse(rec)
        record = NativeExporter.format_for_export(transformed, self.tablefields, self.fieldmap)
        self.export_file.write(record)
        self.counter += 1

    @staticmethod
    def format_for_export(trec: Dict, tablefields: [Dict], fieldmap: Dict[str, ColumnMap]):
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
                        parts.append(NativeExporter._escape(val))
                    else:
                        parts.append(str(val))
            else:
                parts.append('\\N')
        return bytes('\t'.join(parts) + '\n', 'utf-8')

    @staticmethod
    def _escape(val):
        if '\\' in val or '\n' in val or '\r' in val or '\t' in val:
            val = val.replace('\\', '\\\\')
            val = val.replace('\n', '\\n')
            val = val.replace('\r', '\\r')
            val = val.replace('\t', '\\t')
        return val

    def close(self):
        self.export_file.close()
        if sys.stdout.isatty():
            print("\nexported {} records{}".format(self.counter, ' ' * 10))


class Driver(DbDriverMeta):

    def __init__(self):
        self.driver_type = "postgresql"
        self.dbenv = None
        self.db: connection = None
        self.storagedir = None
        self.log = logging.getLogger('dbdriver')
        self.schema_name = None

    def connect(self, dbenv: ConnectionConfig):
        self.dbenv = dbenv
        dbport = dbenv.dbport if dbenv.dbport is not None and len(dbenv.dbport) > 2 else '5432'
        try:
            self.db: connection = psycopg2.connect(
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
    def cursor(self) -> cursor:
        return self.db.cursor()

    def close(self):
        self.db.close()

    def verify_db_setup(self):
        self.exec_ddl(f'CREATE SCHEMA IF NOT EXISTS {self.schema_name}')
        if not self.table_exists('gf_mdata_sync_stats'):
            ddl = f'create table {self.schema_name}.gf_mdata_sync_stats (' + \
                  '  id         serial primary key, ' + \
                  '  jobid      integer, ' + \
                  '  table_name text not null, ' + \
                  '  inserts    numeric(8) not null, ' + \
                  '  updates    numeric(8) not null, ' + \
                  '  deletes    numeric(8) not null, ' + \
                  '  api_calls  numeric(8) not null, ' + \
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
        self.db.commit()
        return rowid

    def finish_sync_job(self, jobid):
        cur = self.cursor
        cur.execute(f'update {self.schema_name}.gf_mdata_sync_jobs set date_finish=%s where id=%s',
                    (datetime.datetime.now(), jobid))
        cur.close()

    def insert_sync_stats(self, jobid, table_name, sync_start, sync_end, sync_since, inserts, updates, deletes, api_calls):
        cur = self.cursor
        if sync_since is None:
            sync_since = datetime.datetime(1970, 1, 1, 0, 0, 0)
        dml = f'insert into {self.schema_name}.gf_mdata_sync_stats ' + \
              '(jobid, table_name, inserts, updates, deletes, sync_start, ' + \
              'sync_end, sync_since, api_calls) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        cur.execute(dml, (jobid, table_name, inserts, updates, deletes, sync_start, sync_end, sync_since, api_calls))
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

        exportfile = os.path.join(self.storagedir, 'export', tablename + '.exp.gz')
        with self.cursor as cur:
            with gzip.open(exportfile, 'rb') as infile:
                cur.copy_from(infile, tablename)
            self.db.commit()

    def export_native(self, table_name, output_path):
        table_name = table_name.lower()
        with self.cursor as cur:
            with gzip.open(output_path, 'wb', compresslevel=6) as outfile:
                cur.copy_to(outfile, table_name)

    def delete(self, cur, table_name: str, key: str):
        table_name = self.fq_table(table_name)
        try:
            if cur.execute(f"SELECT id from {table_name} where id=%s", [key]) is None:
                return 0
            cur.execute(f'delete from {table_name} where Id=%s', [key])
            return 1
        except Exception as ex:
            self.log.error(f'Deleting record {key} from {table_name}')
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

    def dump_ids(self, table_name: str, output_filename: str):
        cur = self.cursor
        sql = f'select id from {self.schema_name}.{table_name} order by id'
        cur.execute(sql)
        with open(output_filename, 'w') as out:
            for rec in cur:
                out.write(rec[0] + '\n')
        cur.close()

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
            # nothing to handle, this is just an aggregation of fields already exposed for syncing
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
            if field.get_type == 'address':
                continue
            col_def: [ColumnMap] = self._make_column(sobject_name, field)
            if len(col_def) == 0:
                continue
            col = col_def[0]
            ddl = ddl_template.format(self.fq_table(sobject_name), col.db_field, col.dml)
            print('    adding column {} to {}'.format(col.db_field, self.fq_table(sobject_name)))
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
            self.log.info('  dropping column {} from {}'.format(field, sobject_name))
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
            if field.is_externalid or field.is_idlookup or field.name == 'SystemModStamp':
                if field.name != 'id':  # Id is already set as the pkey
                    ddl = ddl_template.format(sobject_name, field.name, self.fq_table(sobject_name), field.name)
                    cur.execute(ddl)
                    self.log.info(f'  created index {sobject_name}_{field.name}')
        self.db.commit()
        cur.close()

    def make_select_statement(self, field_names: [str], sobject_name: str) -> str:
        select = 'select ' + ',\n'.join(field_names) + ' from ' + sobject_name
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
        col_cursor.execute('select max(SystemModStamp) from ' + self.fq_table(tablename))
        stamp, = col_cursor.fetchone()
        col_cursor.close()
        return stamp

    def make_transformer(self, sobject_name, table_name, fieldlist: [ColumnMap]):
        parser = 'from gurglefish.transformutils import id, bl, db, dt, st, ts, inte\n\n'
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

    def create_exporter(self, sobject_name: str, ctx: Context, just_sample=False, timestamp=None) -> DbNativeExporter:
        exporter = NativeExporter(sobject_name, self, ctx.filemgr, just_sample, timestamp)
        return exporter
