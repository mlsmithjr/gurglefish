import csv
import gzip
import json
import operator
import os

import sys

import datetime
import yaml
import config
import querytools
import tools
from DriverManager import DbDriverMeta
from context import Context
from schema import SchemaManager

__author__ = 'mark'



def _escape(val):
    val = val.replace('\\', '\\\\')
    val = val.replace('\n', '\\n')
    val = val.replace('\r', '\\r')
    val = val.replace('\t', '\\t')
    return val


class SFExporter:

    def __init__(self, context : Context):
        self.context = context
        self.storagedir = context.filemgr.exportdir
        os.makedirs(self.storagedir, exist_ok=True)


    def sync_tables(self, schema_mgr : SchemaManager, filterlist = None):
        if filterlist:
            filterlist = [name.lower() for name in filterlist]
        tablelist = [table for table in schema_mgr.get_os_tables() if table['exists']]
        if filterlist:
            tablelist = [table for table in tablelist if table['name'] in filterlist]
        for table in tablelist:
            tablename = table['name']
            print('{}:'.format(tablename))
            tstamp = self.context.dbdriver.getMaxTimestamp(tablename)
            self.etl(self.context.filemgr.get_sobject_query(tablename), tablename, timestamp=tstamp)

    def etl(self, soql, sobject_name, timestamp=None, path=None):
        if path is None: path = './'

        dbdriver = self.context.dbdriver

        xlate_handler = self.context.filemgr.load_translate_handler(sobject_name)
        if not timestamp is None:
            soql += " where LastModifiedDate > {}".format(querytools.sfTimestamp(timestamp))
        cur = dbdriver.cursor
        counter = 0
        journal = self.context.filemgr.create_journal(sobject_name)
        try:
            for rec in self.context.sfclient.query(soql):
                del rec['attributes']
                trec = xlate_handler.parse(rec)

                try:
                    dbdriver.upsert(cur, sobject_name, trec, journal)
                except Exception as ex:
                    with open('/tmp/debug.json', 'w') as x:
                       x.write(json.dumps(trec, indent=4, default=tools.json_serial))
                    raise ex

                counter += 1
                if counter % 100 == 0:
                    print('processed {}'.format(counter))
                    dbdriver.commit()
            dbdriver.commit()
            print('processed {}'.format(counter))
        except Exception as ex:
            dbdriver.rollback()
            raise ex
        finally:
            cur.close()
            journal.close()

    def export_copy(self, sobject_name, timestamp=None, path=None):
        if path is None: path = './'

        xlate_handler = self.context.filemgr.load_translate_handler(sobject_name)

        fieldlist = self.context.filemgr.get_sobject_map(sobject_name)
        fieldmap = dict((f['db_field'].lower(), f) for f in fieldlist)

        tablefields = self.context.dbdriver.get_table_fields(sobject_name)
        tablefields = sorted(tablefields.values(), key=operator.itemgetter('ordinal_position'))
        soqlfields = [fm['sobject_field'] for fm in fieldmap.values()]

        soql = 'select {} from {}'.format(','.join(soqlfields), sobject_name)
        if not timestamp is None:
            soql += ' where LastModifiedDate > {0}'.format(querytools.sfTimestamp(timestamp))
        counter = 0
        totalSize = self.context.sfclient.record_count(sobject_name)
        if sys.stdout.isatty():
            print('{}: exporting {} records: 0%'.format(sobject_name, totalSize), end='\r', flush=True)
        else:
            print('{}: exporting {} records'.format(sobject_name, totalSize))
        with gzip.open(os.path.join(self.storagedir, sobject_name + '.exp.gz'), 'wb', compresslevel=9) as export:
            for rec in self.context.sfclient.query(soql):
                trec = xlate_handler.parse(rec)
                parts = []
                for tf in tablefields:
                    n = tf['column_name']
                    f = fieldmap[n]
                    soqlf = f['sobject_field']
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
                                parts.append(_escape(val))
                            else:
                                parts.append(str(val))
                    else:
                        parts.append('\\N')
                export.write(bytes('\t'.join(parts) + '\n', 'utf-8'))
                counter += 1
                if counter % 2000 == 0 and sys.stdout.isatty():
                    print('{}: exporting {} records: {:.0f}%\r'.format(sobject_name, totalSize, (counter / totalSize) * 100), end='\r', flush=True)
            print("\nexported {} records{}".format(counter, ' '*10))


    def exportYAML(self, db, sf, sobject_name, timestamp=None, path=None):
        if path is None: path = './'

        try:
            from yaml import CLoader as Loader, CDumper as Dumper
        except ImportError:
            from yaml import Loader, Dumper

        xlate_handler = self.context.filemgr.load_translate_handler(sobject_name)

        soql = db.get_query(sobject_name)
        if not timestamp is None:
            soql += ' where LastModifiedDate > {0}'.format(querytools.sfTimestamp(timestamp))
        with open(os.path.join(self.storagedir, sobject_name + '.yaml'), 'w') as export:
            for rec in sf.query(soql):
                del rec['attributes']
                trec = xlate_handler.parse(rec)
                export.write('---\n')
                yaml.dump(trec, export, Dumper=Dumper, default_flow_style=True)

    def exportJSON(self, db, sf, sobject_name, timestamp = None, path = None):
        if path is None: path = './'

        soql = db.get_query(sobject_name)
        if not timestamp is None:
            soql += ' where LastModifiedDate > {0}'.format(timestamp)
#        with gzip.open(os.path.join(self.storagedir, sobject_name + '.json.gz'), 'wb') as export:
        with open(os.path.join(self.storagedir, sobject_name + '.json'), 'w') as export:
            export.write('[\n')
            for rec in sf.query(soql):
                del rec['attributes']
                export.write(json.dumps(rec, indent=4))
                export.write(',\n')
            export.write(']\n')

    def exportInsert(self, db, sf, sobject_name, timestamp=None, path=None):
        if path is None: path = './'

        fieldlist = self.context.filemgr.get_sobject_map(sobject_name)

        fieldmap = dict((f['db_field'], f) for f in fieldlist)
        soqlnames = fieldmap.keys()

        handle = self.context.filemgr.load_translate_handler(sobject_name)
        soql = db.get_query(sobject_name)
        if not timestamp is None:
            soql += ' where LastModifiedDate > {0}'.format(querytools.sfTimestamp(timestamp))
            #        with gzip.open(os.path.join(self.storagedir, sobject_name + '.json.gz'), 'wb') as export:
        with open(os.path.join(self.storagedir, sobject_name + '.sql'), 'w') as export:
            for rec in sf.query(soql):
                del rec['attributes']

                trec = handle.parse(rec)
                for n in list(trec.keys()):
                    if n not in soqlnames:
                        print('warn: field skipped - ' + n)
                        continue
                    if trec[n] is None:
                        trec[n] = 'null'
                    else:
                        if isinstance(trec[n], str):
                            fixed = trec[n].replace("'", "''")
                            trec[n] = "'" + fixed + "'"
                        elif isinstance(trec[n], datetime.datetime):
                            trec[n] = "'" + str(trec[n]) + "'"
                names = trec.keys()
                namelist = ','.join(names)
                values = ','.join([str(v) for v in trec.values()])
                export.write('insert into {} ({}) values ({});\n'.format(sobject_name, namelist, values))
            export.write('\n')

    def exportCSV(self, db, sf, sobject_name, timestamp = None, path = None):
        if path is None: path = './'

        handle = self.context.filemgr.load_translate_handler(sobject_name)
        with open(os.path.join(config.storagedir, 'db', self.dbname, 'schema', sobject_name, '{}_map.json'.format(sobject_name)), 'r') as mapfile:
            fieldlist = json.load(mapfile)

        fieldmap = dict((f['db_field'], f) for f in fieldlist)
        fieldnames = sorted(fieldmap.keys())
        soql = db.get_query(sobject_name)
        if not timestamp is None:
            soql += ' where LastModifiedDate > {0}'.format(querytools.sfTimestamp(timestamp))

#        with gzip.open(os.path.join(self.storagedir, sobject_name + '.csv.gz'), 'wb') as export:
        with open(os.path.join(self.storagedir, sobject_name + '.csv'), 'w') as export:
            csvwriter = csv.writer(export, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(fieldnames)
            for rec in sf.query(soql):
                record = []
                trec = handle.parse(rec)
                for fname in fieldnames:
                    if fname in trec:
                        record.append(trec[fname])
                    else:
                        record.append('null')
                csvwriter.writerow(record)
