import csv
import gzip
import json

import datetime
#import yaml
import os
import config
from DriverManager import DbDriverMeta
from schema import SFSchemaManager

__author__ = 'mark'


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial
    return str(obj)
    raise TypeError("Type not serializable")


class SFImporter:
    context = None

    def __init__(self, context, schema_mgr : SFSchemaManager):
        self.context = context
        self.storagedir = context.filemgr.exportdir
        self.schema_mgr = schema_mgr

    def bulk_load(self, sobject_name):

        if not self.context.dbdriver.table_exists(sobject_name):
            self.schema_mgr.create_table(sobject_name)

        return self.context.dbdriver.bulk_load(sobject_name)


    def import_yaml(self, db : DbDriverMeta, sobject_name, path = None):
        if path is None: path = './'

        try:
            from yaml import CLoader as Loader, CDumper as Dumper
        except ImportError:
            from yaml import Loader, Dumper

        #tablefields = db.get_table_fields(sobject_name)
        counter = 0
        with open(os.path.join(self.context.filemgr.exportdir, sobject_name + '.yaml'), 'r') as yamlfile:
            cur = db.cursor
            for rec in yaml.load_all(yamlfile, Loader=Loader):
                namelist = []
                sqlrec = []
                for k,v in rec.items():
                    #length = tablefields[k.lower()]['character_maximum_length']
                    #if isinstance(v, str) and len(v) > length:
                    #    print('value too large!')

                    namelist.append(k)
                    sqlrec.append(v)
                fieldnames = ','.join(namelist)
                valuelist = ','.join('%s' for i in range(len(sqlrec)))
                sql = 'insert into "{0}" ({1}) values ({2});\n'.format(sobject_name, fieldnames, valuelist)

                #dbg = dict(zip(namelist, sqlrec))
                #with open('/tmp/debug.json', 'w') as x:
                #    x.write(json.dumps(dbg, indent=4, default=json_serial))

                if counter > 11100:
                    cur.execute(sql, sqlrec)
                counter += 1
                if counter % 100 == 0:
                    print('inserted {}'.format(counter))
                    db.db.commit()
                    if counter > 11100: print(counter)
            db.db.commit()
            cur.close()


    def import_csv(self, db, sobject_name, path = None):
        if path is None: path = './'
        table_name, fieldlist = db.get_field_map(sobject_name)
#        with gzip.open(os.path.join(self.storagedir, sobject_name + '.csv.gz'), 'rb') as csvfile:
        with open(os.path.join(self.context.filemgr.exportdir, sobject_name + '.csv'), 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            fieldnames = next(csvreader)
            cur = db.db.cursor
            for row in csvreader:
                newrow = []
                namelist = []
                for i in range(len(fieldnames)):
                    if not row[i] is None and len(row[i]) > 0:
                        newrow.append(row[i])
                        namelist.append(fieldnames[i])
                fieldnamelist = ','.join(namelist)
                valuelist = ','.join('%s' for i in range(len(newrow)))
                sql = 'insert into "{0}" ({1}) values ({2});\n'.format(table_name, fieldnamelist, valuelist)
                print(sql)
                cur.execute(sql, newrow)
            db.db.commit()
            cur.close()