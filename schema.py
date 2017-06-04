import json
import os

from context import Context

__author__ = 'mark'


class SchemaManager:
    db = None
    sfclient = None
    driver = None
    createmap = {}
    createstack = []
    fieldmap = {}
    refs = {}
    sodict = dict()

    def __init__(self, context : Context):
        self.driver = context.dbdriver
        self.sfclient = context.sfclient
        self.storagedir = context.schemadir
        os.makedirs(self.storagedir, exist_ok = True)

    def get_os_tables(self):
        table_list = []
        for root, dirs, files in os.walk(self.storagedir):
            for dir in dirs:
                try:
                    sqlname = os.path.join(self.storagedir, dir, '{}.sql'.format(dir))
                    if os.path.isfile(sqlname):
                        item = {'name': dir}
                        item['exists'] = self.driver.table_exists(dir)
                        table_list.append(item)
                except Exception as ex:
                    print(ex)
        return table_list

    def get_query(self, sobject_name):
        with open(os.path.join(self.storagedir, sobject_name, 'query.soql'), 'r') as queryfile:
            soql = queryfile.read()
            return soql

    def create_tables(self, filterlist = None):
        for root, dirs, files in os.walk(self.storagedir):
            for dir in dirs:
                if not filterlist is None and not dir in filterlist:
                    continue
                try:
                    sqlname = os.path.join(self.storagedir, dir, '{}.sql'.format(dir))
                    if os.path.isfile(sqlname):
                        if not self.driver.table_exists(dir):
                            with open(sqlname, 'r') as f:
                                sql = f.read()
                                print('creating ' + dir)
                                self.driver.exec_dml(sql)
                except Exception as ex:
                    print(ex)

    def drop_tables(self):
        for root, dirs, files in os.walk(self.storagedir):
            for dir in dirs:
                try:
                    if self.driver.table_exists(dir):
                        self.driver.exec_dml('drop table ' + dir)
                except Exception as ex:
                    print(ex)

    def exportSObject(self, names):
        docs = []
        for name in names:
            try:
                doc = self.sfclient.getSobject(name)
                docs.append(doc)
            except Exception as ex:
                print('Unable to retrieve {}, skipping'.format(name))
                print(ex)
                raise ex
        return self.process_sobjects(docs)

    def exportSObjects(self):
        print('loading sobjects')
        solist = self.sfclient.getSobjectList()
        return self.process_sobjects(solist)

    def process_sobjects(self, solist):
        self.sodict = dict([(so['name'], so) for so in solist])

        # first check for new sobjects
        sobject_names = set([so['name'].lower() for so in solist])
        table_names = set([table.tablename for table in self.driver.get_db_tables()])
        new_names = sobject_names - table_names

        #with open(os.path.join(self.storagedir, 'schema-new.sql'), 'w') as ts:
        for new_sobject_name in sorted(new_names):
            new_sobject_name = new_sobject_name.lower()

            os.makedirs(os.path.join(self.storagedir, new_sobject_name), exist_ok=True)

            table_name, fieldlist, sql, select = self.driver.make_create_table(self.sfclient, new_sobject_name)

            parser = self.driver.make_transformer(new_sobject_name, table_name, fieldlist)

            with open(os.path.join(self.storagedir, new_sobject_name, '{}_map.json'.format(new_sobject_name)), 'w') as mapfile:
                json.dump(fieldlist, mapfile, indent=2)

            with open(os.path.join(self.storagedir, new_sobject_name, '{}.sql'.format(new_sobject_name)), 'w') as schemafile:
                schemafile.write(sql)
                schemafile.write(';\n')
                schemafile.write('\n\n')

            with open(os.path.join(self.storagedir, new_sobject_name, '{}_Transform.py'.format(new_sobject_name)), 'w') as parserfile:
                parserfile.write(parser)

            with open(os.path.join(self.storagedir, new_sobject_name, 'query.soql'), 'w') as queryfile:
                queryfile.write(select)

                    #for field in fieldmap:
            #    ts.write('insert into map_drop (sobject_name, table_name, sobject_field, table_field, fieldtype) values (')
            #    ts.write("'{0}',".format(new_sobject_name))
            #    ts.write("'{0}',".format(field['table_name']))
            #    ts.write("'{0}',".format(field['sobject_field']))
            #    ts.write("'{0}',".format(field['db_field']))
            #    ts.write("'{0}');\n".format(field['fieldtype']))

