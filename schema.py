import json
import os

from context import Context

__author__ = 'mark'


class SchemaManager:
    db = None
    filemgr = None
    sfclient = None
    driver = None
    createmap = {}
    createstack = []
    fieldmap = {}
    refs = {}
    sodict = dict()
    filters = []

    def __init__(self, context : Context):
        self.driver = context.dbdriver
        self.sfclient = context.sfclient
        self.filemgr = context.filemgr
        self.storagedir = self.filemgr.schemadir
        self.filters = context.filemgr.get_global_filters() + context.filemgr.get_filters()

    def inspect(self):
        solist = self.sfclient.getSobjectList()
        solist = [sobj for sobj in solist if self.accept_sobject(sobj)]
        return solist

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

    def accept_sobject(self, sobj:dict) -> bool:
        """
        determine if the named sobject is suitable for exporting

        :param sobj:Name of sobject/table
        :return: True or False
        """

        name = sobj['name']

        if len(self.filters) > 0 and name not in self.filters:
            return False

        if not sobj['custom']:
            if not name in ['Account','Opportunity','User','Contact','Asset','Campaign','CampaignMember','Contract','Lead','RecordType']:
                return False
        if sobj['customSetting'] == True or sobj['replicateable'] == False or sobj['updateable'] == False:
            return False
        if name.endswith('__Tag') or name.endswith('__History') or name.endswith('__Feed'): return False
        if name.find('__') != name.find('__c'):
            return False
        if name[0:4] == 'Apex' or name in ('scontrol','weblink','profile'):
            return False
        return True

    def export_sobjects(self, filter = None):
        print('loading sobjects')
        solist = self.sfclient.getSobjectList()
        if filter:
            solist = [sobj for sobj in solist if sobj in filter]
        else:
            solist = [sobj for sobj in solist if self.accept_sobject(sobj)]
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

            fields = self.sfclient.getFieldList(new_sobject_name)
            table_name, fieldmap, sql, select = self.driver.make_create_table(fields, new_sobject_name)
            parser = self.driver.make_transformer(new_sobject_name, table_name, fieldmap)

            self.filemgr.save_sobject_fields(new_sobject_name, fields)
            self.filemgr.save_sobject_transformer(new_sobject_name, parser)
            self.filemgr.save_sobject_map(new_sobject_name, fieldmap)
            self.filemgr.save_table_create(new_sobject_name, sql + ';\n\n')
            self.filemgr.save_sobject_query(new_sobject_name, select)

