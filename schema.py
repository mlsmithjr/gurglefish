import logging
from typing import Dict

from context import Context
from objects.files import LocalTableConfig
from salesforce.sfapi import SObjectFields

__author__ = 'mark'


class SFSchemaManager:
    createmap = {}
    createstack = []
    fieldmap = {}
    refs = {}
    sodict = dict()
    log = logging.getLogger('schema')

    def __init__(self, context: Context):
        self.driver = context.dbdriver
        self.sfclient = context.sfclient
        self.filemgr = context.filemgr
        self.storagedir = self.filemgr.schemadir
        self.filters = context.filemgr.get_global_filters() + context.filemgr.get_filters()
        self.context = context

    def inspect(self):
        solist = self.sfclient.get_sobject_list()
        solist = [sobj for sobj in solist if self.accept_sobject(sobj)]
        for so in solist:
            #
            # enrich the data with the package name
            #
            name = so['name']
            pos = name.find('__')
            if pos != -1 and pos < len(name) - 5:
                so['package'] = name[0:pos]
            else:
                so['package'] = 'unpackaged'
        return solist

    def prepare_configured_sobjects(self):
        table_config: [LocalTableConfig] = self.context.filemgr.get_configured_tables()
        table_list = [table.name for table in table_config if table.enabled]
        return self.prepare_sobjects(table_list)

    def prepare_sobjects(self, names):
        docs = []
        for name in names:
            try:
                doc = self.sfclient.get_sobject_definition(name)
                docs.append(doc)
            except Exception as ex:
                print('Unable to retrieve {}, skipping'.format(name))
                print(ex)
                raise ex
        return self._process_sobjects(docs)

    def accept_sobject(self, sobj: Dict) -> bool:
        """
        determine if the named sobject is suitable for exporting

        :param sobj:Name of sobject/table
        :return: True or False
        """
        name = sobj['name']

        if len(self.filters) > 0 and name not in self.filters:
            return False

        if sobj['name'].endswith('_del__c'):
            return False

        if sobj['customSetting'] is True or sobj['replicateable'] is False or sobj['updateable'] is False:
            return False
        if name.endswith('__Tag') or name.endswith('__History') or name.endswith('__Feed'):
            return False
        if name[0:4] == 'Apex' or name in ('scontrol', 'weblink', 'profile'):
            return False
        return True

    def _process_sobjects(self, solist):
        self.sodict = dict([(so['name'], so) for so in solist])
        sobject_names = set([so['name'].lower() for so in solist])
        for new_sobject_name in sorted(sobject_names):
            self._process_sobject(new_sobject_name)

    def create_table(self, sobject_name):
        new_sobject_name = sobject_name.lower()

        fields = self.filemgr.get_sobject_fields(sobject_name)
        if fields is None:
            fields = self.sfclient.get_field_list(new_sobject_name)
            self.filemgr.save_sobject_fields(sobject_name, fields)

        table_name, fieldmap, create_table_dml = self.driver.make_create_table(fields, new_sobject_name)
        select = self.driver.make_select_statement([field['sobject_field'] for field in fieldmap],
                                                   new_sobject_name)

        parser = self.driver.make_transformer(new_sobject_name, table_name, fieldmap)

        self.filemgr.save_sobject_fields(new_sobject_name, fields)
        self.filemgr.save_sobject_transformer(new_sobject_name, parser)
        self.filemgr.save_sobject_map(new_sobject_name, fieldmap)
        self.filemgr.save_sobject_query(new_sobject_name, select)

        #
        # now create the table, if needed
        #

        try:
            if not self.driver.table_exists(sobject_name):
                self.log.info(f'  creating {sobject_name}')
                self.driver.exec_dml(create_table_dml)
                self.log.info(f'  creating indexes')
                self.driver.maintain_indexes(sobject_name, fields)
        except Exception as ex:
            print(ex)

    def _process_sobject(self, sobject_name):
        new_sobject_name = sobject_name.lower()

        fields = self.sfclient.get_field_list(new_sobject_name)
        table_name, fieldmap, create_table_dml = self.driver.make_create_table(fields, new_sobject_name)
        select = self.driver.make_select_statement([field['sobject_field'] for field in fieldmap],
                                                   new_sobject_name)

        parser = self.driver.make_transformer(new_sobject_name, table_name, fieldmap)

        self.filemgr.save_sobject_fields(new_sobject_name, fields)
        self.filemgr.save_sobject_transformer(new_sobject_name, parser)
        self.filemgr.save_sobject_map(new_sobject_name, fieldmap)
        self.filemgr.save_table_create(new_sobject_name, create_table_dml + ';\n\n')
        self.filemgr.save_sobject_query(new_sobject_name, select)

    def update_sobject(self, sobject_name: str, allow_add=True, allow_drop=True):
        sobject_name = sobject_name.lower()

        sobj_columns: SObjectFields = self.sfclient.get_field_list(sobject_name)
        table_columns = self.driver.get_db_columns(sobject_name)

        #
        # check for added/dropped columns
        #
        table_field_names = set([tbl['column_name'] for tbl in table_columns])
        new_fields = sobj_columns.names() - table_field_names
        dropped_fields = table_field_names - sobj_columns.names()

        if len(new_fields) > 0:
            if not allow_add:
                self.log.warning(f'  new column found for {sobject_name}, auto-create disabled, skipping')
            else:
                self.log.info(f'  new columns found, updating table and indexes')
                new_field_defs = [sobj_columns.find(f) for f in new_fields]
                newfieldmap = self.driver.alter_table_add_columns(new_field_defs, sobject_name)
                if len(newfieldmap) > 0:
                    self.driver.maintain_indexes(sobject_name, new_field_defs)

                    fieldmap = self.filemgr.get_sobject_map(sobject_name)
                    fieldmap.extend(newfieldmap)
                    self.filemgr.save_sobject_map(sobject_name, fieldmap)
                    select = self.driver.make_select_statement([field['sobject_field'] for field in fieldmap],
                                                               sobject_name)
                    self.filemgr.save_sobject_query(sobject_name, select)
                    parser = self.driver.make_transformer(sobject_name, sobject_name, fieldmap)
                    self.filemgr.save_sobject_transformer(sobject_name, parser)

                    self.filemgr.save_sobject_fields(sobject_name, [f for f in sobj_columns.values()])

        if len(dropped_fields) > 0:
            if not allow_drop:
                self.log.warning(f'  dropped column detected for {sobject_name}, auto-drop disabled, skipping')
                # do not allow sync until field(s) allowed to be dropped
                return False
            fieldmap = self.filemgr.get_sobject_map(sobject_name)
            newlist = list()
            for item in fieldmap:
                if item['sobject_field'] in dropped_fields:
                    pass
                else:
                    newlist.append(item)
            fieldmap = newlist
            self.log.info(f'  dropped column(s) detected')
            self.driver.alter_table_drop_columns(dropped_fields, sobject_name)
            self.filemgr.save_sobject_map(sobject_name, fieldmap)
            select = self.driver.make_select_statement([field['sobject_field'] for field in fieldmap], sobject_name)
            self.filemgr.save_sobject_query(sobject_name, select)
            parser = self.driver.make_transformer(sobject_name, sobject_name, fieldmap)
            self.filemgr.save_sobject_transformer(sobject_name, parser)

            self.filemgr.save_sobject_fields(sobject_name, [f for f in sobj_columns.values()])

        return True
