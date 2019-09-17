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
import logging
from typing import Dict

from gurglefish import FileManager
from gurglefish.DriverManager import DbDriverMeta
from gurglefish.context import Context
from gurglefish.objects.files import LocalTableConfig
from gurglefish.objects.sobject import ColumnMap
from gurglefish.sfapi import SObjectFields, SFClient
from gurglefish.tools import make_arg_list

__author__ = 'mark'


class SFSchemaManager:

    def __init__(self, context: Context):
        self.filters = context.filemgr.get_global_filters() + context.filemgr.get_filters()
        self.context = context
        self.log = logging.getLogger("schema")

    @property
    def driver(self) -> DbDriverMeta:
        return self.context.driver

    @property
    def sfclient(self) -> SFClient:
        return self.context.sfclient

    @property
    def filemgr(self) -> FileManager:
        return self.context.filemgr

    @property
    def storagedir(self) -> str:
        return self.context.filemgr.schemadir

    def inspect(self) -> [Dict]:
        solist: [Dict] = self.sfclient.get_sobject_list()
        solist: [Dict] = [sobj for sobj in solist if self.accept_sobject(sobj)]
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
            self.create_table(new_sobject_name)

    def create_table(self, sobject_name: str):
        new_sobject_name = sobject_name.lower()

        fields: SObjectFields = self.filemgr.get_sobject_fields(sobject_name)
        if fields is None:
            fields: SObjectFields = self.sfclient.get_field_list(new_sobject_name)
            self.filemgr.save_sobject_fields(sobject_name, fields)

        table_name, fieldlist, create_table_dml = self.driver.make_create_table(fields, new_sobject_name)
        select = self.driver.make_select_statement([field.sobject_field for field in fieldlist],
                                                   new_sobject_name)

        parser = self.driver.make_transformer(new_sobject_name, table_name, fieldlist)

        self.filemgr.save_sobject_fields(new_sobject_name, fields)
        self.filemgr.save_sobject_transformer(new_sobject_name, parser)
        self.filemgr.save_sobject_map(new_sobject_name, fieldlist)
        self.filemgr.save_table_create(new_sobject_name, create_table_dml + ';\n\n')
        self.filemgr.save_sobject_query(new_sobject_name, select)

        #
        # now create the table, if needed
        #

        try:
            if not self.driver.table_exists(sobject_name):
                self.log.info(f'  creating {sobject_name}')
                self.driver.exec_ddl(create_table_dml)
                self.log.info(f'  creating indexes')
                self.driver.maintain_indexes(sobject_name, fields)
        except Exception as ex:
            print(ex)
            raise ex

    def update_sobject_definition(self, sobject_name: str, allow_add=True, allow_drop=True):
        sobject_name = sobject_name.lower()

        sobj_columns: SObjectFields = self.sfclient.get_field_list(sobject_name)
        table_columns = self.driver.get_db_columns(sobject_name)

        #
        # check for added/dropped columns
        #
        table_field_names = set([tbl['column_name'] for tbl in table_columns])
        new_field_names = sobj_columns.names() - table_field_names
        dropped_fields = table_field_names - sobj_columns.names()

        if len(new_field_names) > 0:
            if not allow_add:
                self.log.warning(f'  new column found for {sobject_name}, auto-create disabled, skipping')
            else:
                self.log.info(f'  new columns found, updating table and indexes')
                new_field_defs = [sobj_columns.find(f) for f in new_field_names]
                newfields: [ColumnMap] = self.driver.alter_table_add_columns(new_field_defs, sobject_name)
                if len(newfields) > 0:
                    self.driver.maintain_indexes(sobject_name, SObjectFields(new_field_defs))

                    fieldmap: [ColumnMap] = self.filemgr.get_sobject_map(sobject_name)
                    fieldmap.extend(newfields)
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
            fieldmap: [ColumnMap] = self.filemgr.get_sobject_map(sobject_name)
            newlist: [ColumnMap] = list()
            for item in fieldmap:
                if item.sobject_field in dropped_fields:
                    pass
                else:
                    newlist.append(item)
            fieldmap = newlist
            self.log.info(f'  dropped column(s) detected')
            self.driver.alter_table_drop_columns(dropped_fields, sobject_name)
            self.filemgr.save_sobject_map(sobject_name, fieldmap)
            select = self.driver.make_select_statement([field.sobject_field for field in fieldmap], sobject_name)
            self.filemgr.save_sobject_query(sobject_name, select)
            parser = self.driver.make_transformer(sobject_name, sobject_name, fieldmap)
            self.filemgr.save_sobject_transformer(sobject_name, parser)

            self.filemgr.save_sobject_fields(sobject_name, sobj_columns)
        return True

    def initialize_config(self, envname: str):
        if self.filemgr.get_configured_tables() is not None:
            self.log.error('Initialization halted, config.json already exists. '
                           'Remove file and tables manually to start over')
            exit(1)
        sobject_list: [Dict] = self.inspect()
        sobjectconfig = []
        for sobject in sobject_list:
            sobjectconfig.append(LocalTableConfig({'name': sobject['name'].lower(), 'enabled': False}))
        self.filemgr.save_configured_tables(sobjectconfig)
        self.log.info(f'Initial configuration created for {envname}')

    def enable_table_sync(self, table_names: [str], flag: bool):
        table_config: [LocalTableConfig] = self.filemgr.get_configured_tables()
        to_enable = [a.lower() for a in make_arg_list(table_names)]
        for entry in table_config:
            if entry.name in to_enable:
                self.log.info(f"Setting {entry.name} sync to {flag}")
                entry.enabled = flag
        self.filemgr.save_configured_tables(table_config)
