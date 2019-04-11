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

import gzip
import json
import os
import sys
from typing import Optional

from gurglefish.objects.files import LocalTableConfig
from gurglefish.objects.sobject import ColumnMap

from gurglefish.sfapi import SObjectFields


class FileManager(object):

    def __init__(self, basedir, envname):
        self.basedir = basedir
        self.envname = envname
        self.schemadir = os.path.join(basedir, 'db', envname, 'schema')
        self.exportdir = os.path.join(basedir, 'db', envname, 'export')

        if not os.path.exists(basedir):
            print(f'Gurglefish root director {basedir} does not exists')
            sys.exit(1)

        try:
            os.makedirs(self.schemadir, exist_ok=True)
        except PermissionError as pex:
            print(f'Permission denied creating {self.schemadir}')
            sys.exit(1)

        try:
            os.makedirs(self.exportdir, exist_ok=True)
        except PermissionError as pex:
            print(f'Permission denied creating {self.exportdir}')
            sys.exit(1)

    def create_journal(self, sobject_name):
        f = gzip.open(os.path.join(self.exportdir, '{}_journal.log.gz'.format(sobject_name)), 'wb')
        return f

    def get_global_filters(self):
        try:
            with open(os.path.join(self.basedir, 'global-filters.txt'), 'r') as filterfile:
                return filterfile.readlines()
        except:
            pass
        return []

    def get_filters(self):
        try:
            with open(os.path.join(self.basedir, 'db', self.envname, 'filters.txt', 'r')) as filterfile:
                return filterfile.readlines()
        except:
            pass
        return []

    def get_schema_list(self):
        return os.listdir(self.schemadir)

    def get_export_list(self):
        return os.listdir(self.exportdir)

    def load_translate_handler(self, sobject_name):
        import importlib.machinery
        loader = importlib.machinery.SourceFileLoader(sobject_name, os.path.join(self.schemadir, sobject_name, '{}_Transform.py'.format(sobject_name)))
        handler = loader.load_module(sobject_name)
        return handler

    def get_sobject_fields(self, sobject_name: str) -> Optional[SObjectFields]:
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        filename = os.path.join(self.schemadir, sobject_name, sobject_name + '.json')
        try:
            with open(filename, 'r') as jsonfile:
                return SObjectFields(json.load(jsonfile))
        except Exception:
            return None

    def save_sobject_fields(self, sobject_name: str, fields: SObjectFields):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}.json'.format(sobject_name)), 'w') as mapfile:
            mapfile.write(json.dumps(fields.values_exportable(), indent=4))

    def get_configured_tables(self) -> [LocalTableConfig]:
        try:
            with open(os.path.join(self.basedir, 'db', self.envname, 'config.json'), 'r') as configfile:
                return [LocalTableConfig(t) for t in json.load(configfile)['configuration']['sobjects']]
        except FileNotFoundError:
            return None

    def save_configured_tables(self, new_config: [LocalTableConfig]):
        config = {'configuration': {'sobjects': []}}
        try:
            with open(os.path.join(self.basedir, 'db', self.envname, 'config.json'), 'r') as configfile:
                config = json.load(configfile)
        except FileNotFoundError:
            pass
        config['configuration']['sobjects'] = [t.dict for t in new_config]
        with open(os.path.join(self.basedir, 'db', self.envname, 'config.json'), 'w') as configfile:
            configfile.write(json.dumps(config, indent=4))

    def get_sobject_map(self, sobject_name: str) -> [ColumnMap]:
        sobject_name = sobject_name.lower()
        with open(os.path.join(self.schemadir, sobject_name, '{}_map.json'.format(sobject_name)), 'r') as mapfile:
            return [ColumnMap(f) for f in json.load(mapfile)]

    def save_sobject_map(self, sobject_name: str, fieldmap: [ColumnMap]):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}_map.json'.format(sobject_name)), 'w') as mapfile:
            mapfile.write(json.dumps([f.as_dict() for f in fieldmap], indent=4))

    def get_sobject_query(self, sobject_name: str):
        sobject_name = sobject_name.lower()
        with open(os.path.join(self.schemadir, sobject_name, 'query.soql'), 'r') as queryfile:
            return queryfile.read()

    def save_table_create(self, sobject_name: str, sql: str):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}.sql'.format(sobject_name)), 'w') as schemafile:
            schemafile.write(sql)

    def save_sobject_transformer(self, sobject_name: str, xformr: str):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}_Transform.py'.format(sobject_name)),
                  'w') as parserfile:
            parserfile.write(xformr)

    def save_sobject_query(self, sobject_name: str, soql: str):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, 'query.soql'), 'w') as queryfile:
            queryfile.write(soql)
