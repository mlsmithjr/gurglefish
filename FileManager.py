import gzip
import json
import os
from typing import Dict

import yaml

from objects.files import LocalTableConfig


class FileManager(object):
    envname = None
    basedir = None
    schemadir = None
    exportdir = None

    def __init__(self, basedir, envname):
        self.basedir = basedir
        self.envname = envname
        self.schemadir = os.path.join(basedir, 'db', envname, 'schema')
        self.exportdir = os.path.join(basedir, 'db', envname, 'export')
        os.makedirs(self.schemadir, exist_ok=True)
        os.makedirs(self.exportdir, exist_ok=True)

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

    def get_sobject_fields(self, sobject_name):
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        filename = os.path.join(self.schemadir, sobject_name, sobject_name + '.yml')
        try:
            with open(filename, 'r') as ymlfile:
                return yaml.load(ymlfile, Loader=yaml.FullLoader)
                # return json.load(jsonfile)
        except Exception:
            return None

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

    def get_global_settings(self) -> Dict:
        with open(os.path.join(self.basedir, 'db', self.envname, 'settings.yml'), 'r') as configfile:
            return yaml.load(configfile, Loader=yaml.FullLoader)

    def save_global_settings(self, configmap: Dict):
        with open(os.path.join(self.basedir, 'db', self.envname, 'settings.yml'), 'w') as configfile:
            configfile.write(yaml.dump(configmap))

    def get_sobject_map(self, sobject_name):
        sobject_name = sobject_name.lower()
        with open(os.path.join(self.schemadir, sobject_name, '{}_map.yml'.format(sobject_name)), 'r') as mapfile:
            return yaml.load(mapfile, Loader=yaml.FullLoader)  # json.load(mapfile)

    def get_sobject_query(self, sobject_name):
        sobject_name = sobject_name.lower()
        with open(os.path.join(self.schemadir, sobject_name, 'query.soql'), 'r') as queryfile:
            return queryfile.read()

    def save_sobject_fields(self, sobject_name, fields):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}.yml'.format(sobject_name)), 'w') as mapfile:
            # json.dump(fields, jsonfile, indent=2)
            mapfile.write(yaml.dump(fields))

    def save_sobject_map(self, sobject_name, fieldmap):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}_map.yml'.format(sobject_name)), 'w') as mapfile:
            # json.dump(fieldmap, jsonfile, indent=2)
            mapfile.write(yaml.dump(fieldmap))

    def save_table_create(self, sobject_name, sql):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}.sql'.format(sobject_name)), 'w') as schemafile:
            schemafile.write(sql)

    def save_sobject_transformer(self, sobject_name, xformr):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}_Transform.py'.format(sobject_name)),
                  'w') as parserfile:
            parserfile.write(xformr)

    def save_sobject_query(self, sobject_name, soql):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, 'query.soql'), 'w') as queryfile:
            queryfile.write(soql)
