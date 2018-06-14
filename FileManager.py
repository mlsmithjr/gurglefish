import gzip
import os
import json


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
        filename = os.path.join(self.schemadir, sobject_name, sobject_name + '.json')
        try:
            with open(filename, 'r') as jsonfile:
                return json.load(jsonfile)
        except Exception:
            return None

    def get_configured_tables(self):
        with open(os.path.join(self.basedir, 'db', self.envname, 'config.json'), 'r') as configfile:
            return json.load(configfile)['configuration']['sobjects']

    def save_configured_tables(self, new_config):
        config = None
        with open(os.path.join(self.basedir, 'db', self.envname, 'config.json'), 'r') as configfile:
            config = json.load(configfile)
        config['configuration']['sobjects'] = new_config
        with open(os.path.join(self.basedir, 'db', self.envname, 'config.json'), 'w') as configfile:
            json.dump(config, configfile, indent=2)

    def get_config(self):
            with open(os.path.join(self.basedir, 'db', self.envname, 'config.json'), 'r') as configfile:
                return json.load(configfile)

    def save_config(self, configmap):
        with open(os.path.join(self.basedir, 'db', self.envname, 'config.json'), 'w') as configfile:
            json.dump(configmap, configfile, indent=4)

    def get_sobject_map(self, sobject_name):
        sobject_name = sobject_name.lower()
        with open(os.path.join(self.schemadir, sobject_name, '{}_map.json'.format(sobject_name)), 'r') as mapfile:
            return json.load(mapfile)

    def get_sobject_query(self, sobject_name):
        sobject_name = sobject_name.lower()
        with open(os.path.join(self.schemadir, sobject_name, 'query.soql'), 'r') as queryfile:
            return queryfile.read()

    def save_sobject_fields(self, sobject_name, fields):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}.json'.format(sobject_name)), 'w') as jsonfile:
            json.dump(fields, jsonfile, indent=2)

    def save_sobject_map(self, sobject_name, fieldmap):
        sobject_name = sobject_name.lower()
        os.makedirs(os.path.join(self.schemadir, sobject_name), exist_ok=True)
        with open(os.path.join(self.schemadir, sobject_name, '{}_map.json'.format(sobject_name)), 'w') as jsonfile:
            json.dump(fieldmap, jsonfile, indent=2)

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
