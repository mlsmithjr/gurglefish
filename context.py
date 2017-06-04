import os

import config
from db.mdatadb import ConfigEnv


class Context:
    env = None
    driver = None
    sfapi = None
    schemadir = None
    exportdir = None

    def __init__(self, env: ConfigEnv, dbdriver, sfclient):
        self.env = env
        self.driver = dbdriver
        self.sfapi = sfclient

        self.schemadir = os.path.join(config.storagedir, 'db', self.env.dbname, 'schema')
        self.exportdir = os.path.join(config.storagedir, 'db', self.env.dbname, 'export')

    @property
    def config_env(self):
        return self.env

    @property
    def dbdriver(self):
        return self.driver

    @property
    def sfclient(self):
        return self.sfapi

