import os

import config
from FileManager import FileManager
from db.mdatadb import ConfigEnv


class Context:
    filemgr = None
    env = None
    driver = None
    sfapi = None

    def __init__(self, env: ConfigEnv, dbdriver, sfclient):
        self.env = env
        self.driver = dbdriver
        self.sfapi = sfclient

        self.filemgr = FileManager(config.storagedir, self.env.id)

    @property
    def config_env(self):
        return self.env

    @property
    def dbdriver(self):
        return self.driver

    @property
    def sfclient(self):
        return self.sfapi

