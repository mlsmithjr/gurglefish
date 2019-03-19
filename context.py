import config
from DriverManager import DbDriverMeta
from FileManager import FileManager
from connections import ConnectionConfig
from salesforce.sfapi import SFClient


class Context:

    def __init__(self, env: ConnectionConfig, dbdriver: DbDriverMeta, sfclient: SFClient):
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
