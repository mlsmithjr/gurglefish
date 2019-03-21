import config
from DriverManager import DbDriverMeta
from FileManager import FileManager
from objects.connections import ConnectionConfig
from sfapi import SFClient


class Context:

    def __init__(self, envname: str, env: ConnectionConfig, dbdriver: DbDriverMeta, sfclient: SFClient):
        self.env = env
        self.envname = envname
        self.driver = dbdriver
        self.sfapi = sfclient
        self.filemgr = FileManager(config.storagedir, self.env.id)

    @property
    def config_env(self) -> ConnectionConfig:
        return self.env

    @property
    def dbdriver(self) -> DbDriverMeta:
        return self.driver

    @property
    def sfclient(self) -> SFClient:
        return self.sfapi
