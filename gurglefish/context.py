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
from gurglefish import config
from gurglefish.DriverManager import DbDriverMeta
from gurglefish.FileManager import FileManager
from gurglefish.objects.connections import ConnectionConfig
from gurglefish.sfapi import SFClient


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
