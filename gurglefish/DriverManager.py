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

import datetime
import os
import pkgutil
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Dict

from gurglefish.objects.connections import ConnectionConfig
from gurglefish.objects.sobject import ColumnMap, SObjectFields


class GetDbTablesResult(object):

    def __init__(self, name):
        self.tablename = name

    @property
    def tablename(self):
        return self.name

    @tablename.setter
    def tablename(self, name):
        self.name = name


class DbNativeExporter(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def soql(self) -> str:
        pass

    @abstractmethod
    def write(self, record: Dict):
        pass

    @abstractmethod
    def close(self):
        pass


class DbDriverMeta(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def connect(self, env: ConnectionConfig):
        pass

    @abstractmethod
    def create_exporter(self, sobject_name: str, ctx, just_sample=False, timestamp=None) -> DbNativeExporter:
        pass

    @abstractmethod
    def get_db_tables(self)-> List[GetDbTablesResult]:
        pass

    @abstractmethod
    def table_exists(self, table_name: str):
        pass

    @abstractmethod
    def get_db_columns(self, table_name: str):
        pass

    @abstractmethod
    def dump_ids(self, table_name: str, output_filename: str):
        pass

    @abstractmethod
    def make_create_table(self, fields: SObjectFields, sobject_name: str):
        pass

    @abstractmethod
    def make_select_statement(self, field_names: [str], sobject_name: str) -> str:
        pass

    @abstractmethod
    def exec_ddl(self, ddl: str):
        pass

    @abstractmethod
    def max_timestamp(self, tablename: str):
        pass

    @abstractmethod
    def format_for_export(self, trec, tablefields, fieldmap):
        pass

    @abstractmethod
    def make_transformer(self, sobject_name, table_name: str, fieldlist):
        pass

    @abstractmethod
    def maintain_indexes(self, sobject_name: str, field_defs):
        pass

    @abstractmethod
    def record_count(self, table_name: str):
        pass

    @abstractmethod
    def get_table_fields(self, table_name: str):
        pass

    @abstractmethod
    def delete(self, cur, table_name: str, key: str):
        pass

    @abstractmethod
    def upsert(self, cur, table_name: str, trec: dict, journal=None):
        pass

    @abstractmethod
    def import_native(self, tablename: str):
        pass

    @abstractmethod
    def export_native(self, table_name, output_path):
        pass

    @abstractmethod
    def start_sync_job(self):
        pass

    @abstractmethod
    def finish_sync_job(self, jobid):
        pass

    @abstractmethod
    def insert_sync_stats(self, jobid, table_name, sync_start, sync_end, sync_since, inserts, updates, deletes, api_calls):
        pass

    @abstractmethod
    def clean_house(self, date_constraint: datetime):
        pass

    @abstractmethod
    def alter_table_drop_columns(self, drop_field_names: [str], sobject_name: str):
        pass

    @abstractmethod
    def alter_table_add_columns(self, new_field_defs, sobject_name: str) -> [ColumnMap]:
        pass

    @abstractmethod
    def cursor(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass


class Manager(object):

    def __init__(self):
        self._res = {}
        path = os.path.dirname(os.path.abspath(__file__))
        modules = pkgutil.iter_modules(path=[os.path.join(path, 'drivers')])
        for finder, mod_name, ispkg in modules:
            toload = finder.find_module(mod_name)
            mod = toload.load_module(mod_name)
            cls = getattr(mod, 'Driver')
            self._res[mod_name] = cls

    def get_driver(self, driver_name) -> Optional[DbDriverMeta]:
        if driver_name in self._res:
            return self._res[driver_name]()
        return None
