
import pkgutil
from abc import ABCMeta, abstractmethod
import string
from typing import List

from db.mdatadb import ConfigEnv
from salesforce.sfapi import SFClient

class GetDbTablesResult(object):

    def __init__(self, name):
        self.tablename = name

    @property
    def tablename(self):
        return self.name

    @tablename.setter
    def tablename(self, name):
        self.name = name




class DbDriverMeta(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def connect(self, env:ConfigEnv):
        pass

    @abstractmethod
    def get_db_tables(self)-> List[GetDbTablesResult]:
        pass

    @abstractmethod
    def table_exists(self, table_name: string):
        pass

    @abstractmethod
    def get_db_columns(self, table_name: string):
        pass

    @abstractmethod
    def make_create_table(self, sf: SFClient, sobject_name: string):
        pass

    @abstractmethod
    def getMaxTimestamp(self, tablename: string):
        pass

    @abstractmethod
    def format_for_export(self, trec, tablefields, fieldmap):
        pass

    @abstractmethod
    def make_transformer(self, sobject_name, table_name, fieldlist):
        pass

    @abstractmethod
    def make_select_statement(self, field_names, sobject_name):
        pass

    @abstractmethod
    def maintain_indexes(self, sobject_name, field_defs):
        pass

    @abstractmethod
    def record_count(self, table_name):
        pass

    @abstractmethod
    def get_table_fields(self, table_name):
        pass

    @abstractmethod
    def upsert(self, cur, table_name, trec: dict, journal=None):
        pass

    @abstractmethod
    def bulk_load(self, tablename):
        pass

    @abstractmethod
    def start_sync_job(self):
        pass

    @abstractmethod
    def finish_sync_job(self, jobid):
        pass

    @abstractmethod
    def insert_sync_stats(self, jobid, table_name, sync_start, sync_end, sync_since, inserts, updates):
        pass

#    @abstractmethod
#    def recent_sync_timestamp(self, tablename = None):
#        pass

    @abstractmethod
    def clean_house(self, date_constraint):
        pass

class Manager(object):

    def __init__(self):
        self._res = {}
        modules = pkgutil.iter_modules(path=['drivers'])
        for finder, mod_name, ispkg in modules:
            l = finder.find_module(mod_name)
            mod = l.load_module(mod_name)
            cls = getattr(mod, 'Driver')
            self._res[mod_name] = cls

        # check subfolders
        # lst = os.listdir(folder)
        # dir = []
        # for d in lst:
        #     s = os.path.abspath(folder) + os.sep + d
        #     if os.path.isdir(s) and os.path.exists(s + os.sep + "__init__.py"):
        #         dir.append(d)
        # # load the modules
        # for d in dir:
        #     print('importing ' + d)
        #     res[d] = importlib.import_module(folder + "." + d)
    #        res[d] = __import__(folder + "." + d, fromlist = ["*"])

    def getDriver(self, driver_name) -> DbDriverMeta:
        if driver_name in self._res:
            return self._res[driver_name]()
        return None


if __name__ == '__main__':
    mgr = Manager()
    driver = mgr.getDriver('postgresql')
#    cls = getClassByName(mods["postgresql"], "PgDriver")
#    obj = cls()

