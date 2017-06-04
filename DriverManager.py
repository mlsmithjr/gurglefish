
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


class GetMappedTablesResult(object):

    def __init__(self, record):
        self.tablename = record['table_name']
        self.sobject = record['sobject']

    @property
    def tablename(self):
        return self.tablename

    @property
    def sobjectname(self):
        return self.sobject

class FieldMapResult(object):
    def __init__(self, record):
        self.sobject_field = record['sobject_field']
        self.db_field = record['db_field']
        self.fieldtype = record['fieldtype']

    @property
    def sobject_field(self):
        return self.sobject_field

    @property
    def db_field(self):
        return self.db_field

    @property
    def fieldtype(self):
        return self.fieldtype


class DbDriverMeta(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def exec_dml(self, dml):
        pass

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
    def get_mapped_tables(self):
        pass

    @abstractmethod
    def get_field_map(self, sobject_name: string):
        pass

    @abstractmethod
    def is_table_mapped(self, sobject) -> bool:
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
    def add_column(self, sobject_name:string, fielddef:dict):
        pass

    @abstractmethod
    def drop_column(self, table_name, column_name):
        pass

    @abstractmethod
    def drop_table(self, table_name):
        pass

    @abstractmethod
    def add_table(self, table_name):
        pass


class Manager(object):

    def __init__(self):
        self._res = {}
        modules = pkgutil.iter_modules(path=['drivers'])
        for finder, mod_name, ispkg in modules:
            print('mod_name=' + mod_name)
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

