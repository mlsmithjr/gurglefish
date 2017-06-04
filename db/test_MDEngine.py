from unittest import TestCase
from db.mdatadb import MDEngine, ConfigEnv
import os

__author__ = 'mark'


class TestMDEngine(TestCase):
    def test_fetch_dblist(self):
        mde = MDEngine.createdb('sqlite:///:memory:')
        sfe = ConfigEnv()
        sfe.login = 'mark'
        sfe.password = 'sdfsdf'
#        sfe.userdb = UserDB()
        sfe.dbname = 'testdb'
        sfe.authurl = 'https://login.salesforce.com'
        mde.save(sfe)
        sfelist = mde.fetch_dblist()
        self.assertGreaterEqual(len(sfelist), 1, 'Expected 1 or more results')

    def test_get_db_env(self):
        mde = MDEngine.createdb('sqlite:///:memory:')
        sfe = ConfigEnv()
        sfe.login = 'mark'
        sfe.password = 'sdfsdf'
        #sfe.userdb = UserDB()
        sfe.dbname = 'testdb'
        sfe.authurl = 'https://login.salesforce.com'
        mde.save(sfe)
        sfe = mde.get_db_env('testdb')
        self.assertIsNotNone(sfe, 'expected to find testdb')
