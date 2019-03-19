
import os
from unittest import TestCase
from connections import Connections, ConnectionConfig

__author__ = 'mark'


class TestMDEngine(TestCase):
    def test_dblist(self):
        if os.path.exists('/tmp/mdata_test.db'):
            os.unlink('/tmp/mdata_test.db')
        mde = Connections('/tmp/mdata_test.db')
        sfe = ConnectionConfig()
        sfe.dbname = 'test'
        sfe.authurl = 'https://test.salesforce.com'
        sfe.consumer_key = 'abc'
        sfe.consumer_secret = '123'
        sfe.sflogin = 'mark'
        sfe.sfpassword = 'hello'
        mde.session.save(sfe)

        thelist = mde.fetch_dblist()
        item = thelist[0]
        self.assertEqual(item.login, 'mark', 'expected login to be "mark"')

        os.unlink('/tmp/mdata_test.db')
