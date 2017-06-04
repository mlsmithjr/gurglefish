import unittest

import DriverManager
from db.mdatadb import MDEngine


class TestMDEngine(unittest.TestCase):

    def test_upsert(self):
        mde = MDEngine()
        env = mde.get_db_env('pgtesting')
        dbdriver = DriverManager.Manager().getDriver('postgresql')
        dbdriver.connect(env)
        try:
            dbdriver.exec_dml('drop table cars')
        except Exception as ex:
            dbdriver.rollback()
        try:
            dbdriver.exec_dml('create table cars (id varchar(10) not null primary key, name varchar(20))')
        except Exception as ex:
            print(ex)
        cursor = dbdriver.cursor
        cursor.execute("insert into cars (id,name) values ('bmw','BMW')")
        dbdriver.commit()
        cursor.close()
        cursor = dbdriver.cursor
        dbdriver.upsert(cursor, 'cars', { 'id': 'bmw', 'name': 'Bavarian Motor Works'})
        dbdriver.commit()


if __name__ == '__main__':
    unittest.main()
