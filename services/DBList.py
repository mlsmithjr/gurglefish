from flask_restful import Resource

__author__ = 'mark'

from schema import SchemaManager
from db.mdatadb import MDEngine
import json


class RestDBList(Resource):
    def get(self):
        mde = MDEngine()
        thelist = mde.fetch_dblist()
        payload = []
        for sfe in thelist:
            item = {}
            item['authurl'] = sfe.authurl
            item['login'] = sfe.login
            item['dbname'] = sfe.dbname
            payload.append(item)
        return { 'payload': payload, 'success':True }


