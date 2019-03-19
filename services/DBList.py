from flask_restful import Resource

__author__ = 'mark'

from connections import Connections


class RestDBList(Resource):
    def get(self):
        mde = Connections()
        thelist = mde.fetch_dblist()
        payload = []
        for sfe in thelist:
            item = {}
            item['authurl'] = sfe.authurl
            item['login'] = sfe.login
            item['dbname'] = sfe.dbname
            payload.append(item)
        return { 'payload': payload, 'success':True }


