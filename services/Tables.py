import operator

from flask_restful import Resource
from flask import request

import tools

__author__ = 'mark'


class GetTables(Resource):
    def get(self):
        envname = request.args['envname']
        env, dbmgr, sf = tools.setup_env(envname)
        table_list = sorted(dbmgr.get_os_tables(), key=operator.itemgetter('name'))
        return { 'payload': table_list, 'success': True }
