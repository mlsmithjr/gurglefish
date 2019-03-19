import string
from flask_restful import Resource
from flask import request

__author__ = 'mark'

from connections import Connections
from connections import ConnectionConfig


class SaveEnv(Resource):

    def required(self, d: dict, key: string) -> None:
        if not key in d: raise Exception('required field missing: ' + key)

    def post(self) -> dict:
        env = request.json
        #print(env)

        try:
            self.required(env, 'dbname')
            self.required(env, 'login')
            self.required(env, 'password')
            self.required(env, 'authkey')
            self.required(env, 'authsecret')

            dbname = env['dbname']
            envid = None
            if 'envid' in env: envid = env['envid']
            mde = Connections()
            sfenv = mde.get_db_env(dbname)
            if envid is None:
                if not sfenv is None:
                    return {'success':False, 'message':'Database name {0} already in use'.format(dbname)}
                else:
                    sfenv = ConnectionConfig()

            username = env['login']
            password = env['password']
            consumer_key = env['authkey']
            consumer_secret = env['authsecret']
            if env['envtype'] == 'Production':
                server = 'https://login.salesforce.com'
            else:
                server = 'https://test.salesforce.com'

            sfenv.login = username
            sfenv.password = password
            sfenv.consumer_key = consumer_key
            sfenv.consumer_secret = consumer_secret
            sfenv.authurl = server
            sfenv.dbname = dbname
            mde.save(sfenv)

        except Exception as ex:
            return { 'success':False, 'message': str(ex) }

        return { 'success':True, 'envid': sfenv.id }


# masmith@redhat.com.fte0
# redhat123nO8W1Bg8STEseMaAyTUUWJqE
# 3MVG9RHx1QGZ7OsjkJy8naE3ZBaR5FL1ISOcQVumkw.GIjcSqjLpB_WaZC6tyzrahTjSMv8LaoZNFbD_.btoD
# 4057585844817217438
# https://test.salesforce.com
