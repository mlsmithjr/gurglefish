from flask_restful import Resource
from flask import request
from salesforce.sfapi import SFClient

__author__ = 'mark'

from schema import SchemaManager
from db.mdatadb import MDEngine
import json

# 3MVG9RHx1QGZ7OsigFuhg6XILDVDC11BwhXUjSMH1zlTFnuWU0iFDz6dHugoQX2OcNXnfExbMyGSUguZ0FMcP
# 6852393882456282702
# token: F9rdKoQRA1NtjEKmqqVgMeff

class TestEnv(Resource):

    def required(self, d, key):
        if not key in d: raise Exception('required field missing: ' + key)

    def post(self):
        env = request.json
        print(env)

        try:
            self.required(env, 'login')
            self.required(env, 'password')
            self.required(env, 'authkey')
            self.required(env, 'authsecret')

            username = env['login']
            password = env['password']
            consumer_key = env['authkey']
            consumer_secret = env['authsecret']
            client = SFClient()
            if env['envtype'] == 'Production':
                server = 'https://login.salesforce.com'
            else:
                server = 'https://test.salesforce.com'

            client.login(consumer_key, consumer_secret, username, password, server)
        except Exception as ex:
            return { 'success':False, 'message': str(ex) }

        return { 'success':True}


