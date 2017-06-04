import json

__author__ = 'mark'

import os
from config import BASE_DIR, storagedir


class ConfigEnv(object):
    def __init__(self, d, dbpath=None):
        self.fields = d

    @property
    def id(self):
        return self.fields['id']

    @id.setter
    def id(self, i):
        self.fields['id'] = i

    @property
    def login(self):
        return self.fields['login']

    @login.setter
    def sflogin(self, l):
        self.fields['login'] = l

    @property
    def password(self):
        return self.fields['password']

    @password.setter
    def password(self, x):
        self.fields['password'] = x

    @property
    def consumer_key(self):
        return self.fields['consumer_key']

    @consumer_key.setter
    def consumer_key(self, value):
        self.fields['consumer_key'] = value

    @property
    def consumer_secret(self):
        return self.fields['consumer_secret']

    @consumer_secret.setter
    def consumer_secret(self, value):
        self.fields['consumer_secret'] = value

    @property
    def authurl(self):
        return self.fields['authurl']

    @authurl.setter
    def authurl(self, value):
        self.fields['authurl'] = value

    @property
    def dbvendor(self):
        return self.fields['dbvendor']

    @dbvendor.setter
    def dbvendor(self, value):
        self.fields['dbvendor'] = value

    @property
    def dbname(self):
        return self.fields['dbname']

    @dbname.setter
    def dbname(self, value):
        self.fields['dbname'] = value

    @property
    def dbuser(self):
        return self.fields['dbuser']

    @dbuser.setter
    def dbuser(self, value):
        self.fields['dbuser'] = value

    @property
    def dbpass(self):
        return self.fields['dbpass']

    @dbpass.setter
    def dbpass(self, value):
        self.fields['dbpass'] = value

    @property
    def dbhost(self):
        return self.fields['dbhost']

    @dbhost.setter
    def dbhost(self, value):
        self.fields['dbhost'] = value

    @property
    def dbport(self):
        return self.fields['dbport']

    @dbport.setter
    def dbport(self, value):
        self.fields['dbport'] = value


class MDEngine(object):
    def __init__(self, dbpath=None):
        if dbpath is None:
            dbpath = os.path.join(storagedir, 'mdata.json')
        self.dbpath = dbpath
        if os.path.exists(dbpath):
            with open(dbpath, 'r') as f:
                self.data = json.loads(f.read())['metadata']
                self.data = [ConfigEnv(item) for item in self.data]
        else:
            self.data = []

    def close(self):
        pass

    def fetch_dblist(self):
        return self.data

    def save(self, sfe: ConfigEnv):
        self.data.append(sfe)
        with open(self.dbpath, 'w') as f:
            f.write(json.dumps({ 'metadata' : self.data }))

    def get_db_env(self, envname) -> ConfigEnv:
        for e in self.data:
            if e.id == envname:
                return e
        return None
