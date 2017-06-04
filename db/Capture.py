__author__ = 'mark'

import json
import os
from config import storagedir

class CaptureManager(object):

    @staticmethod
    def save(bucket, name, resource):
        pname = os.path.join(storagedir, 'cap', bucket)
        os.makedirs(pname, exist_ok = True)
        fname = os.path.join(storagedir, 'cap', bucket, name + '.json')
        with open(fname, "w") as f:
            f.write(json.dumps(resource, indent=4))

    @staticmethod
    def exists(bucket, name) -> bool:
        fname = os.path.join(storagedir, 'cap', bucket, name + '.json')
        return os.path.exists(fname)

    @staticmethod
    def fetch(bucket, name):
        if not CaptureManager.exists(bucket, name): return None
        fname = os.path.join(storagedir, 'cap', bucket, name + '.json')
        with open(fname, "r") as f:
            resource = json.loads(f.read())
            return resource
