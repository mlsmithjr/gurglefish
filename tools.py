import datetime

import DriverManager
from context import Context
from db.mdatadb import MDEngine
from salesforce.sfapi import SFClient
from schema import SchemaManager


def setup_env(envname) -> Context:
    mde = MDEngine()
    env = mde.get_db_env(envname)

    sf = SFClient()
    sf.login(env.consumer_key, env.consumer_secret, env.login, env.password, env.authurl)

    dbdriver = DriverManager.Manager().getDriver('postgresql')
    dbdriver.connect(env)
    return Context(env, dbdriver, sf)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial
    return str(obj)
    raise TypeError("Type not serializable")
