import datetime
import os

import DriverManager
import config
from context import Context
from db.mdatadb import MDEngine, ConfigEnv
from salesforce.sfapi import SFClient


def setup_env(envname) -> Context:
    mde = MDEngine()
    env = mde.get_db_env(envname)

    sf = SFClient(os.path.join(config.storagedir, 'db', envname))
    sf.login(env.consumer_key, env.consumer_secret, env.login, env.password, env.authurl)

    dbdriver = DriverManager.Manager().getDriver('postgresql')
    dbdriver.connect(env)
    return Context(env, dbdriver, sf)


def save_env(cfg: ConfigEnv) -> None:
    mde = MDEngine()
    mde.save(cfg)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial
    return str(obj)
    #raise TypeError("Type not serializable")


def dict_list_to_dict(alist, keyfield):
    assert(keyfield is not None)
    assert(alist is not None)

    result = dict()
    for item in alist:
        key = item[keyfield]
        result[key] = item
    return result
