import datetime
from typing import Optional

import DriverManager
from context import Context
from db.mdatadb import MDEngine, ConfigEnv
from salesforce.sfapi import SFClient
import logging

_log = logging.getLogger('main')


def setup_env(envname) -> Optional[Context]:
    mde = MDEngine()
    env = mde.get_db_env(envname)
    if env is None:
        _log.error(f'Configuration for {envname} not found')
        exit(1)

    sf = SFClient()
    try:
        sf.login(env.consumer_key, env.consumer_secret, env.login, env.password, env.authurl)
    except Exception as ex:
        _log.error(f'Unable to connect to {env.authurl} as {env.login}: {str(ex)}')
        return None

    dbdriver = DriverManager.Manager().getDriver('postgresql')
    if not dbdriver.connect(env):
        return None
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
