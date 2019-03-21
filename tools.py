import datetime
from typing import Optional

import yaml

import DriverManager
from context import Context
from objects.connections import Connections, ConnectionConfig
from sfapi import SFClient
import logging

_log = logging.getLogger('main')


def setup_env(envname) -> Optional[Context]:
    mde = Connections()
    env: ConnectionConfig = mde.get_db_env(envname)
    if env is None:
        _log.error(f'Configuration for {envname} not found')
        exit(1)

    sf = SFClient()
    try:
        sf.login(env.consumer_key, env.consumer_secret, env.login, env.password, env.authurl)
    except Exception as ex:
        _log.error(f'Unable to connect to {env.authurl} as {env.login}: {str(ex)}')
        return None

    return Context(envname, env, get_db_connection(envname), sf)


def get_db_connection(envname: str) -> DriverManager.DbDriverMeta:
    mde = Connections()
    env: ConnectionConfig = mde.get_db_env(envname)
    if env is None:
        _log.error(f'Configuration for {envname} not found')
        exit(1)

    driver = DriverManager.Manager().get_driver(env.dbvendor)
    driver.connect(env)
    return driver


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial
    return str(obj)


def dict_list_to_dict(alist, keyfield):
    assert(keyfield is not None)
    assert(alist is not None)

    result = dict()
    for item in alist:
        key = item[keyfield]
        result[key] = item
    return result


def sf_timestamp(t: datetime):
    s = t.isoformat()[0:19]
    s += '+00:00'
    return s


def parse_timestamp(t):
    return datetime.datetime.strptime(t[0:19], '%Y-%m-%dT%H:%M:%S')


def load_file_items(filename):
    with open(filename, 'r') as f:
        line_list = f.readlines()
        stripped_list = [line.strip() for line in line_list if len(line) > 0]
        return stripped_list


def make_arg_list(args_list):
    processed_args = []
    for arg in args_list:
        if len(arg) == 0:
            continue
        if arg.startswith('@'):
            processed_args.extend(load_file_items(arg[1:]))
        else:
            processed_args.append(arg)
    return processed_args


def load_log_config():
    with open('logging.yml', 'r') as configfile:
        _logconfig = yaml.load(configfile.read(), Loader=yaml.FullLoader)
        return _logconfig
