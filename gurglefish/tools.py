#    Copyright 2018, 2019 Marshall L Smith Jr
#
#    This file is part of Gurglefish.
#
#    Gurglefish is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Gurglefish is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Gurglefish.  If not, see <http://www.gnu.org/licenses/>.
import datetime
import os
from typing import Optional

import yaml

from gurglefish import DriverManager
from gurglefish.context import Context
from gurglefish.objects.connections import Connections, ConnectionConfig
from gurglefish.sfapi import SFClient
import logging.config

_log = logging.getLogger('main')


def setup_env(envname) -> Optional[Context]:

    logconfig = load_log_config()
    logging.config.dictConfig(logconfig)

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
    path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(path, 'logging.yml'), 'r') as configfile:
        _logconfig = yaml.load(configfile.read(), Loader=yaml.FullLoader)
        return _logconfig
