import json

import DriverManager
import tools
from db.mdatadb import MDEngine, ConfigEnv
from salesforce.sfapi import SFClient

MAX_ALLOWED_SEED_RECORDS = 50000


def envlist():
    mde = MDEngine()
    thelist = mde.fetch_dblist()
    payload = []
    for sfe in thelist:
        payload.append(sfe.to_dict())
    return payload


def verifydb(config):
    cenv = ConfigEnv.from_dict(config)
    try:
        dbdriver = DriverManager.Manager().getDriver('postgresql')
        dbdriver.connect(cenv)
    except Exception as ex:
        return {'verified': False, 'message': str(ex)}
    return {'verified': True, 'message': ''}


def verify_salesforce(config):
    env = ConfigEnv.from_dict(config)
    try:
        sf = SFClient()
        sf.login(env.consumer_key, env.consumer_secret, env.login, env.password, env.authurl)
    except Exception as ex:
        return {'verified': False, 'message': 'str(ex)'}
    return {'verified': True, 'message': ''}


def sobjects(envname):
    try:
        ctx = tools.setup_env(envname)
    except Exception as ex:
        print(ex)
        return {'success': False, 'message': 'environment not found'}
    config = ctx.filemgr.get_configured_tables()
    return config


def enable_sobject(envname, sobject_name):
    try:
        ctx = tools.setup_env(envname)
    except Exception as ex:
        print(ex)
        return {'success': False, 'message': 'environment not found'}
    config = ctx.filemgr.get_config()
    for so in config['configuration']['sobjects']:
        if so['name'] == sobject_name:
            so['enabled'] = True
            ctx.filemgr.save_config(config)
            return {'success': True}
    return {'success': False, 'message': f'SObject {sobject_name} not found in {envname}'}


def disable_sobject(envname, sobject_name):
    try:
        ctx = tools.setup_env(envname)
    except Exception as ex:
        print(ex)
        return {'success': False, 'message': 'environment not found'}
    config = ctx.filemgr.get_config()
    for so in config['configuration']['sobjects']:
        if so['name'] == sobject_name:
            so['enabled'] = False
            ctx.filemgr.save_config(config)
            return {'success': True}
    return {'success': False, 'message': f'SObject {sobject_name} not found in {envname}'}


def check_if_can_enable(envname, sobject_name) -> (bool, str):
    try:
        ctx = tools.setup_env(envname)
    except Exception as ex:
        print(ex)
        return False, 'Internal Server Error'
    try:
        sf_records = ctx.sfapi.record_count(sobject_name)
        if ctx.dbdriver.table_exists(sobject_name):
            local_records = ctx.dbdriver.record_count(sobject_name)
            if abs(local_records - sf_records) > MAX_ALLOWED_SEED_RECORDS:
                return False, 'Too many records to begin sync - initial manual load required'
        if sf_records > MAX_ALLOWED_SEED_RECORDS:
            return False, 'Too many records to begin sync - initial manual load required'
    except Exception as ex:
        print(ex)
        return False, 'Manual load required'
    return True, None
