import tools
from db.mdatadb import MDEngine


def envlist():
    mde = MDEngine()
    thelist = mde.fetch_dblist()
    payload = []
    for sfe in thelist:
        item = dict()
        item['authurl'] = sfe.authurl
        item['login'] = sfe.login
        item['dbname'] = sfe.dbname
        payload.append(item)
    return payload


def sobjects(envname):
    try:
        ctx = tools.setup_env(envname)
    except Exception as ex:
        return {'error': True, 'message': 'environment not found' }
    config = ctx.filemgr.get_configured_tables()
    return  config
