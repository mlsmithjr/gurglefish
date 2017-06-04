
import logging
import datetime


log = logging.getLogger(__name__)


def buildQuery(objectname, fieldlist, startdate, enddate, filter = None):
    s = sfTimestamp(startdate)
    e = sfTimestamp(enddate)
    soql = 'select '
    soql += ','.join(fieldlist)
    soql += ' from ' + objectname
    soql += " where LastModifiedDate >= {0} and LastModifiedDate <= {1}".format(s, e)
    if not filter is None:
        soql += ' and ' + filter
    log.debug(soql)
    return soql

def soqlfetcher(sfclient, objectname, fieldlist, start, end, filter = None):
    log.debug('soql fetching for {0}, start={1}, end={2}'.format(objectname, start, end))
    soql = buildQuery(objectname, fieldlist, start, end)
    for row in sfclient.query(soql):
        yield row

def prepareFetchPackage(sfclient, objectname, fieldlist, start, end):
    log.debug('fetching for {0}, start={1}, end={2}'.format(objectname, start, end))
    result = sfclient.getUpdatedIdList(objectname, start, end)
    idlist = result['ids']
    if len(idlist) == 0:
        log.info('nothing updated')
        return None
    latestDateCovered = result['latestDateCovered'][0:19]
    return {'latestDateCovered': latestDateCovered, 'idlist':idlist, 'objectname':objectname, 'fieldlist':fieldlist}

def fetcher(sfclient, package):
    objectname = package['objectname']
    fieldlist = package['fieldlist']
    for recid in package['idlist']:
        record = sfclient.fetchRecord(objectname, recid, fieldlist)
        yield record


def sfTimestamp(t):
    s = t.isoformat()[0:19]
    s += '+00:00'
    return s

def pyTimestamp(t):
    return datetime.strptime(t[0:19], '%Y-%m-%dT%H:%M:%S')

