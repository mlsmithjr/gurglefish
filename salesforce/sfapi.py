import logging
import json
import operator
from typing import Dict, Set, Optional

import requests
from fastcache import lru_cache

MAX_BATCH_SIZE = 100
_API_VERSION = '40.0'


class SFError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class SObjectField(object):
    def __init__(self, field: Dict):
        self.field = field

    @property
    def name(self) -> str:
        return self.field['name']

    @property
    def is_custom(self) -> bool:
        return self.field['custom']

    @property
    def digits(self) -> int:
        return self.field['digits']

    @property
    def label(self) -> str:
        return self.field['label']

    @property
    def length(self) -> int:
        return self.field['length']

    @property
    def references(self) -> [str]:
        return self.field.get('referenceTo', [])

    @property
    def relationship_name(self) -> str:
        return self.field.get('relationshipName', None)

    @property
    def get_type(self):
        return self.field['type']

    @property
    def is_unique(self) -> bool:
        return self.field['unique']


class SObjectFields(object):
    def __init__(self, fields: [Dict]):
        self.fields = dict()
        for field in fields:
            name = field['name']
            self.fields[name.lower()] = SObjectField(field)

    def find(self, name: str) -> Optional[SObjectField]:
        return self.fields.get(name.lower(), None)

    def names(self) -> Set:
        return set(self.fields.keys())

    def values(self) -> [Dict]:
        return self.fields.values()


class SFClient:

    def __init__(self):
        self.logger = logging.getLogger('salesforce')
        self.access_token = None
        self.service_url = None
        self.client = None
        self._username = None

    def login(self, consumer_key, consumer_secret, username, password, server_url):
        self._username = username
        payload = {'grant_type': 'password',
                   'username': username,
                   'password': password,
                   'client_id': consumer_key,
                   'client_secret': consumer_secret
                   }
        # self.logger.debug('url=%s, payload=%s' % (server_url, payload))
        rsp = requests.post(server_url + '/services/oauth2/token', data=payload,
                            headers={'content-type': 'application/x-www-form-urlencoded'})
        payload = json.loads(rsp.text)
        if 'error' in payload:
            raise Exception(payload['error_description'])
        self.logger.debug('payload=%s' % (rsp.text,))
        self.construct(payload['access_token'], payload['instance_url'])

    def construct(self, token, server_url):
        self.access_token = token
        self.service_url = server_url
        self.client = requests.Session()
        self.client.headers.update({'Authorization': 'OAuth ' + token,
                                    'Content-Type': 'application/json; charset=UTF-8',
                                    'Accept-Encoding': 'gzip, compress, deflate', 'Accept-Charset': 'utf-8'})

    def close(self):
        pass

    @lru_cache(maxsize=3, typed=False)
    def get_sobject_definition(self, name: str) -> Dict:
        sobject_doc = self._invoke_get('sobjects/{}/describe'.format(name), {})
        return sobject_doc

    @lru_cache(maxsize=1, typed=False)
    def get_sobject_list(self) -> [Dict]:
        payload = self._invoke_get('sobjects/', {})
        return payload['sobjects']

    @lru_cache(maxsize=3, typed=False)
    def get_field_list(self, sobject_name: str) -> SObjectFields:
        fielddef = self._invoke_get('sobjects/%s/describe/' % (sobject_name,), {})
        fieldlist = fielddef['fields']
        fieldlist.sort(key=operator.itemgetter('name'))
        return SObjectFields(fieldlist)

    def fetch_record(self, objectname: str, recid: str, fieldlist: [str]) -> Dict:
        fieldstring = ','.join(fieldlist)
        url = 'sobjects/{0}/{1}'.format(objectname, recid)
        result = self._invoke_get(url, {'fields': fieldstring})
        return result

    def _invoke_get(self, url, url_params):
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/{url}'
        # self.logger.debug('get %s', fullurl)
        response = self.client.get(fullurl, params=url_params)
        result_payload = response.text
        response.raise_for_status()
        data = json.loads(result_payload)
        return data

    def record_count(self, sobject: str, query_filter: str = None):
        soql = 'select count() from ' + sobject
        if filter:
            soql += ' where ' + query_filter
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/query/'
        # self.logger.debug('get %s', fullurl)
        response = self.client.get(fullurl, params={'q': soql})
        result_payload = response.json()
        if response.status_code != 200:
            self.logger.error(f'query error {response.status_code}, {response.reason}')
            self.logger.error(result_payload)
            return
        return result_payload['totalSize']

    def query(self, soql: str):
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/query/'
        # self.logger.debug('get %s', fullurl)
        response = self.client.get(fullurl, params={'q': soql})
        result_payload = response.text
        if response.status_code != 200:
            self.logger.error(f'query error {response.status_code}, {response.reason}')
            self.logger.error(result_payload)
            return
        data = json.loads(result_payload)
        recs = data['records']
        for rec in recs:
            yield (rec)
        while 'nextRecordsUrl' in data:
            next_records_url = data['nextRecordsUrl']
            if next_records_url:
                response = self.client.get('%s%s' % (self.service_url, next_records_url))
                txt = response.text
                if isinstance(txt, str):
                    result_payload = txt
                else:
                    result_payload = str(txt, 'utf-8')
                data = json.loads(result_payload)
                recs = data['records']
                for rec in recs:
                    yield (rec)
            else:
                break
