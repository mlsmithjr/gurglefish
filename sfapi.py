import logging
import json
import operator
from typing import Dict

import requests
from fastcache import lru_cache

from objects.sobject import SObjectFields

MAX_BATCH_SIZE = 100
_API_VERSION = '44.0'


class SFClient:

    def __init__(self):
        self.log = logging.getLogger('salesforce')
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
        self.log.debug('url=%s, payload=%s' % (server_url, payload))
        rsp = requests.post(server_url + '/services/oauth2/token', data=payload,
                            headers={'content-type': 'application/x-www-form-urlencoded'})
        payload = json.loads(rsp.text)
        if 'error' in payload:
            raise Exception(payload['error_description'])
        self.log.debug('payload=%s' % (rsp.text,))
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

    def record_count(self, sobject: str, query_filter: str = None):
        soql = 'select count() from ' + sobject
        if filter:
            soql += ' where ' + query_filter
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/query/'
        self.log.debug('get %s', fullurl)
        response = self.client.get(fullurl, params={'q': soql})
        result_payload = response.json()
        if response.status_code != 200:
            self.log.error(f'query error {response.status_code}, {response.reason}')
            self.log.error(result_payload)
            return
        return result_payload['totalSize']

    def query(self, soql: str):
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/query/'
        self.log.debug('get %s', fullurl)
        response = self.client.get(fullurl, params={'q': soql})
        result_payload = response.text
        if response.status_code != 200:
            self.log.error(f'query error {response.status_code}, {response.reason}')
            self.log.error(result_payload)
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

    def _invoke_get(self, url, url_params):
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/{url}'
        self.log.debug('get %s', fullurl)
        response = self.client.get(fullurl, params=url_params)
        result_payload = response.text
        response.raise_for_status()
        data = json.loads(result_payload)
        return data
