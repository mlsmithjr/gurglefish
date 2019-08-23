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
import logging
import json
import operator
import time
from typing import Dict, Optional

import requests
from fastcache import lru_cache

from gurglefish.objects.sobject import SObjectFields

MAX_BATCH_SIZE = 100
_API_VERSION = '44.0'


class SFQueryTooLarge(Exception):
    def __init__(self):
        super().__init__(self)


class JobBatch:

    def __init__(self, batchinfo: Dict, parent):
        self.parent = parent
        self.batch_id = batchinfo['id']
        self.batchinfo = batchinfo

    @property
    def state(self) -> str:
        return self.batchinfo['state']

    @property
    def id(self) -> str:
        return self.batch_id

    def get_results(self):
        response = self.parent.client.get(
            f'{self.parent.service_url}/services/async/{_API_VERSION}/job/{self.parent.job_id}/batch/{self.batch_id}/result')
        response.raise_for_status()
        self.client.calls += 1

        result = json.loads(response.text)
        for resultid in result:
            response = self.parent.client.get(
                f'{self.parent.service_url}/services/async/{_API_VERSION}/job/{self.parent.job_id}/batch/{self.batch_id}/result/{resultid}',
                stream=True)
            self.client.calls += 1
            doc = ''
            for chunk in response.iter_lines():
                if chunk is None:
                    continue
                chunk = chunk.decode('utf-8')
                if chunk[0] == '[':
                    doc = '{'
                elif chunk[0] == '}':
                    doc += '}'
                    yield json.loads(doc)
                    doc = '{'
                else:
                    doc += chunk

    def refresh(self):
        if self.state != 'Completed':
            response = self.parent.client.get(
                f'{self.parent.service_url}/services/async/{_API_VERSION}/job/{self.parent.job_id}/batch/{self.batch_id}')
            self.client.calls += 1
            response.raise_for_status()
            self.batchinfo = json.loads(response.text)
            return True
        return False

    def wait_for_start(self, timeout: int = 180) -> bool:
        for i in range(0, timeout, 30):
            time.sleep(30)
            self.refresh()
            if self.state in ('Completed', 'NotProcessed'):
                return True
            if self.state == 'Failed':
                return False
        return False


class BulkJob:
    JOB_OP_QUERY = 'query'
    JOB_OP_INSERT = 'insert'
    JOB_OP_UPDATE = 'update'
    JOB_TYPE_CSV = 'CSV'
    JOB_TYPE_XML = 'XML'
    JOB_TYPE_JSON = 'JSON'
    CONCUR_PARALLEL = 'Parallel'
    CONCUR_SERIAL = 'Serial'

    def __init__(self, jobinfo: Dict, client, service_url):
        self.job_id = jobinfo['id']
        self.jobinfo = jobinfo
        self.client = client
        self.service_url = service_url
        self.pending: [JobBatch] = list()
        self.complete: [str] = list()

    def create_batch(self, payload):
        response = self.client.post(f'{self.service_url}/services/async/{_API_VERSION}/job/{self.job_id}/batch',
                                    data=json.dumps(payload, indent=4),
                                    headers={'Content-Type': 'application/json; charset=UTF-8'})
        self.client.calls += 1
        response.raise_for_status()
        result = json.loads(response.text)
        return JobBatch(result, self)

    @property
    def batch_count(self) -> int:
        return self.jobinfo['numberBatchesTotal'] + self.jobinfo['numberBatchesQueued']

    def bulk_query(self, soql: str):
        response = self.client.post(f'{self.service_url}/services/async/{_API_VERSION}/job/{self.job_id}/batch',
                                    data=soql + ' ',
                                    headers={'Content-Type': 'application/json; charset=UTF-8'})
        self.client.calls += 1
        response.raise_for_status()
        result = json.loads(response.text)
        batch = JobBatch(result, self)
        self.pending.append(batch)
        return batch

    def is_done(self) -> bool:
        self.refresh()
        self.get_batches()
        all = self.jobinfo['numberBatchesQueued'] + self.jobinfo['numberBatchesInProgress'] + \
              self.jobinfo['numberBatchesFailed'] + self.jobinfo['numberBatchesCompleted']

        return len(self.complete) >= all and len(self.pending) == 0 and self.jobinfo['numberBatchesQueued'] == 0

#        return 0 < all < self.jobinfo['numberBatchesTotal'] \
#               and len(self.complete) < len(self.pending) \
#               and self.jobinfo['numberBatchesQueued'] == 0

    #
    # Returns:
    # {
    #   "apexProcessingTime": 0,
    #   "apiActiveProcessingTime": 0,
    #   "apiVersion": 36.0,
    #   "concurrencyMode": "Parallel",
    #   "contentType": "JSON",
    #   "createdById": "005D0000001b0fFIAQ",
    #   "createdDate": "2015-12-15T20:45:25.000+0000",
    #   "id": "750D00000004SkGIAU",
    #   "numberBatchesCompleted": 0,
    #   "numberBatchesFailed": 0,
    #   "numberBatchesInProgress": 0,
    #   "numberBatchesQueued": 0,
    #   "numberBatchesTotal": 0,
    #   "numberRecordsFailed": 0,
    #   "numberRecordsProcessed": 0,
    #   "numberRetries": 0,
    #   "object": "Account",
    #   "operation": "insert",
    #   "state": "Open",
    #   "systemModstamp": "2015-12-15T20:45:25.000+0000",
    #   "totalProcessingTime": 0
    # }
    #
    def refresh(self):
        response = self.client.get(f'{self.service_url}/services/async/{_API_VERSION}/job/{self.job_id}')
        self.client.calls += 1
        response.raise_for_status()
        self.jobinfo = json.loads(response.text)

    #
    # Returns:
    #
    # {
    #   "batchInfo": [
    #   {
    #     "apexProcessingTime": 0,
    #     "apiActiveProcessingTime": 0,
    #     "createdDate": "2015-12-15T21:56:43.000+0000",
    #     "id": "751D00000004YGZIA2",
    #     "jobId": "750D00000004SkVIAU",
    #     "numberRecordsFailed": 0,
    #     "numberRecordsProcessed": 0,
    #     "state": "Queued",
    #     "systemModstamp": "2015-12-15T21:57:19.000+0000",
    #     "totalProcessingTime": 0
    #   }
    #   ]
    # }
    #
    def get_batches(self) -> [Dict]:
        response = self.client.get(f'{self.service_url}/services/async/{_API_VERSION}/job/{self.job_id}/batch')
        self.client.calls += 1
        response.raise_for_status()
        result = json.loads(response.text)
        self.pending: [JobBatch] = list()
        for bi in result['batchInfo']:
            if bi['id'] not in self.complete:
                if bi['state'] in ('Queued', 'Completed', 'InProgress'):
                    self.pending.append(JobBatch(bi, self))
                else:
                    self.release_batch(bi['id'])
                    print('**** unhandled state ' + bi['state'])

    def release_batch(self, batch_id: str):
        self.complete.append(batch_id)

    def get_completed_batch(self) -> Optional[JobBatch]:
        self.get_batches()
        for batch in self.pending:
            if batch.state == 'Completed':
                return batch
        return None

    def close(self):
        response = self.client.post(f'{self.service_url}/services/async/{_API_VERSION}/job/{self.job_id}',
                                    data='{"state":"Closed"}')
        self.client.calls += 1
        response.raise_for_status()
        result = json.loads(response.text)
        return result


class SFClient:

    def __init__(self):
        self.log = logging.getLogger('salesforce')
        self.access_token = None
        self.service_url = None
        self.client = None
        self._username = None
        self.calls = 0

    def login(self, consumer_key, consumer_secret, username, password, server_url):
        self._username = username
        payload = {'grant_type': 'password',
                   'username': username,
                   'password': password,
                   'client_id': consumer_key,
                   'client_secret': consumer_secret
                   }
        rsp = requests.post(server_url + '/services/oauth2/token', data=payload,
                            headers={'content-type': 'application/x-www-form-urlencoded'})
        payload = json.loads(rsp.text)
        if 'error' in payload:
            raise Exception(payload['error_description'])
        # self.log.debug('payload=%s' % (rsp.text,))
        self.construct(payload['access_token'], payload['instance_url'])

    def construct(self, token, server_url):
        self.access_token = token
        self.service_url = server_url
        self.client = requests.Session()
        self.client.headers.update({'Authorization': 'OAuth ' + token,
                                    'X-SFDC-Session': token,
                                    'Content-Type': 'application/json; charset=UTF-8',
                                    'Accept-Encoding': 'gzip, compress, deflate', 'Accept-Charset': 'utf-8'})

    def close(self):
        pass

    @lru_cache(maxsize=3, typed=False)
    def get_sobject_definition(self, name: str) -> Dict:
        sobject_doc = self._get('sobjects/{}/describe'.format(name), {})
        return sobject_doc

    @lru_cache(maxsize=1, typed=False)
    def get_sobject_list(self) -> [Dict]:
        payload = self._get('sobjects/', {})
        return payload['sobjects']

    @lru_cache(maxsize=3, typed=False)
    def get_field_list(self, sobject_name: str) -> SObjectFields:
        fielddef = self._get('sobjects/%s/describe/' % (sobject_name,), {})
        fieldlist = fielddef['fields']
        fieldlist.sort(key=operator.itemgetter('name'))
        return SObjectFields(fieldlist)

    def dump_ids(self, sobject_name, output_filename: str):
        with open(output_filename, 'w') as out:
            for rec in self.query(f'select Id from {sobject_name} order by Id'):
                out.write(rec['Id'][0:15] + '\n')

    def record_count(self, sobject: str, query_filter: str = None):
        soql = 'select count() from ' + sobject
        if query_filter:
            soql += ' where ' + query_filter
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/query/'
        self.log.debug('get %s', fullurl)
        response = self.client.get(fullurl, params={'q': soql})
        result_payload = response.json()
        if response.status_code != 200:
            self.log.error(f'query error {response.status_code}, {response.reason}')
            self.log.error(result_payload)
            return
        self.calls += 1
        return result_payload['totalSize']

    def query(self, soql: str, include_deleted=False):
        resource = 'queryAll' if include_deleted else 'query'
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/{resource}/'
        #
        # Need to make specific changes to soql to avoid upsetting Salesforce and
        # using requests built-in escaping causes problems.
        #
        soql = soql.replace('+', '%2b').replace('\n', '').replace('\r', '').replace(' ', '+')
        response = self.client.get(fullurl, params=f'q={soql}')
        result_payload = response.text
        if response.status_code == 431:
            raise SFQueryTooLarge()
        self.calls += 1
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
                self.calls += 1
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

    def _get(self, url, url_params):
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/{url}'
        response = self.client.get(fullurl, params=url_params)
        result_payload = response.text
        self.calls += 1
        if response.status_code != 200:
            self.log.debug('get %s', fullurl)
        response.raise_for_status()
        data = json.loads(result_payload)
        return data

    def _post(self, url, url_params):
        fullurl = f'{self.service_url}/services/data/v{_API_VERSION}/{url}'
        response = self.client.post(fullurl, params=url_params)
        self.calls += 1
        if response.status_code != 200:
            self.log.debug('post %s', fullurl)
        result_payload = response.text
        response.raise_for_status()
        data = json.loads(result_payload)
        return data

    def add_header(self, name: str, val: str):
        self.client.headers[name] = val

    def drop_header(self, name: str):
        if name in self.client.headers:
            del self.client.headers[name]

    def create_job(self, op, sobject_name, content_type='JSON', concur_mode=BulkJob.CONCUR_PARALLEL):
        payload = {"operation": op, "object": sobject_name, "contentType": content_type,
                   "concurrencyMode": concur_mode}
        url = f'{self.service_url}/services/async/{_API_VERSION}/job'
        self.client.headers['Content-Type'] = 'application/json; charset=UTF-8'
        response = self.client.post(url, data=json.dumps(payload),
                                    headers={'Content-Type': 'application/json; charset=UTF-8'})
        self.calls += 1
        response.raise_for_status()
        result = response.json()
        if result['state'] != 'Open':
            raise Exception('Invalid job state: {}'.format(result['state']))
        return BulkJob(result, self.client, self.service_url)

    def bulk_query(self, sobject: str, soql: str, job_id=None, timeout=600):

        if job_id is None:
            job = self.create_job(BulkJob.JOB_OP_QUERY, sobject)
            main_batch = job.bulk_query(soql + ' ')
            job.close()
            self.log.info(f'Waiting on bulk query job to start, timeout is {timeout} seconds')

            # suppress annoying requests debug logging
            logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)

            if main_batch.wait_for_start(timeout):
                counter = 0
                batches = job.batch_count
                job.release_batch(main_batch.id)
                while not job.is_done():
                    completed = job.get_completed_batch()
                    if completed is not None:
                        counter += 1
                        self.log.debug(f'Processing batch {counter} of {batches}')
                        for result in completed.get_results():
                            del result['attributes']
                            yield result
                        job.release_batch(completed.id)
                    else:
                        time.sleep(30)
            else:
                self.log.error('Timed out waiting for bulk query job to start')
        else:
            job = BulkJob({'id': job_id, 'state': 'Closed'}, self.client, self.service_url)
            counter = 1
            while not job.is_done():
                total = job.batch_count
                completed = job.get_completed_batch()
                if completed is not None:
                    self.log.debug(f'Processing batch {completed.batch_id} - {counter} of {total}')
                    for result in completed.get_results():
                        del result['attributes']
                        yield result
                    job.release_batch(completed.id)
                    counter += 1
                else:
                    time.sleep(20)
