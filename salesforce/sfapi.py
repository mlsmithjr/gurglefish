import logging
import json
import operator
from typing import List

import requests

MAX_BATCH_SIZE = 100
_API_VERSION = '40.0'


class SFError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class SFClient:
    client = None
    service_url = None
    _username = None
    _bucket = None

    def __init__(self):
        self.logger = logging.getLogger('salesforce')

    def login(self, consumer_key, consumer_secret, username, password, server_url):
        self._username = username
        self._bucket = 'sf_' + self._username
        payload = {'grant_type': 'password',
                   'username': username,
                   'password': password,
                   'client_id': consumer_key,
                   'client_secret': consumer_secret
                   }
        #self.logger.debug('url=%s, payload=%s' % (server_url, payload))
        rsp = requests.post(server_url + '/services/oauth2/token', data=payload,
                            headers={'content-type': 'application/x-www-form-urlencoded'})
        payload = json.loads(rsp.text)
        if 'error' in payload: raise Exception(payload['error_description'])
        self.logger.debug('payload=%s' % (rsp.text,))
        self.construct(payload['access_token'], payload['instance_url'])

    def construct(self, token, server_url):
        self.access_token = token
        self.service_url = server_url
        self.client = requests.Session()
        self.client.headers.update({'Authorization': 'OAuth ' + token,
                                    'Content-Type': 'application/json; charset=UTF-8',
                                    'Accept-Encoding': 'gzip', 'Accept-Charset': 'utf-8'})

    def close(self):
        pass

    def get_sobject_definition(self, name):
        sobject_doc = self._invokeGetREST('sobjects/{}/describe'.format(name), {})
        return sobject_doc

    def getSobjectList(self, omit_packages=True):
        # if CaptureManager.exists(self._bucket, 'sobjectList'):
        #    stuff = CaptureManager.fetch(self._bucket, 'sobjectList')
        #    return stuff
        sobjectList = self._invokeGetREST('sobjects/', {})
        sobjectList = sobjectList['sobjects']
        # if omit_packages:
        #    sobjectList = [sobj for sobj in sobjectList if not '__' in sobj['name']]
        # scrub out objects we can't use

        # sobjectList = [sobj for sobj in sobjectList if self.accept_sobject(sobj)]
        # CaptureManager.save(self._bucket, 'sobjectList', sobjectList)
        return sobjectList

    def get_field_list(self, sobject_name) -> List:
        """

        :param sobject_name:
        :return: list of dictionaries.
        Ex:
          {
            "autoNumber": false,
            "byteLength": 18,
            "calculated": false,
            "calculatedFormula": null,
            "cascadeDelete": false,
            "caseSensitive": false,
            "controllerName": null,
            "createable": true,
            "custom": false,
            "defaultValue": null,
            "defaultValueFormula": null,
            "defaultedOnCreate": true,
            "dependentPicklist": false,
            "deprecatedAndHidden": false,
            "digits": 0,
            "displayLocationInDecimal": false,
            "encrypted": false,
            "externalId": false,
            "extraTypeInfo": null,
            "filterable": true,
            "filteredLookupInfo": null,
            "groupable": true,
            "highScaleNumber": false,
            "htmlFormatted": false,
            "idLookup": false,
            "inlineHelpText": null,
            "label": "Created By ID",
            "length": 18,
            "mask": null,
            "maskType": null,
            "name": "CreatedById",
            "nameField": false,
            "namePointing": false,
            "nillable": false,
            "permissionable": false,
            "picklistValues": [],
            "precision": 0,
            "queryByDistance": false,
            "referenceTargetField": null,
            "referenceTo": [
              "User"
            ],
            "relationshipName": "CreatedBy",
            "relationshipOrder": null,
            "restrictedDelete": false,
            "restrictedPicklist": false,
            "scale": 0,
            "soapType": "tns:ID",
            "sortable": true,
            "type": "reference",
            "unique": false,
            "updateable": false,
            "writeRequiresMasterRead": false
          }

        """
        # if CaptureManager.exists(self._bucket, sobject_name):
        #    return CaptureManager.fetch(self._bucket, sobject_name)
        fielddef = self._invokeGetREST('sobjects/%s/describe/' % (sobject_name,), {})
        fieldlist = fielddef['fields']
        fieldlist.sort(key=operator.itemgetter('name'))
        # CaptureManager.save(self._bucket, sobject_name, fieldlist)
        return fieldlist

    def get_field_map(self, sobject_name):
        thelist = self.get_field_list(sobject_name)
        return dict((f['name'].lower(), f) for f in thelist)

    def fetchRecord(self, objectname, recid, fieldlist):
        fieldstring = ','.join(fieldlist)
        url = 'sobjects/{0}/{1}'.format(objectname, recid)
        result = self._invokeGetREST(url, {'fields': fieldstring})
        return result

    def _insertObject(self, objectName, user_params):
        self.logger.debug('insert object %s with %s' % (objectName, user_params))
        data = self._invokePostREST(objectName, json.dumps(user_params))
        return data['id'] if data else None

    def _updateObject(self, objectName, objectId, user_params):
        if not isinstance(user_params, str): user_params = json.dumps(user_params)
        self.logger.debug('updating object %s, id %s with %s' % (objectName, objectId, user_params))
        data = self._invokePatchREST(objectName, objectId, user_params)
        if not data == None: return data['records']
        return None

    def _invokePostREST(self, objectName, payload):
        if not isinstance(payload, str):
            payload = json.dumps(payload)
        self.logger.debug('post invoking /services/data/v%s/sobjects/%s' % (_API_VERSION, objectName))
        try:
            response = self.client.post(
                '%s/services/data/v%s/sobjects/%s/' % (self.service_url, _API_VERSION, objectName), data=payload)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(response)
            raise ex
        resultPayload = response.text
        if 'errorCode' in resultPayload:
            self.logger.error('response: %s', resultPayload)
        response.raise_for_status()
        data = json.loads(resultPayload)
        return data

    def _invokeGetREST(self, url, url_params):
        self.logger.debug('get invoking /services/data/v%s/%s' % (_API_VERSION, url))
        self.logger.debug('service_url=%s', self.service_url)
        response = self.client.get('%s/services/data/v%s/%s' % (self.service_url, _API_VERSION, url), params=url_params)
        resultPayload = response.text
        # self.logger.debug('http response=%s', response.text)
        response.raise_for_status()
        data = json.loads(resultPayload)
        return data

    def record_count(self, sobject, filter=None):
        soql = 'select count() from ' + sobject
        if filter:
            soql += ' where ' + filter
        response = self.client.get('%s/services/data/v%s/query/' % (self.service_url, _API_VERSION), params={'q': soql})
        resultPayload = response.json()
        if response.status_code != 200:
            print((response.status_code, response.reason))
            print(resultPayload)
            return
        return resultPayload['totalSize']

    def query(self, soql):
        self.logger.debug('invoking /services/data/v%s/query' % (_API_VERSION,))
        response = self.client.get('%s/services/data/v%s/query/' % (self.service_url, _API_VERSION), params={'q': soql})
        resultPayload = response.text
        if response.status_code != 200:
            print((response.status_code, response.reason))
            print(resultPayload)
            return
        data = json.loads(resultPayload)
        recs = data['records']
        for rec in recs:
            yield (rec)
        while 'nextRecordsUrl' in data:
            nextRecordsUrl = data['nextRecordsUrl']
            if nextRecordsUrl:
                # u = '%s/%s' % (self.service_url, nextRecordsUrl)
                # print 'NEXTURL=%s' % (u,)
                response = self.client.get('%s%s' % (self.service_url, nextRecordsUrl))
                txt = response.text
                if isinstance(txt, str):
                    resultPayload = txt
                else:
                    resultPayload = str(txt, 'utf-8')
                data = json.loads(resultPayload)
                recs = data['records']
                for rec in recs:
                    yield (rec)
            else:
                break

    def _invokePatchREST(self, objectName, objectId, url_data):
        self.logger.debug('patch invoking /services/data/v%s/sobjects/%s/%s' % (_API_VERSION, objectName, objectId))
        response = self.client.patch(
            '%s/services/data/v%s/sobjects/%s/%s/' % (self.service_url, _API_VERSION, objectName, objectId),
            data=url_data)
        self.logger.debug('response=%s', response.text)
        response.raise_for_status()


