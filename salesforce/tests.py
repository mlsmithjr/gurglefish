
from django.test import TestCase
from .sfapicached import SFClientCached
from .sfapi import SFClient
import datetime
from .querytools import prepareFetchPackage, fetcher, soqlfetcher
import random


consumer_key='3MVG9RHx1QGZ7OsjkJy8naE3ZBaR5FL1ISOcQVumkw.GIjcSqjLpB_WaZC6tyzrahTjSMv8LaoZNFbD_.btoD'
consumer_secret='4057585844817217438'
svcurl = 'https://cs20.salesforce.com'
token='TOA2kCnlOek6SrZv9Zqz4pjQ'

class CacheTest(TestCase):
    def test_caching(self):
        agent = SFClientCached(str(random.randint(0,1000)))
        agent.login(consumer_key, consumer_secret, 'masmith@redhat.com.fte0', 'redhat234' + token, svcurl)
        solist = agent.getSobjectList()
        self.assertEqual(agent.cache_hits, 0)
        solist = agent.getSobjectList()
        self.assertEqual(agent.cache_hits, 1, 'Expected 1 cache hit')

        acctdef = agent.get_field_list('Account')
        acctdef = agent.get_field_list('Account')
        self.assertEqual(agent.cache_hits, 2, 'Expected 2 cache hits')

class FetcherTest(TestCase):
    def test_fetch(self):
        client = SFClient()
        client.login(consumer_key, consumer_secret, 'masmith@redhat.com.fte0', 'redhat234' + token, svcurl)
        then = datetime.datetime.now() + datetime.timedelta(hours=-2)
        now = datetime.datetime.now()
        pkg = prepareFetchPackage(client, 'Account', ['Name','Id'], then, now)
        for rec in fetcher(client, pkg):
            print(rec['Name'])

    def test_soql_fetch(self):
        client = SFClient()
        client.login(consumer_key, consumer_secret, 'masmith@redhat.com.fte0', 'redhat234' + token, svcurl)
        then = datetime.datetime.now() + datetime.timedelta(days = -40)
        now = datetime.datetime.now()
        fields = ['Name','Id', 'Global_Region__c', 'BillingCountry']
        for rec in soqlfetcher(client, 'Account', fields, then, now, 'ispartner=true'):
            pass
            #print('{0}  {1}  {2}'.format(rec['Name'], rec['BillingCountry'], rec['Global_Region__c']))

