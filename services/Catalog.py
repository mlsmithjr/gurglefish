from flask_restful import Resource
from flask import request
from DriverManager import Manager
from config import storagedir
from db.journal import Journal
from salesforce.sfapi import SFClient


from services.payloads import ChangePayload

__author__ = 'mark'

from db.mdatadb import MDEngine




class GetMappingCatalog(Resource):
    def post(self):
        payload = request.json['db']
        dbname = payload['dbname']
        mde = MDEngine()
        print('dbname=' + dbname)
        env = mde.get_db_env(dbname)
        dm = Manager()
        driver = dm.getDriver('postgresql')
        driver.connect(env)
        mapped_tables = driver.get_mapped_tables()
        mapped_sobjects = set([table['sobject_name'] for table in mapped_tables])
#        table_sobject_name_map = dict()
#        sobject_table_name_map = dict()
#        for table in mapped_tables:
#            table_sobject_name_map[table['table_name']] = table['sobject_name']
#            sobject_table_name_map[table['sobject_name']] = table['table_name']

        sfc = SFClient()
        try:
            sfc.login(env.consumer_key, env.consumer_secret, env.sflogin, env.sfpassword, env.authurl)
        except Exception as ex:
            print(ex)
            return {'success':False, 'message': 'Salesforce: ' + str(ex)}

        sobjects = sfc.getSobjectList()
        available_sobjects = set([sobject['name'] for sobject in sobjects])

        net_new_sobjects = available_sobjects - mapped_sobjects
        missing_sobjects = mapped_sobjects - available_sobjects

        #
        # build results
        #
        mapping_state = []
        for new_sobject in net_new_sobjects:
            mapping_state.append(dict(sobject=new_sobject, table=None, selected=False, deleted=False))
        for table in mapped_tables:
            sobject_name = table['sobject_name']
            table_name = table['table_name']
            d = dict(sobject=sobject_name, table=table_name, selected=True, deleted=sobject_name in missing_sobjects)
            mapping_state.append(d)

        #
        # returns list of dict
        #
        mde.close()
        return {'payload': mapping_state, 'success': True}

class ValidateSObjectMappings(Resource):
    def post(self):
        print(request.json)
        db = request.json['db']
        dbname = db['dbname']
        payload = request.json['sobject']
        sobjectName = payload['sobject']
        mde = MDEngine()
        print('sobjectName=' + sobjectName)
        env = mde.get_db_env(dbname)
        dm = Manager()
        driver = dm.getDriver('postgresql')
        driver.connect(env)
        table_name, mapped_fields = driver.get_field_map(sobjectName)
        if mapped_fields is None or len(mapped_fields) == 0:
            sobject_names = set()
        else:
            sobject_names = set([field['sobject_field'] for field in mapped_fields])

        sfc = SFClient()
        sfc.login(env.consumer_key, env.consumer_secret, env.login, env.password, env.authurl)
        sobjectFields = sfc.get_field_list(sobjectName)
        fieldDict = dict([ (field['name'], field) for field in sobjectFields ])
        available_fields = set([field['name'] for field in sobjectFields])

        net_new_fields = available_fields - sobject_names
        missing_fields = sobject_names - available_fields

        #
        # build results
        #
        mapping_state = []
        for new_field in net_new_fields:
            fielddef = fieldDict[new_field]
            mapping_state.append(dict(field=new_field, db_name=None, selected=False, deleted=False, type=fielddef['type'], length=fielddef['length']))
        if mapped_fields:
            for field in mapped_fields:
                sobject_name = field['sobject_name']
                db_name = field['db_name']
                mapping_state.append(dict(field=sobject_name, db_name=db_name, selected=True, deleted=sobject_name in missing_fields, type='', length='0'))

        #
        # returns list of dict
        #
        mde.close()
        return {'payload': mapping_state, 'success': True}

class GetSObjectMappings(Resource):
    def post(self):
        print(request.json)
        db = request.json['db']
        dbname = db['dbname']
        payload = request.json['sobject']
        sobjectName = payload['sobject']
        mde = MDEngine()
        print('sobjectName=' + sobjectName)
        env = mde.get_db_env(dbname)
        dm = Manager()
        driver = dm.getDriver('postgresql')
        driver.connect(env)
        table_name, mapped_fields = driver.get_field_map(sobjectName)
        if mapped_fields is None or len(mapped_fields) == 0:
            sobject_names = set()
        else:
            sobject_names = set([field['sobject_field'] for field in mapped_fields])

        sfc = SFClient()
        sfc.login(env.consumer_key, env.consumer_secret, env.login, env.password, env.authurl)
        sobjectFields = sfc.get_field_list(sobjectName)
        fieldDict = dict([ (field['name'], field) for field in sobjectFields ])
        available_fields = set([field['name'] for field in sobjectFields])

        net_new_fields = available_fields - sobject_names
        missing_fields = sobject_names - available_fields

        #
        # build results
        #
        mapping_state = []
        for new_field in net_new_fields:
            fielddef = fieldDict[new_field]
            mapping_state.append(dict(field=new_field, db_name=None, selected=False, deleted=False, type=fielddef['type'], length=fielddef['length']))
        if mapped_fields:
            for field in mapped_fields:
                sobject_name = field['sobject_name']
                db_name = field['db_name']
                mapping_state.append(dict(field=sobject_name, db_name=db_name, selected=True, deleted=sobject_name in missing_fields, type='', length='0'))

        #
        # returns list of dict
        #
        mde.close()
        return {'payload': mapping_state, 'success': True}



class SaveMappings(Resource):
    def post(self):
        payload = ChangePayload(request.json)
        mde = MDEngine()
        env = mde.get_db_env(payload.dbname)
        dm = Manager()
        driver = dm.getDriver('postgresql')
        driver.connect(env)

        jrnl = Journal()

        sfc = SFClient()
        sfc.login(env.consumer_key, env.consumer_secret, env.sflogin, env.sfpassword, env.authurl)

        for change in payload.changes:
            if not change.dirty: continue
            if change.deleted:
                jrnl.action({ 'drop-table': change.table })
                continue
            if not change.selected: continue

            if change.table is None or not driver.is_table_mapped(change.sobject):
                jrnl.action({ 'create-table': change })
                continue

            #
            # examine the columns
            #
            _, mapped_table_fields = driver.get_field_map(change.sobject)
            mapped_columns = [c['sobject_field'] for c in mapped_table_fields]
            sobjectFields = sfc.get_field_list(change.sobject)
            sobject_field_map = dict([ (field['name'], field) for field in sobjectFields ])

            for field in change.fields:
                if field.deleted:
                    jrnl.action({'drop-column': { 'table': change.table, 'column': field.field } })
                    continue

                if field.selected and not field.field in mapped_columns:
                    f = sobject_field_map.get(field.field)
                    jrnl.action({'add-column': { 'sobject': change.sobject, 'column': f}})

        return { 'success': True }

#
# Sample Save() payload:
# {
#     "db":{
#         "login":"masmith@redhat.com.fte0",
#         "dbname":"fte0",
#         "authurl":"https://test.salesforce.com"
#     },
#     "changes":[
#         {
#             "deleted":false,
#             "table":null,
#             "sobject":"Address__c",
#             "selected":true,
#             "fields":[
#                 {
#                     "deleted":false,
#                     "field":"Not_validated_by_Google__c",
#                     "selected":false,
#                     "db_name":null,
#                     "type":"boolean",
#                     "length":0
#                 },
#                 {
#                     "deleted":false,
#                     "field":"Identifying_Address__c",
#                     "selected":false,
#                     "db_name":null,
#                     "type":"boolean",
#                     "length":0
#                 },
#                 {
#                     "deleted":false,
#                     "field":"LastModifiedDate",
#                     "selected":false,
#                     "db_name":null,
#                     "type":"datetime",
#                     "length":0
#                 },
#                 {
#                     "deleted":false,
#                     "field":"IsDeleted",
#                     "selected":false,
#                     "db_name":null,
#                     "type":"boolean",
#                     "length":0
#                 },
#                 {
#                     "deleted":false,
#                     "field":"SystemModstamp",
#                     "selected":false,
#                     "db_name":null,
#                     "type":"datetime",
#                     "length":0
#                 }
#             ],
#             "dirty":true
#         }
#     ]
# }
