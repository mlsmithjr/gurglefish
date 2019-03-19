import gzip
import json
import logging
import operator
import os
import sys
import datetime

import arrow

import tools
from context import Context
from schema import SFSchemaManager
from objects.files import LocalTableConfig

__author__ = 'mark'


class SFExporter:
    logger = logging.getLogger('main')

    def __init__(self, context: Context):
        self.context = context
        self.storagedir = context.filemgr.exportdir
        os.makedirs(self.storagedir, exist_ok=True)

    def sync_tables(self, schema_mgr: SFSchemaManager):
        table_config: [LocalTableConfig] = self.context.filemgr.get_configured_tables()
        tablelist: [LocalTableConfig] = [table for table in table_config if table.enabled]
        jobid = self.context.dbdriver.start_sync_job()
        try:
            for table in tablelist:
                tablename = table.name.lower()
                print(f'sync {tablename}')
                if not self.context.dbdriver.table_exists(tablename):
                    schema_mgr.create_table(tablename)
                else:
                    # check for column changes and process accordingly
                    proceed = schema_mgr.update_sobject(tablename, allow_add=table.auto_create_columns,
                                                        allow_drop=table.auto_drop_columns)
                    if not proceed:
                        print('sync of {} skipped due to warnings'.format(tablename))
                        return

                tstamp = self.context.dbdriver.max_timestamp(tablename)
                self.etl(jobid, self.context.filemgr.get_sobject_query(tablename), tablename, timestamp=tstamp)
        finally:
            self.context.dbdriver.finish_sync_job(jobid)
            self.context.dbdriver.clean_house(arrow.now().shift(months=-2).datetime)

    def etl(self, jobid, soql, sobject_name, timestamp=None):

        sobject_name = sobject_name.lower()
        dbdriver = self.context.dbdriver

        xlate_handler = self.context.filemgr.load_translate_handler(sobject_name)
        if timestamp is not None:
            soql += " where SystemModStamp > {}".format(tools.sf_timestamp(timestamp))
            soql += " order by SystemModStamp ASC"
        cur = dbdriver.cursor
        counter = 0
        journal = self.context.filemgr.create_journal(sobject_name)
        try:
            sync_start = datetime.datetime.now()
            inserted = 0
            updated = 0
            for rec in self.context.sfclient.query(soql):
                del rec['attributes']
                trec = xlate_handler.parse(rec)

                try:
                    i, u = dbdriver.upsert(cur, sobject_name, trec, journal)
                    if i:
                        inserted += 1
                    if u:
                        updated += 1
                except Exception as ex:
                    with open('/tmp/debug.json', 'w') as x:
                        x.write(json.dumps(trec, indent=4, default=tools.json_serial))
                    raise ex

                if i or u:
                    counter += 1
                    if counter % 100 == 0:
                        print('processed {}'.format(counter))
                    if counter % 1000 == 0:
                        dbdriver.commit()
            dbdriver.commit()
            print('processed {}'.format(counter))
            if counter > 0:
                dbdriver.insert_sync_stats(jobid, sobject_name, sync_start, datetime.datetime.now(), timestamp,
                                           inserted,
                                           updated)
        except Exception as ex:
            dbdriver.rollback()
            raise ex
        finally:
            cur.close()
            journal.close()

    def export_copy_sql(self, sobject_name, schema_mgr: SFSchemaManager, just_sample=False, timestamp=None):

        sobject_name = sobject_name.lower()
        if not self.context.dbdriver.table_exists(sobject_name):
            schema_mgr.create_table(sobject_name)

        xlate_handler = self.context.filemgr.load_translate_handler(sobject_name)

        fieldlist = self.context.filemgr.get_sobject_map(sobject_name)
        fieldmap = dict((f['db_field'].lower(), f) for f in fieldlist)

        tablefields = self.context.dbdriver.get_table_fields(sobject_name)
        tablefields = sorted(tablefields.values(), key=operator.itemgetter('ordinal_position'))
        soqlfields = [fm['sobject_field'] for fm in fieldmap.values()]

        soql = 'select {} from {}'.format(','.join(soqlfields), sobject_name)
        if timestamp is not None:
            soql += ' where SystemModStamp > {0}'.format(tools.sf_timestamp(timestamp))
        if just_sample:
            self.logger.info('sampling 500 records max')
            soql += ' limit 500'
        counter = 0
        total_size = self.context.sfclient.record_count(sobject_name)
        if sys.stdout.isatty():
            print('{}: exporting {} records: 0%'.format(sobject_name, total_size), end='\r', flush=True)
        else:
            print(f'{sobject_name}: exporting {total_size} records')
        with gzip.open(os.path.join(self.storagedir, sobject_name + '.exp.gz'), 'wb', compresslevel=5) as export:
            for rec in self.context.sfclient.query(soql):
                trec = xlate_handler.parse(rec)
                record = self.context.dbdriver.format_for_export(trec, tablefields, fieldmap)
                export.write(record)
                counter += 1
                if counter % 2000 == 0 and sys.stdout.isatty():
                    print('{}: exporting {} records: {:.0f}%\r'.format(sobject_name, total_size,
                                                                       (counter / total_size) * 100), end='\r',
                          flush=True)
            export.close()
            if sys.stdout.isatty():
                print("\nexported {} records{}".format(counter, ' ' * 10))
