import json
import logging
import os
import datetime
from multiprocessing import Lock
from queue import Queue
from threading import Thread

import arrow

import FileManager
import tools
from context import Context
from schema import SFSchemaManager
from objects.files import LocalTableConfig
from sfapi import SFClient

__author__ = 'mark'


class ExportThread(Thread):
    def __init__(self, queue: Queue, env_name: str, ctx: Context, db_lock: Lock):
        super().__init__(daemon=True)
        self.queue = queue
        self.ctx = ctx
        self.env_name = env_name
        self.db_lock: Lock = db_lock

    def run(self):
        db = None
        log = logging.getLogger(self.name)
        try:
            db = tools.get_db_connection(self.env_name)
            while not self.queue.empty():
                job = self.queue.get()
                try:
                    table_name = job['table_name']
                    schema_mgr = job['schema_mgr']
                    just_sample = job['just_sample']

                    if not db.table_exists(table_name):
                        schema_mgr.create_table(table_name)

                    log.info(f'Exporting {table_name}')
                    db.export_native(table_name, self.ctx, just_sample)
                finally:
                    self.queue.task_done()
        finally:
            db.close()


class SyncThread(Thread):
    def __init__(self, queue: Queue, env_name: str, filemgr: FileManager, sfclient: SFClient, db_lock: Lock):
        super().__init__(daemon=True)
        self.queue = queue
        self.filemgr = filemgr
        self.sfclient = sfclient
        self.env_name = env_name
        self.db_lock: Lock = db_lock

    def run(self):
        db = None
        log = logging.getLogger(self.name)
        try:
            db = tools.get_db_connection(self.env_name)
            while not self.queue.empty():
                job = self.queue.get()
                jobid = job['jobid']
                soql = job['soql']
                sobject_name = job['sobject_name']
                timestamp = job['timestamp']

                sobject_name = sobject_name.lower()
                log.info(f'start sync {sobject_name}')

                xlate_handler = self.filemgr.load_translate_handler(sobject_name)
                if timestamp is not None:
                    soql += " where SystemModStamp > {}".format(tools.sf_timestamp(timestamp))
                    soql += " order by SystemModStamp ASC"
                cur = db.cursor
                counter = 0
                journal = self.filemgr.create_journal(sobject_name)
                try:
                    sync_start = datetime.datetime.now()
                    inserted = 0
                    updated = 0
                    for rec in self.sfclient.query(soql):
                        del rec['attributes']
                        trec = xlate_handler.parse(rec)

                        try:
                            self.db_lock.acquire()
                            i, u = db.upsert(cur, sobject_name, trec, journal)
                            if i:
                                inserted += 1
                            if u:
                                updated += 1
                        except Exception as ex:
                            # with open('/tmp/debug.json', 'w') as x:
                            #     x.write(json.dumps(trec, indent=4, default=tools.json_serial))
                            raise ex
                        finally:
                            self.db_lock.release()

                        if i or u:
                            counter += 1
                            if counter % 1000 == 0:
                                log.info(f'{sobject_name} processed {counter}')
                            if counter % 1000 == 0:
                                db.commit()
                    db.commit()
                    log.info(f'end sync {sobject_name}: {inserted} inserts, {updated} updates')
                    if counter > 0:
                        db.insert_sync_stats(jobid, sobject_name, sync_start, datetime.datetime.now(), timestamp,
                                             inserted,
                                             updated)
                except Exception as ex:
                    db.rollback()
                    raise ex
                finally:
                    self.queue.task_done()
                    cur.close()
                    journal.close()
        finally:
            db.close()
            return


class SFExporter:
    def __init__(self, context: Context):
        self.context = context
        self.storagedir = context.filemgr.exportdir
        os.makedirs(self.storagedir, exist_ok=True)
        self.log = logging.getLogger('main')

    def sync_tables(self, schema_mgr: SFSchemaManager):
        table_config: [LocalTableConfig] = self.context.filemgr.get_configured_tables()
        if table_config is None:
            self.log.error('No configuration found - Use --init to create and then edit')
            return
        tablelist: [LocalTableConfig] = [table for table in table_config if table.enabled]
        if len(tablelist) == 0:
            self.log.warning('No tables enabled for sync')
            return
        jobid = self.context.dbdriver.start_sync_job()
        queue: Queue = Queue()
        shared_lock: Lock = Lock()
        try:
            self.log.info('Building table sync queue')
            for table in tablelist:
                tablename = table.name.lower()
                if not self.context.dbdriver.table_exists(tablename):
                    schema_mgr.create_table(tablename)
                else:
                    # check for column changes and process accordingly
                    proceed = schema_mgr.update_sobject_definition(tablename, allow_add=table.auto_create_columns,
                                                                   allow_drop=table.auto_drop_columns)
                    if not proceed:
                        print('sync of {} skipped due to warnings'.format(tablename))
                        return

                tstamp = self.context.dbdriver.max_timestamp(tablename)
                soql = self.context.filemgr.get_sobject_query(tablename)
                queue.put({'jobid': jobid, 'soql': soql, 'sobject_name': tablename, 'timestamp': tstamp})
                # self.etl(jobid, soql, tablename, timestamp=tstamp)

            self.log.info(f'Allocating {self.context.env.threads} thread(s)')
            pool: [SyncThread] = list()
            for i in range(0, self.context.env.threads):
                job = SyncThread(queue, self.context.envname, self.context.filemgr, self.context.sfclient, shared_lock)
                job.start()
                pool.append(job)
            for t in pool:
                self.log.debug(f'Waiting on thread {t.name}')
                t.join()
            if not queue.empty():
                self.log.warning('All threads finished before queue was drained')
            # queue.join()

        finally:
            self.context.dbdriver.finish_sync_job(jobid)
            self.context.dbdriver.clean_house(arrow.now().shift(months=-2).datetime)

    def etl(self, jobid, soql, sobject_name, timestamp=None):

        sobject_name = sobject_name.lower()
        dbdriver = self.context.dbdriver

        self.log.info(f'sync {sobject_name}')
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
        if not self.context.driver.table_exists(sobject_name):
            schema_mgr.create_table(sobject_name)
        self.context.driver.export_native(sobject_name, just_sample, timestamp)

    def export_tables(self, table_list: [str], schema_mgr: SFSchemaManager, just_sample=False):
        queue: Queue = Queue()
        shared_lock: Lock = Lock()
        for tablename in table_list:
            tablename = tablename.lower()
            queue.put({'table_name': tablename, 'just_sample': just_sample})

        self.log.info(f'Allocating {self.context.env.threads} thread(s)')
        pool: [SyncThread] = list()
        for i in range(0, self.context.env.threads):
            job = ExportThread(queue, self.context.envname, self.context, shared_lock)
            job.start()
            pool.append(job)
        for t in pool:
            self.log.debug(f'Waiting on thread {t.name}')
            t.join()
        if not queue.empty():
            self.log.warning('All threads finished before queue was drained')
