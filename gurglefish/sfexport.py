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
import os
import datetime
import sys
from multiprocessing import Process, JoinableQueue, Queue, Value

import arrow

from gurglefish import FileManager
from gurglefish import tools
from gurglefish.context import Context
from gurglefish.objects.sobject import ColumnMap
from gurglefish.schema import SFSchemaManager
from gurglefish.objects.files import LocalTableConfig
from gurglefish.sfapi import SFClient, SFQueryTooLarge

__author__ = 'mark'


class ExportThread(Process):
    def __init__(self, queue: Queue, env_name: str):
        super().__init__(daemon=True)
        self.queue = queue
        self.env_name = env_name
        self.ctx = tools.setup_env(env_name)
        self.schema_mgr = SFSchemaManager(self.ctx)
        self.filemgr = self.ctx.filemgr
        self.sfclient = self.ctx.sfclient

    def run(self):
        db = self.ctx.dbdriver
        log = logging.getLogger(self.name)
        try:
            table_configs = self.ctx.filemgr.get_configured_tables()
            while not self.queue.empty():
                job = self.queue.get()
                try:
                    table_name = job['table_name'].lower()
                    just_sample = job['just_sample']

                    this_table: LocalTableConfig = None
                    for table_config in table_configs:
                        if table_config.name == table_name:
                            this_table = table_config
                            break

                    if this_table is None:
                        log.error(f'Configuration for {table_name} not found in config.json - skipping')
                        continue

                    if not db.table_exists(table_name):
                        self.schema_mgr.create_table(table_name)

                    total_size = self.ctx.sfclient.record_count(table_name)

                    if this_table.use_bulkapi:
                        if total_size > 200_000:
                            self.ctx.sfclient.add_header('Sforce-Enable-PKChunking', 'chunkSize=5000')
                        else:
                            self.ctx.sfclient.drop_header('Sforce-Enable-PKChunking')
                        #
                        # Salesforce does an annoying thing - datetime fields retrieve via bulk api are in
                        # millis-since-epoch rather than the usual ISO string format.  So, we need to convert
                        # them back. Get the map and remove everything but datetime fields (for speed).
                        #
                        colmap: [ColumnMap] = self.ctx.filemgr.get_sobject_map(table_name)
                        dtmap: [ColumnMap] = list()
                        for col in iter(colmap):
                            if col.field_type == 'datetime':
                                dtmap.append(col)

                        log.info(f'Exporting {total_size} records in {table_name} using bulk query (may take longer)')
                        with db.create_exporter(table_name, self.ctx, just_sample) as exporter:
                            for rec in self.ctx.sfclient.bulk_query(table_name, exporter.soql()):
                                # replace all the numeric timestamps with correct strings
                                for col in dtmap:
                                    epoch = rec.get(col.sobject_field, None)
                                    if epoch is not None:
                                        dt = datetime.datetime.fromtimestamp(epoch / 1000)
                                        rec[col.sobject_field] = tools.sf_timestamp(dt)
                                exporter.write(rec)
                    else:
                        log.info(f'Exporting {table_name}')
                        with db.create_exporter(table_name, self.ctx, just_sample) as exporter:
                            for rec in self.ctx.sfclient.query(exporter.soql()):
                                exporter.write(rec)

                                if exporter.counter % 5000 == 0 and sys.stdout.isatty():
                                    print('{}: exporting {} records: {:.0f}%\r'.format(exporter.sobject_name,
                                                                               total_size,
                                                                               (exporter.counter / total_size) * 100),
                                                                               end='\r', flush=True)
                finally:
                    self.queue.task_done()
        finally:
            db.close()


class SyncThread(Process):
    def __init__(self, queue: Queue, env_name: str, filemgr: FileManager, sfclient: SFClient, total_calls: Value,
                 scrub=False):
        super().__init__(daemon=True)
        self.queue = queue
        self.filemgr = filemgr
        self.sfclient = sfclient
        self.env_name = env_name

        self.context = tools.setup_env(env_name)
        self.schema_mgr = SFSchemaManager(self.context)
        self.filemgr = self.context.filemgr
        self.sfclient = self.context.sfclient
        self.total_calls: Value = total_calls
        self.force_scrub = scrub

    def run(self):
        db = self.context.dbdriver
        log = logging.getLogger(self.name)
        try:
            while not self.queue.empty():
                try:
                    self.sfclient.calls = 0
                    job = self.queue.get()
                    jobid = job['jobid']
                    tabledef: LocalTableConfig = job['table']
                    sobject_name = tabledef.name.lower()

                    log.info(f'Checking {sobject_name} schema for changes')
                    proceed = self.schema_mgr.update_sobject_definition(sobject_name,
                                                                        allow_add=tabledef.auto_create_columns,
                                                                        allow_drop=tabledef.auto_drop_columns)
                    if not proceed:
                        print(f'sync of {sobject_name} skipped due to warnings')
                        continue

                    timestamp = self.context.dbdriver.max_timestamp(sobject_name)
                    soql = self.context.filemgr.get_sobject_query(sobject_name)

                    xlate_handler = self.filemgr.load_translate_handler(sobject_name)
                    new_sync = False
                    if timestamp is not None:
                        soql += " where SystemModStamp >= {}".format(tools.sf_timestamp(timestamp))
                        soql += " order by SystemModStamp ASC"
                        log.info(f'start sync {sobject_name} changes after {timestamp}')
                    else:
                        soql += ' order by SystemModStamp ASC'
                        log.info(f'start full download of {sobject_name}')
                        new_sync = True
                    with db.cursor as cur:
                        counter = 0
                        # journal = self.filemgr.create_journal(sobject_name)
                        try:
                            sync_start = datetime.datetime.now()
                            inserted = 0
                            updated = 0
                            deleted = 0
                            for rec in self.sfclient.query(soql, not new_sync):
                                del rec['attributes']
                                if rec.get('IsDeleted', False):
                                    deleted += db.delete(cur, sobject_name, rec['Id'][0:15])
                                    continue
                                trec = xlate_handler.parse(rec)

                                try:
                                    i, u = db.upsert(cur, sobject_name, trec, None)
                                    if i:
                                        inserted += 1
                                    if u:
                                        updated += 1
                                except Exception as ex:
                                    # with open('/tmp/debug.json', 'w') as x:
                                    #     x.write(json.dumps(trec, indent=4, default=tools.json_serial))
                                    raise ex

                                if i or u:
                                    counter += 1
                                    if counter % 5000 == 0:
                                        log.info(f'{sobject_name} processed {counter}')
                                    if counter % 10000 == 0:
                                        db.commit()
                            db.commit()

                            # scrub deleted records
                            if tabledef.auto_scrub == "always" or self.force_scrub:
                                deleted += self.scrub_deletes(cur, sobject_name)

                            self.total_calls.value += self.sfclient.calls
                            log.info(f'end sync {sobject_name}: {inserted} inserts, {updated} updates, {deleted} deletes')
                            log.info(f'API calls used for {sobject_name}: {self.sfclient.calls}')

                            if counter > 0:
                                db.insert_sync_stats(jobid, sobject_name, sync_start, datetime.datetime.now(), timestamp,
                                                     inserted, updated, deleted, self.sfclient.calls)
                        except SFQueryTooLarge:
                            log.error(f'Query for {sobject_name} too large for REST API - switch to bulkapi to continue')

                        except Exception as ex:
                            db.rollback()
                            raise ex
                finally:
                    self.queue.task_done()
        finally:
            db.close()

    def scrub_deletes(self, cur, sobject_name: str) -> int:
        db = self.context.dbdriver
        log = logging.getLogger(self.name)
        table_file = os.path.join('/tmp', sobject_name + '.sobject')
        sobject_file = os.path.join('/tmp', sobject_name + '.table')
        try:
            db.dump_ids(sobject_name, table_file)
            self.context.sfapi.dump_ids(sobject_name, sobject_file)

            # lets 'diff' the two lists, finding deleted rows to purge from local database
            with open(table_file, 'r') as local:
                with open(sobject_file, 'r') as remote:
                    left = set(local.readlines())
                    right = set(remote.readlines())
                    ids_to_delete = left - right
                    for i in ids_to_delete:
                        db.delete(cur, sobject_name, i.strip())
            os.unlink(sobject_file)
            os.unlink(table_file)
            return len(ids_to_delete)

        except Exception as ex:
            log.error(ex)
        return 0


class SFExporter:
    def __init__(self, context: Context):
        self.context = context
        self.storagedir = context.filemgr.exportdir
        os.makedirs(self.storagedir, exist_ok=True)
        self.log = logging.getLogger('main')

    def sync_tables(self, schema_mgr: SFSchemaManager, scrub=False):
        table_config: [LocalTableConfig] = self.context.filemgr.get_configured_tables()
        if table_config is None:
            self.log.error('No configuration found - Use --init to create and then edit')
            return
        tablelist: [LocalTableConfig] = [table for table in table_config if table.enabled]
        if len(tablelist) == 0:
            self.log.warning('No tables enabled for sync')
            return
        jobid = self.context.dbdriver.start_sync_job()
        queue: Queue = JoinableQueue()
        try:
            self.log.info('Building table sync queue')
            for table in tablelist:
                tablename = table.name.lower()
                if not self.context.dbdriver.table_exists(tablename):
                    schema_mgr.create_table(tablename)

                queue.put({'jobid': jobid, 'table': table})

            self.log.info(f'Allocating {self.context.env.threads} thread(s)')
            pool: [SyncThread] = list()
            total_api_calls: Value = Value('i', 0)
            for i in range(0, self.context.env.threads):
                job = SyncThread(queue, self.context.envname, self.context.filemgr, self.context.sfclient,
                                 total_api_calls, scrub)
                job.start()
                pool.append(job)

            for t in pool:
                self.log.debug(f'Waiting on thread {t.name}')
                t.join()
            if not queue.empty():
                self.log.warning('All threads finished before queue was drained')
            self.log.info(f"Total API calls used during sync: {total_api_calls.value}")

        finally:
            self.context.dbdriver.finish_sync_job(jobid)
            self.context.dbdriver.clean_house(arrow.now().shift(months=-2).datetime)

    def export_tables(self, table_list: [str], just_sample=False):
        queue: Queue = JoinableQueue()
        for tablename in table_list:
            tablename = tablename.lower()
            queue.put({'table_name': tablename, 'just_sample': just_sample})

        thread_count = min(self.context.env.threads, queue.qsize())
        self.log.info(f'Allocating {thread_count} thread(s)')
        pool: [ExportThread] = list()
        for i in range(0, thread_count):
            job = ExportThread(queue, self.context.envname)
            job.start()
            pool.append(job)
        for t in pool:
            self.log.debug(f'Waiting on thread {t.name}')
            t.join()
        if not queue.empty():
            self.log.warning('All threads finished before queue was drained')
