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
import argparse
import logging.config
import os
import sys
from typing import Dict

from gurglefish import tools
from gurglefish.schema import SFSchemaManager
from gurglefish.sfexport import SFExporter

from gurglefish.sfimport import SFImporter


def main():
    parser = argparse.ArgumentParser(
        epilog='@file arguments designate a file containing actual arguments, one per line')
    group = parser.add_mutually_exclusive_group()
    parser.add_argument("env", help="Environment/DB settings name", metavar="env_name")
    group.add_argument("--sync", help="sync table updates", nargs="*", metavar="sobject|@file")
    group.add_argument("--schema", help="load sobject schema and create tables if missing", nargs="*", metavar="sobject|@file")
    group.add_argument("--export", help="export full sobject data to file", nargs="+", metavar="sobject|@file")
    group.add_argument("--load", help="load/import full table data, table must be empty", nargs="*",
                       metavar="sobject|@file")
    group.add_argument("--dump", help="dump contents of table to file", nargs="+", metavar="table|@file")
    parser.add_argument("--inspect", help="list available sobjects", action="store_true")
    #parser.add_argument("--sample", help="sample data (500 rows)", action="store_true")
    group.add_argument("--init", help="create config.json file for given environment", action="store_true")
    parser.add_argument("--enable", help="enable one or more tables to sync", nargs="+", metavar="sobject|@file")
    parser.add_argument("--disable", help="disable one or more tables from sync", nargs="+", metavar="sobject|@file")
    parser.add_argument("--scrub", help="force scrub of deleted records", action="store_true")
    args = parser.parse_args()

    envname = args.env

    context = tools.setup_env(envname)
    if context is None:
        exit(1)
    logger = logging.getLogger('main')
    schema_mgr = SFSchemaManager(context)

    if args.init:
        schema_mgr.initialize_config(envname)
        sys.exit(0)

    if args.inspect:
        thelist: [Dict] = schema_mgr.inspect()
        for entry in thelist:
            logger.info(entry['name'])
        sys.exit(0)

    if args.enable is not None:
        schema_mgr.enable_table_sync(args.enable, True)
        sys.exit(0)

    if args.disable is not None:
        schema_mgr.enable_table_sync(args.enable, False)
        sys.exit(0)

    if args.sync is not None:
        exp = SFExporter(context)
        exp.sync_tables(schema_mgr, args.scrub)

    if args.schema is not None:
        if len(args.schema) > 0:
            final_args = tools.make_arg_list(args.schema)
            schema_mgr.prepare_sobjects(final_args)
        else:
            schema_mgr.prepare_configured_sobjects()

    if args.export is not None:
        exp = SFExporter(context)
        table_list = tools.make_arg_list(args.export)
        exp.export_tables(table_list, just_sample=args.sample)

    if args.dump is not None:
        table_list = tools.make_arg_list(args.dump)
        for table in table_list:
            export_file = os.path.join(context.filemgr.exportdir, table + '.exp.gz')
            context.dbdriver.export_native(table, export_file)

    if args.load and len(args.load) > 0:
        imp = SFImporter(context, schema_mgr)
        table_list = tools.make_arg_list(args.load)
        for tablename in table_list:
            logger.info('loading {}'.format(tablename))
            count = imp.bulk_load(tablename)
            logger.info('loaded {} records'.format(count))


if __name__ == '__main__':
    main()

