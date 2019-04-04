import argparse
import logging.config
import sys
from typing import Dict

import tools
from schema import SFSchemaManager
from sfexport import SFExporter
from sfimport import SFImporter


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        epilog='@file arguments designate a file containing actual arguments, one per line')
    group = parser.add_mutually_exclusive_group()
    parser.add_argument("env", help="Environment/DB settings name", metavar="env_name")
    group.add_argument("--sync", help="sync table updates", nargs="*", metavar="sobject|@file")
    group.add_argument("--schema", help="load sobject schema and create tables", nargs="*", metavar="sobject|@file")
    group.add_argument("--export", help="export full sobject data", nargs="+", metavar="sobject|@file")
    group.add_argument("--load", help="load/import full table data, table must be empty", nargs="*",
                       metavar="sobject|@file")
    parser.add_argument("--inspect", help="inspect objects", action="store_true")
    parser.add_argument("--sample", help="sample data (500 rows)", action="store_true")
    group.add_argument("--init", help="initialize configuration for given environment", action="store_true")
    parser.add_argument("--enable", help="enable one or more tables to sync", nargs="+", metavar="sobject|@file")
    parser.add_argument("--disable", help="enable one or more tables to sync", nargs="+", metavar="sobject|@file")
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
        exp.sync_tables(schema_mgr)

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

    if args.load and len(args.load) > 0:
        imp = SFImporter(context, schema_mgr)
        table_list = tools.make_arg_list(args.load)
        for tablename in table_list:
            logger.info('loading {}'.format(tablename))
            count = imp.bulk_load(tablename)
            logger.info('loaded {} records'.format(count))
