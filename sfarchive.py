import json

import DriverManager
import tools
from db.mdatadb import MDEngine
from salesforce.sfapi import SFClient
from schema import SchemaManager
from sfexport import SFExporter
from sfimport import SFImporter
import argparse
import os

def debug(o):
    s = json.dumps(o, sort_keys=True, indent=4, separators=(',', ': '))
    print(s)

def load_file_items(filename):
    with open(filename, 'r') as f:
        line_list = f.readlines()
        stripped_list = [line.strip() for line in line_list if len(line) > 0]
        return stripped_list

def make_arg_list(args):
    final_args = []
    for arg in args:
        if len(arg) == 0:
            continue
        if arg.startswith('@'):
            final_args.extend(load_file_items(arg[1:]))
        else:
            final_args.append(arg)
    return final_args


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="Environment/DB settings name", metavar="env_name", required=True)
    parser.add_argument("--sync", help="sync table updates", nargs="*", metavar="table")
    parser.add_argument("--schema", help="export database schema", nargs="*", metavar="table")
    parser.add_argument("--create", help="create missing tables", action="store_true")
    parser.add_argument("--exports", help="export full table data", nargs="+", metavar="table")
    parser.add_argument("--imports", help="load full table data", nargs="+", metavar="table")
    parser.add_argument("--updates", help="export table data updates", nargs="+", metavar="table")
    args = parser.parse_args()


    envname = args.env

#    env, dbmgr, sf = tools.setup_env(envname)
    context = tools.setup_env(envname)
    schema_mgr = SchemaManager(context)

    if not args.sync is None:
        exp = SFExporter(context)
        if len(args.sync) > 0:
            final_args = make_arg_list(args.sync)
            exp.sync_tables(schema_mgr, final_args)
        else:
            exp.sync_tables(schema_mgr)

    if args.schema:
        if len(args.schema) > 0:
            final_args = make_arg_list(args.schema)
            schema_mgr.exportSObject(final_args)
        else:
            schema_mgr.exportSObjects()

    if args.create:
        schema_mgr.create_tables()

    if args.exports and len(args.exports) > 0:
        exp = SFExporter(context)
        for tablename in args.exports:
            exp.export_copy(tablename)

    if args.imports and len(args.imports) > 0:
        imp = SFImporter(context.config_env.dbname)
        for tablename in args.imports:
            count = imp.bulk_load(context.dbdriver, tablename)
            print('loaded {} records'.format(count))

    if args.updates and len(args.updates) > 0:
        exp = SFExporter()
        for tablename in args.updates:
            stamp = dbmgr.getMaxTimestamp(tablename)
            if not stamp is None:
                print('stamp=' + stamp)
            exp.export_copy(dbmgr, sf, tablename, timestamp=stamp)


