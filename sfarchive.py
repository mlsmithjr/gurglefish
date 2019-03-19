
import tools
from schema import SchemaManager
from sfexport import SFExporter
from sfimport import SFImporter
import argparse
import logging.config
import yaml

from objects.files import LocalTableConfig


def load_file_items(filename):
    with open(filename, 'r') as f:
        line_list = f.readlines()
        stripped_list = [line.strip() for line in line_list if len(line) > 0]
        return stripped_list


def make_arg_list(args_list):
    processed_args = []
    for arg in args_list:
        if len(arg) == 0:
            continue
        if arg.startswith('@'):
            processed_args.extend(load_file_items(arg[1:]))
        else:
            processed_args.append(arg)
    return processed_args


def load_log_config():
    with open('logging.yml', 'r') as configfile:
        _logconfig = yaml.load(configfile.read(), Loader=yaml.FullLoader)
        return _logconfig


if __name__ == '__main__':
    parser = argparse.ArgumentParser(epilog='@file arguments designate a file containing actual arguments, one per line')
    group = parser.add_mutually_exclusive_group()
    parser.add_argument("env", help="Environment/DB settings name", metavar="env_name")
    group.add_argument("--sync", help="sync table updates", nargs="*", metavar="sobject|@file")
    group.add_argument("--schema", help="load sobject schema and create tables", nargs="*", metavar="sobject|@file")
    group.add_argument("--export", help="export full sobject data", nargs="+", metavar="sobject|@file")
    group.add_argument("--load", help="load/import full table data, table must be empty", nargs="*", metavar="sobject|@file")
    parser.add_argument("--inspect", help="inspect objects", action="store_true")
    parser.add_argument("--sample", help="sample data (500 rows)", action="store_true")
    group.add_argument("--init", help="initialize configuration for given environment", action="store_true")
    parser.add_argument("--enable", help="enable one or more tables to sync", nargs="+", metavar="sobject|@file")
    parser.add_argument("--disable", help="enable one or more tables to sync", nargs="+", metavar="sobject|@file")
    args = parser.parse_args()

    logconfig = load_log_config()
    logging.config.dictConfig(logconfig)
    logger = logging.getLogger('simple')

    envname = args.env

    context = tools.setup_env(envname)
    if context is None:
        exit(1)

    schema_mgr = SchemaManager(context)

    if args.init:
        if context.filemgr.get_configured_tables() is not None:
            print('Initialization halted, config.yml already exists. Please remove manually to start over')
            exit(1)
        sobject_list = schema_mgr.inspect()
        sobjectconfig = []
        for sobject in sobject_list:
            sobjectconfig.append(LocalTableConfig({'name': sobject['name'].lower(), 'enabled': False}))
        context.filemgr.save_configured_tables(sobjectconfig)
        print('config created')

    if args.inspect:
        thelist = schema_mgr.inspect()
        for entry in thelist:
            logger.info(entry['name'])

    if args.enable is not None:
        table_config: [LocalTableConfig] = context.filemgr.get_configured_tables()
        to_enable = [a.lower() for a in make_arg_list(args.enable)]
        for entry in table_config:
            if entry.name in to_enable:
                print(f"enabling {entry.name}")
                entry.enabled = True
        context.filemgr.save_configured_tables(table_config)

    if args.disable is not None:
        table_config: [LocalTableConfig] = context.filemgr.get_configured_tables()
        to_disable = [a.tolower() for a in make_arg_list(args.disable)]
        for entry in table_config:
            if entry.name in to_disable:
                print(f"disabling {entry.name}")
                entry.enabled = False
        context.filemgr.save_configured_tables(table_config)

    if args.sync is not None:
        exp = SFExporter(context)
        exp.sync_tables(schema_mgr)

    if args.schema is not None:
        if len(args.schema) > 0:
            final_args = make_arg_list(args.schema)
            schema_mgr.prepare_sobjects(final_args)
        else:
            schema_mgr.prepare_configured_sobjects()

    if args.export is not None:
        exp = SFExporter(context)
        table_list = make_arg_list(args.export)
        for tablename in table_list:
            exp.export_copy_sql(tablename, schema_mgr, just_sample=args.sample)

    if args.load and len(args.load) > 0:
        imp = SFImporter(context, schema_mgr)
        table_list = make_arg_list(args.load)
        for tablename in table_list:
            logger.info('loading {}'.format(tablename))
            count = imp.bulk_load(tablename)
            logger.info('loaded {} records'.format(count))



