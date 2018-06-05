from __future__ import print_function

import argparse
import json
import os
import sys
from datetime import datetime

try:
    import yaml
except ImportError:
    yaml = None

from . import schedulers
from .dbconn import DataStorage, DataSchema


def init(args):
    ds = DataSchema.load_from_file(args.schema)
    args.data_storage.prepare(ds)


def clear(args):
    args.data_storage.clear(remove_tables=args.remove_tables)


def _sanitize_job_id(args):
    if args.jobid:
        return int(args.jobid)
    else:
        return schedulers.auto_detect_job_id()


def get(args):
    job_id = _sanitize_job_id(args)
    value = args.data_storage.get_value(job_id, args.field)
    print(value)


def set(args):
    job_id = _sanitize_job_id(args)

    if args.value == '-':
        value = sys.stdin.read()
    else:
        value = args.value
    args.data_storage.set_value(job_id, args.field, value)


def setnow(args):
    job_id = _sanitize_job_id(args)
    args.data_storage.set_value(job_id, args.field, datetime.now())


def setdict(args):
    job_id = _sanitize_job_id(args)

    if args.file == '-':
        fp = sys.stdin
        if args.dialect is None:
            args.dialect = 'json'
    else:
        fp = open(args.file, 'rb')
        if args.dialect is None:
            if args.file.endswith(".json"):
                args.dialect = 'json'
            elif args.file.endswith(".yaml"):
                args.dialect = 'pickle'
            else:
                args.dialect = 'json'

    if args.dialect == 'json':
        d = json.load(fp)
    elif args.dialect == 'yaml':
        if yaml is None:
            raise ImportError("No YAML library available, cannot parse YAML documents")
        d = yaml.load(fp)

    fields, values = zip(*(d.items()))
    args.data_storage.set_values(job_id, fields, values)


def main():
    parser = argparse.ArgumentParser("tad4bj")
    parser.add_argument('--database', '-d', action='store',
                        default=os.path.expanduser(os.getenv(
                            "TAD4BJ_DATABASE", "~/tad4bj.db")),
                        help='path to database file (default: environ variable '
                             '$TAD4BJ_DATABASE or ~/tad4bj.db')
    parser.add_argument('--table', '-t', action='store',
                        help='name of the table to work with. If not present, tad4bj will'
                             'try to autodetect it from the jobname of the scheduler')
    parser.add_argument('--verbose', '-v', help='verbose output')

    subparsers = parser.add_subparsers(help='available commands')

    parser_init = subparsers.add_parser('init', help="Initialize a table in the database")
    parser_init.set_defaults(func=init)
    parser_init.add_argument('schema', action='store',
                             help='Schema file for the table that will be initialized')

    parser_clear = subparsers.add_parser('clear')
    parser_clear.set_defaults(func=clear)
    parser_clear.add_argument('--remove-tables', '-r', action='store_true',
                              default=False,
                              help='Remove content and tables --requires `init` '
                                   'before the table can be used again')

    jobid_args = ('--jobid', '-j')
    jobid_kwargs = {
        'action': 'store',
        'help': 'Job identifier that will be used as row id. '
                'If not present, tad4bj will try to autodetect'
                'it from the jobid of the scheduler'
    }

    parser_get = subparsers.add_parser('get')
    parser_get.set_defaults(func=get)
    parser_get.add_argument(*jobid_args, **jobid_kwargs)
    parser_get.add_argument('field', action='store',
                            help='Name of the field that will be retrieved')

    parser_set = subparsers.add_parser('set')
    parser_set.set_defaults(func=set)
    parser_set.add_argument(*jobid_args, **jobid_kwargs)
    parser_set.add_argument('field', action='store',
                            help='Name of the field that will be set')
    parser_set.add_argument('value', action='store',
                            help='Value that will be assigned to the field')

    parser_setdict = subparsers.add_parser('setdict')
    parser_setdict.set_defaults(func=setdict)
    parser_setdict.add_argument(*jobid_args, **jobid_kwargs)
    parser_setdict.add_argument('--dialect', '-d', choices=['yaml', 'json'],
                                help='Serialization format of the dictionary used')
    parser_setdict.add_argument('file', action='store',
                                help='Path containing a file with a dictionary that will '
                                     'be used to assign values to fields')

    parser_setnow = subparsers.add_parser('setnow')
    parser_setnow.set_defaults(func=setnow)
    parser_setnow.add_argument(*jobid_args, **jobid_kwargs)
    parser_setnow.add_argument('field', action='store',
                               help='Name of the (date-)field that will be set to "now"')

    args = parser.parse_args()

    if not args.table:
        args.table = schedulers.auto_detect_table_name()

    args.data_storage = DataStorage(args.database, args.table)

    args.func(args)


if __name__ == '__main__':
    main()
