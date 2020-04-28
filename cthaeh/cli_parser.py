import argparse
import pathlib

from cthaeh import __version__
from cthaeh.commands import (
    do_main,
    do_initialize_database,
)


parser = argparse.ArgumentParser(description='Cthaeh')
parser.set_defaults(func=do_main)

#
# subparser for sub commands
#
subparser = parser.add_subparsers(dest='subcommand')

#
# Argument Groups
#
cthaeh_parser = parser.add_argument_group('core')
loading_parser = parser.add_argument_group('loading')
database_parser = parser.add_argument_group('database')
logging_parser = parser.add_argument_group('logging')
jsonrpc_parser = parser.add_argument_group('jsonrpc')


#
# Globals
#
cthaeh_parser.add_argument('--version', action='version', version=__version__)

#
# Logging configuration
#
logging_parser.add_argument(
    '-l',
    '--log-level',
    type=int,
    dest="log_level",
    help=(
        "Configure the logging level. "
    ),
)

#
# Database
#
database_url_parser = database_parser.add_mutually_exclusive_group()

database_url_parser.add_argument(
    '--database-url',
    type=str,
    dest="database_url",
    help=(
        "The database url for the databse that should be used."
    )
)
database_url_parser.add_argument(
    '--db-memory',
    const='sqlite:///:memory:',
    action="store_const",
    dest="database_url",
    help=(
        "Use an in-memory sqlite3 database."
    )
)

initialize_database_parser = subparser.add_parser(
    'init-db',
    help='Initialize the database schema',
)
initialize_database_parser.set_defaults(func=do_initialize_database)

#
# Data loading
#
loading_parser.add_argument(
    '--concurrency',
    type=int,
    dest="concurrency",
    default=3,
    help=(
        "The level of concurrency which which to pull data"
    )
)
loading_parser.add_argument(
    '--start-block',
    type=int,
    dest="start_block",
    help=(
        "The starting block number to load"
    )
)
loading_parser.add_argument(
    '--end-block',
    dest="end_block",
    type=int,
    help=(
        "The ending block number to load.  If not present loading follow the "
        "head of the chain once it is reached."
    )
)

#
# JSON-RPC
#
jsonrpc_parser.add_argument(
    '--ipc-path',
    type=pathlib.Path,
    help=(
        "The path for the IPC socket that will be used for the JSON-RPC server"
    ),
)
jsonrpc_parser.add_argument(
    '--disable-jsonrpc',
    action="store_true",
    help=(
        "Disable the JSON-RPC server"
    )
)
