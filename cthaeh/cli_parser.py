import argparse

from cthaeh import __version__


parser = argparse.ArgumentParser(description='Cthaeh')

#
# subparser for sub commands
#
subparser = parser.add_subparsers(dest='subcommand')

#
# Argument Groups
#
cthaeh_parser = parser.add_argument_group('core')
logging_parser = parser.add_argument_group('logging')


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
