"""
Module to create Help/Usage and to deal with arguments.
"""

import argparse
import sys

#############
# Functions #
#############

def parse_args():
    """
    Generate the argument parsers and and Usage/Help by invoking the functions
    requested.
    """
    # Get CLI arguments
    args = sys.argv[1:]

    # Initiate parser and subparser objects
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    # Options meant to be used by any sub-command
    add_general_options(parser)

    # Sub-command options argument sub-parsers
    add_stats_subparser(subparser)
    add_reports_subparser(subparser)

    # Print help if no arguments were passed
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser.parse_args(args)


def add_general_options(parser):
    """
    General optional arguments
    """

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        dest='verbose',
        help="Show on stderr events according to loglevel."
    )


def add_reports_subparser(subparser):
    """
    Optional arguments for the 'reports' sub-command.
    """
    # Initiate parser.
    parser = subparser.add_parser(
        'reports',
        help="In development"
    )

    # Set the cmd to execute
    parser.set_defaults(cmd='reports')

    # Logging options
    group_logging = parser.add_argument_group("Logging options")
    group_logging.add_argument(
        '--logfile',
        action='store',
        default='/tmp/dynafed_storagestats.log',
        dest='logfile',
        help="Set logfile's path. " \
             "Default: /tmp/dynafed_storagestats.log"
    )
    group_logging.add_argument(
        '--loglevel',
        action='store',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='WARNING',
        dest='loglevel',
        help="Set log output level. " \
        "Default: WARNING."
    )

def add_stats_subparser(subparser):
    """
    Optional arguments for the 'stats' sub-command.
    """
    # Initiate parser.
    parser = subparser.add_parser(
        'stats',
        help="Obtain and output storage stats."
    )

    # Set the cmd to execute
    parser.set_defaults(cmd='stats')

    # General options
    parser.add_argument(
        '-c', '--config',
        action='store',
        default=['/etc/ugr/conf.d'],
        dest='config_path',
        nargs='*',
        help="Path to UGR's endpoint .conf files or directories. " \
             "Accepts any number of arguments. " \
             "Default: '/etc/ugr/conf.d'."
    )
    parser.add_argument(
        '-e', '--endpoint',
        action='store',
        default=True,
        dest='endpoint',
        help="Choose endpoint to check. " \
             "If not present, all endpoints will be checked."
    )

    # Logging options
    group_logging = parser.add_argument_group("Logging options")
    group_logging.add_argument(
        '--logfile',
        action='store',
        default='/tmp/dynafed_storagestats.log',
        dest='logfile',
        help="Set logfile's path. " \
             "Default: /tmp/dynafed_storagestats.log"
    )
    group_logging.add_argument(
        '--loglevel',
        action='store',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='WARNING',
        dest='loglevel',
        help="Set log output level. " \
        "Default: WARNING."
    )

    # Memcached Options
    group_memcached = parser.add_argument_group("Memcached Options")
    group_memcached.add_argument(
        '--memhost',
        action='store',
        default='127.0.0.1',
        dest='memcached_ip',
        help="IP or hostname of memcached instance." \
             "Default: 127.0.0.1"
    )
    group_memcached.add_argument(
        '--memport',
        action='store',
        default='11211',
        dest='memcached_port',
        help="Port of memcached instance. " \
             "Default: 11211"
    )

    # Output Options
    group_output = parser.add_argument_group("Output options")
    group_output.add_argument(
        '--debug',
        action='store_true',
        default=False,
        dest='debug',
        help="Declare to enable debug output on stdout."
    )
    group_output.add_argument(
        '-m', '--memcached',
        action='store_true',
        default=False,
        dest='output_memcached',
        help="Declare to enable uploading storage stats to memcached."
    )
    group_output.add_argument(
        '-j', '--json',
        action='store',
        const="dynafed_storagestats.json",
        default=False,
        dest='to_json',
        nargs='?',
        help="Set to output stats to json file. Add argument to set filename." \
             "Default: dynafed_storagestats.json"
             "!!In development!!"
    )
    group_output.add_argument(
        '-o', '--output-dir',
        action='store',
        default='.',
        dest='output_path',
        help="Set output directory for flags -j, -x and -p. " \
             "Default: '.'"
    )
    group_output.add_argument(
        '-p', '--plain',
        action='store',
        const="dynafed_storagestats.txt",
        default=False,
        dest='to_plaintext',
        nargs='?',
        help="Set to output stats to plain txt file. Add argument to set filename." \
             "Default: dynafed_storagestats.txt"
    )
    group_output.add_argument(
        '--stdout',
        action='store_true',
        default=False,
        dest='output_stdout',
        help="Set to output stats on stdout."
    )

    group_output.add_argument(
        '-x', '--xml',
        action='store',
        const="dynafed_storagestats.xml",
        default=False,
        dest='output_xml',
        nargs='?',
        help="Set to output stats to json file. Add argument to set filename." \
             "Default: dynafed_storagestats.json"
             "!!In development!!"
    )
