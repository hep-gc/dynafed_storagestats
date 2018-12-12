"""
Module to create Help/Usage and to deal with arguments.
"""

import argparse
import sys

def parse_args(ARGS=sys.argv[1:]):
    PARSER = argparse.ArgumentParser()
    SUBPARSERS = PARSER.add_subparsers()

    ### General optional arguments ###
    PARSER.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        dest='verbose',
        help="Show on stderr events according to loglevel."
    )

    add_stats_subparser(SUBPARSERS)
    add_reports_subparser(SUBPARSERS)

    return PARSER.parse_args(ARGS)


def add_stats_subparser(SUBPARSERS):
    PARSER_STATS = SUBPARSERS.add_parser(
        'stats',
        help="Obtain and output storage stats."
    )

    PARSER_STATS.add_argument(
        '-c', '--config',
        action='store',
        default=['/etc/ugr/conf.d'],
        dest='config_path',
        nargs='*',
        help="Path to UGR's endpoint .conf files or directories. " \
             "Accepts any number of arguments. " \
             "Default: '/etc/ugr/conf.d'."
    )
    PARSER_STATS.add_argument(
        '-e', '--endpoint',
        action='store',
        default=True,
        dest='endpoint',
        help="Choose endpoint to check. " \
             "If not present, all endpoints will be checked."
    )

    GROUP_LOGGING = PARSER_STATS.add_argument_group("Logging options")
    GROUP_LOGGING.add_argument(
        '--logfile',
        action='store',
        default='/tmp/dynafed_storagestats.log',
        dest='logfile',
        help="Set logfile's path. " \
             "Default: /tmp/dynafed_storagestats.log"
    )
    GROUP_LOGGING.add_argument(
        '--loglevel',
        action='store',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='WARNING',
        dest='loglevel',
        help="Set log output level. " \
        "Default: WARNING."
    )

    GROUP_MEMCACHED = PARSER_STATS.add_argument_group("Memcached Options")
    GROUP_MEMCACHED.add_argument(
        '--memhost',
        action='store',
        default='127.0.0.1',
        dest='memcached_ip',
        help="IP or hostname of memcached instance." \
             "Default: 127.0.0.1"
    )
    GROUP_MEMCACHED.add_argument(
        '--memport',
        action='store',
        default='11211',
        dest='memcached_port',
        help="Port of memcached instance. " \
             "Default: 11211"
    )

    GROUP_OUTPUT = PARSER_STATS.add_argument_group("Output options")
    GROUP_OUTPUT.add_argument(
        '--debug',
        action='store_true',
        default=False,
        dest='debug',
        help="Declare to enable debug output on stdout."
    )
    GROUP_OUTPUT.add_argument(
        '-m', '--memcached',
        action='store_true',
        default=False,
        dest='output_memcached',
        help="Declare to enable uploading storage stats to memcached."
    )
    GROUP_OUTPUT.add_argument(
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
    GROUP_OUTPUT.add_argument(
        '-o', '--output-dir',
        action='store',
        default='.',
        dest='output_path',
        help="Set output directory for flags -j, -x and -p. " \
             "Default: '.'"
    )
    GROUP_OUTPUT.add_argument(
        '-p', '--plain',
        action='store',
        const="dynafed_storagestats.txt",
        default=False,
        dest='to_plaintext',
        nargs='?',
        help="Set to output stats to plain txt file. Add argument to set filename." \
             "Default: dynafed_storagestats.txt"
    )
    GROUP_OUTPUT.add_argument(
        '--stdout',
        action='store_true',
        default=False,
        dest='output_stdout',
        help="Set to output stats on stdout."
    )

    GROUP_OUTPUT.add_argument(
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

def add_reports_subparser(SUBPARSERS):
    PARSER_REPORTS = SUBPARSERS.add_parser(
        'reports',
        help="In development"
    )
