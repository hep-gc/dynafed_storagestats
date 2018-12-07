#!/bin/python3

"""
Runner to gather storage share information.
"""

import argparse
from multiprocessing.dummy import Pool as ThreadPool

from dynafed_storagestats import configloader
from dynafed_storagestats import helpers
from dynafed_storagestats import output

################
## Help/Usage ##
################

PARSER = argparse.ArgumentParser()
SUBPARSERS = PARSER.add_subparsers(help='sub-command help')

### General optional arguments ###

PARSER.add_argument(
    '-v', '--verbose',
    action='store_true',
    default=False,
    dest='verbose',
    help="Show on stderr events according to loglevel."
)

### Stats Sub-command ###
PARSER_STATS = SUBPARSERS.add_parser(
    'stats',
    help="Subcommand to contact StorageEndpoints and output stats."
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

### Reports Sub-command ###
PARSER_REPORTS = SUBPARSERS.add_parser(
    'reports',
    help="Subcommand to generate reports."
)

# Last needed to bring all arguments together.
ARGS = PARSER.parse_args()

########
# Main #
########

def main():
    """
    Runner to gather storage share information.
    """
    # Setup logger
    helpers.setup_logger(
        logfile=ARGS.logfile,
        loglevel=ARGS.loglevel,
        verbose=ARGS.verbose,
    )

    # Get list of StorageShare objects from the configuration files.
    storage_shares = configloader.get_storage_shares(
        ARGS.config_path
    )

    # Flag storage shares that have been marked offline by Dynafed.
    helpers.get_connectionstats(
        storage_shares,
        ARGS.memcached_ip,
        ARGS.memcached_port
    )

    # Create a list of StorageEndpoint objects with the StorageShares to check,
    # based on user input or unique URL's.
    storage_endpoints = configloader.get_storage_endpoints(
        storage_shares,
        ARGS.endpoint
    )

    # This tuple is necessary for the starmap function to send multiple
    # arguments to the process_storagestats function.
    storage_endpoints_list_and_args_tuple = [
        (storage_endpoint, ARGS) for storage_endpoint in storage_endpoints
    ]

    # Process each storage endpoints' shares using multithreading.
    # Number of threads to use.
    pool = ThreadPool(len(storage_endpoints_list_and_args_tuple))
    pool.starmap(
        helpers.process_storagestats,
        storage_endpoints_list_and_args_tuple
    )


#### Output #####

    # Print all StorageEndpoints's StorageShares stats to the standard output.
    if ARGS.output_stdout:
        output.to_stdout(storage_endpoints, ARGS)

    # Create StAR Storagestats XML files for each storage share.
    if ARGS.output_xml:
        output.to_xml(storage_endpoints, ARGS.to_xml, ARGS.output_path)

    # Create json file with storagestats
    if ARGS.to_json:
        output.to_json(storage_endpoints, ARGS.to_json, ARGS.output_path)

    # Create txt file with storagestats
    if ARGS.to_plaintext:
        output.to_plaintext(storage_endpoints, ARGS.to_plaintext, ARGS.output_path)


#############
# Self-Test #
#############

if __name__ == '__main__':
    main()
