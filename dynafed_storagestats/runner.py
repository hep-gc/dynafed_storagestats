#!/usr/bin/env python3

"""Runner to gather storage share information."""

import logging

from multiprocessing.dummy import Pool as ThreadPool

from dynafed_storagestats import args
from dynafed_storagestats import configloader
from dynafed_storagestats import helpers
from dynafed_storagestats import output

########
# Main #
########

def main():
    """Runner to gather storage share information."""
    # Generate Help/Usage and ARGS.
    ARGS = args.parse_args()

    # Setup logger
    helpers.setup_logger(
        logfile=ARGS.logfile,
        loglevel=ARGS.loglevel,
        verbose=ARGS.verbose,
    )

    # Run the specified sub-command.
    if ARGS.cmd == 'reports':
        reports(ARGS)
    elif ARGS.cmd == 'stats':
        stats(ARGS)
    elif ARGS.cmd == 'checksums':
        checksums(ARGS)


################
# Sub-Commands #
################
def checksums(ARGS):
    """Execute the 'checksums' sub-command.

    Run the main() function with 'checksums -h' arguments to see help.

    Arguments:
    ARGS -- argparse object from dynafed_storagestats.args.parse_args()

    """
    # Check that all required arguments were given.
    helpers.check_required_checksum_args(ARGS)

    # Get list of StorageShare objects from the configuration files.
    _storage_shares = configloader.get_storage_shares(
        ARGS.config_path,
        ARGS.endpoint
    )

    for _storage_share in _storage_shares:
        # Process the requested checksum action for file/object.
        if ARGS.sub_cmd == 'get':
            _checksum = helpers.process_checksums_get(
                _storage_share,
                ARGS.hash_type,
                ARGS.url
            )

            print(_checksum)

        elif ARGS.sub_cmd == 'put':
            helpers.process_checksums_put(
                _storage_share,
                ARGS.checksum,
                ARGS.hash_type,
                ARGS.url,
                force=ARGS.force
            )


def reports(ARGS):
    """Execute the 'reports' sub-command.

    Run the main() function with 'reports -h' arguments to see help.

    Arguments:
    ARGS -- argparse object from dynafed_storagestats.args.parse_args()

    """
    # Get list of StorageShare objects from the configuration files.
    storage_shares = configloader.get_storage_shares(
        ARGS.config_path,
        ARGS.endpoint
    )

    # Create a list of StorageEndpoint objects with the StorageShares to check,
    # based on user input or unique URL's.
    storage_endpoints = configloader.get_storage_endpoints(
        storage_shares
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
        helpers.process_storagereports,
        storage_endpoints_list_and_args_tuple
    )


def stats(ARGS):
    """Execute the 'stats' sub-command.

    Run the main() function with 'stats -h' arguments to see help.

    Deals with obtaining config file(s) settings, contacting the storage
    endpoints and output the data using functions from the dynafed_storagestats
    package.

    Arguments:
    ARGS -- argparse object from dynafed_storagestats.args.parse_args()

    """
    # Get list of StorageShare objects from the configuration files.
    storage_shares = configloader.get_storage_shares(
        ARGS.config_path,
        ARGS.endpoint
    )

    # Skip checks of '-f/--force' flag is set.
    if not ARGS.force:
        # Obtain current stats, if any, from memcached.
        _connection_stats, _storage_stats = helpers.get_currentstats(
            storage_shares,
            ARGS.memcached_ip,
            ARGS.memcached_port
        )

        if _connection_stats is not None:
            # Flag storage shares that have been marked offline by Dynafed.
            helpers.check_connectionstats(storage_shares, _connection_stats)

        if _storage_stats is not None:
            # Flag storage shares that are under-due their period.
            helpers.check_frequency(storage_shares, _storage_stats)

            # Update storage_share objects with information obtained from memcache.
            helpers.update_storage_share_storagestats(storage_shares, _storage_stats)

    # Create a list of StorageEndpoint objects with the StorageShares to check,
    # based on user input or unique URL's.
    storage_endpoints = configloader.get_storage_endpoints(
        storage_shares
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
    # if ARGS.output_xml:
    #     output.to_xml(storage_endpoints, ARGS.to_xml, ARGS.output_path)

    # Create json file with storagestats
    if ARGS.to_json:
        output.to_json(storage_endpoints, ARGS.to_json, ARGS.output_path)

    # Create txt file with storagestats
    if ARGS.to_plaintext:
        output.to_plaintext(storage_endpoints, ARGS.to_plaintext, ARGS.output_path)

    if ARGS.to_bdii:
        output.to_ldif(storage_endpoints, ARGS.to_bdii, ARGS.output_path)


#############
# Self-Test #
#############

if __name__ == '__main__':
    main()
