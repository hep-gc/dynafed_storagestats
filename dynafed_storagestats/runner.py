#!/usr/bin/env python3

"""Runner to gather storage share information."""

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

################
# Sub-Commands #
################

def reports(ARGS):
    """Execute the 'reports' sub-command.

    Run the main() function with 'reports -h' arguments to see help.

    Arguments:
    ARGS -- argparse object from dynafed_storagestats.args.parse_args()

    """
    # Get list of StorageShare objects from the configuration files.
    storage_shares = configloader.get_storage_shares(
        ARGS.config_path
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
        ARGS.config_path
    )

    # Obtain current stats, if any, from memcached.
    current_stats = helpers.get_currentstats(
        storage_shares,
        ARGS.memcached_ip,
        ARGS.memcached_port
    )

    if current_stats is not None:
        # Flag storage shares that have been marked offline by Dynafed.
        helpers.check_connectionstats(storage_shares, current_stats)

        # Flag storage shares that are under-due their period.
        helpers.check_frequency(storage_shares, current_stats)

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
