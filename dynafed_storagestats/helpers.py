"""Helper functions used by the other modules."""

import logging, logging.handlers

from dynafed_storagestats import exceptions
from dynafed_storagestats import memcache
from dynafed_storagestats import output

#############
# Functions #
#############

def convert_size_to_bytes(size):
    """Convert given size to bytes.

    Arguments:
    size - string containing number and optionally storage space unit.
           Examples: 1000, 1KiB, 10tb.

    Returns:
    Bytes as integer.

    """
    ############# Creating loggers ################

    ###############################################

    _multipliers = {
        'kib': 1024,
        'mib': 1024**2,
        'gib': 1024**3,
        'tib': 1024**4,
        'pib': 1024**5,
        'kb': 1000,
        'mb': 1000**2,
        'gb': 1000**3,
        'tb': 1000**4,
        'pb': 1000**5,
    }

    for _suffix in _multipliers:
        if size.lower().endswith(_suffix):
            return int(size[0:-len(_suffix)]) * _multipliers[_suffix]

    else:
        if size.lower().endswith('b'):
            return int(size[0:-1])

    try:
        return int(size)

    except ValueError: # for example "1024x"
        print('Malformed input for setting: "storagestats.quota"')
        exit()


def get_connectionstats(storage_share_objects, memcached_ip='127.0.0.1', memcached_port='11211'):
    """Check given StorageShares' connection status in memcache and flag them.

    Check if each StorageShare in the list has information about its status in
    memcached, which would have been uploaded by Dynafed's periodic test.
    Endpoints without this information will be considered online.

    Arguments:
    storage_share_objects -- list of dynafed_storagestats StorageShare objects.
    memcached_ip   -- memcached instance IP.
    memcahced_port -- memcached instance Port.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    _logger.info(
        "Checking memcached server %s:%s for storage_share connection stats.",
        memcached_ip,
        memcached_port
    )

    try:
        # Obtain the latest index number used by UGR and typecast to str if
        # needed. Different versions of memcache module return bytes.
        _idx = memcache.get(
            'Ugrpluginstats_idx',
            memcached_ip,
            memcached_port
        )

        if _idx is None:
            raise exceptions.DSSMemcachedConnectionError()

        if isinstance(_idx, bytes):
            _idx = str(_idx, 'utf-8')

        # Obtain the latest status uploaded by UGR and typecast to str if needed.
        # Different versions of memcache module return bytes.
        _logger.debug(
            "Using memcached connection stats index: Ugrpluginstats_%s",
            _idx
        )

        _connection_stats = memcache.get(
            'Ugrpluginstats_' + _idx,
            memcached_ip,
            memcached_port
        )

        # Check if we actually got information
        if _connection_stats is None:
            raise exceptions.DSSMemcachedIndexError()

    except exceptions.DSSMemcachedError as ERR:
        _logger.error(
            "%s Server %s did not return data. All storage_shares will be " \
            "assumed 'Online'.",
            ERR.debug,
            memcached_ip + ':' + memcached_port
        )

    else:
        if isinstance(_connection_stats, bytes):
            _connection_stats = str(_connection_stats, 'utf-8')

        # Remove the \x00 character.
        _connection_stats = _connection_stats.rsplit('\x00', 1)[0]
        _logger.debug(
            "Connection stats obtained: %s",
            _connection_stats
        )

        # Split the stats per storage_share, delimited by '&&'
        _connection_stats = _connection_stats.rsplit('&&')

        # For each storage_share, we are going to obtain the individual stats
        # from UGR and use the storage_share's ID to also obtain from memcache
        # the storage stats (if any) and concatenate them together, delimited
        # by '%%'. Once this has been done for each storage_share, concatenate
        # all storage_shares together onto one string delimited by '&&'.
        _storage_shares_conn_stats = {}

        for _element in _connection_stats:
            _storage_share, _stats = _element.split("%%", 1)
            _status = _stats.split("%%")[2]
            _storage_shares_conn_stats[_storage_share] = _status

        for _storage_share in storage_share_objects:
            if _storage_share.id in _storage_shares_conn_stats:
                if _storage_shares_conn_stats[_storage_share.id] == '2':
                    _storage_share.stats['check'] = "EndpointOffline"
                    _logger.info(
                        "[%s]Endpoint reported 'Offline'",
                        _storage_share.id
                    )

                else:
                    _logger.info(
                        "[%s]Endpoint reported 'Online'",
                        _storage_share.id
                    )
            else:
                _logger.info(
                    "[%s]Endpoint was not found in connection stats. " \
                    "Will be assumed 'Online'",
                    _storage_share
                )


def process_storagestats(storage_endpoint, args):
    """Run StorageShare.get_storagestats() for storage shares in StorageEndpoint.

    Runs get_storagestats() method for the first StorageShare in the list of the
    StorageEndpoint as long as it has not been flagged as offline. It then calls
    process_endpoint_list_results() to copy the results if there are multiple
    StorageShares. Then if requested upload the stats to memcached.
    Also handles the exceptions to failures in obtaining the stats.

    Arguments:
    storage_endpoint -- dynafed_storagestats StorageEndpoint.
    args -- args -- argparse object.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    try:
        if storage_endpoint.storage_shares[0].stats['check'] is True:
            _logger.info("[%s]Contacting endpoint.", storage_endpoint.storage_shares[0].id)
            storage_endpoint.storage_shares[0].get_storagestats()

        elif storage_endpoint.storage_shares[0].stats['check'] == "EndpointOffline":
            _logger.error(
                "[%s][%s]Bypassing stats check.",
                storage_endpoint.storage_shares[0].id,
                storage_endpoint.storage_shares[0].stats['check']
            )

            raise exceptions.DSSOfflineEndpointError(
                status_code="400",
                error="EndpointOffline"
            )

        else:
            _logger.error(
                "[%s][%s]Bypassing stats check.",
                storage_endpoint.storage_shares[0].id,
                storage_endpoint.storage_shares[0].stats['check']
            )

    except exceptions.DSSOfflineEndpointError as ERR:
        _logger.error("[%s]%s", storage_endpoint.storage_shares[0].id, ERR.debug)
        storage_endpoint.storage_shares[0].debug.append("[ERROR]" + ERR.debug)
        storage_endpoint.storage_shares[0].status.append("[ERROR]" + ERR.error_code)

    except exceptions.DSSWarning as WARN:
        _logger.warning("[%s]%s", storage_endpoint.storage_shares[0].id, WARN.debug)
        storage_endpoint.storage_shares[0].debug.append("[WARNING]" + WARN.debug)
        storage_endpoint.storage_shares[0].status.append("[WARNING]" + WARN.error_code)

    except exceptions.DSSError as ERR:
        _logger.error("[%s]%s", storage_endpoint.storage_shares[0].id, ERR.debug)
        storage_endpoint.storage_shares[0].debug.append("[ERROR]" + ERR.debug)
        storage_endpoint.storage_shares[0].status.append("[ERROR]" + ERR.error_code)

    finally:
        # Copy results if there are multiple endpoints under one URL.
        process_endpoint_list_results(storage_endpoint.storage_shares)

        for storage_share in storage_endpoint.storage_shares:
            # Mark the status as OK if there are no status messages or format the list
            # as a CSV string.
            if len(storage_share.status) is 0:
                storage_share.status = '[OK][OK][200]'
            else:
                storage_share.status = ','.join(storage_share.status)

            # Try to upload stats to memcached.
            if args.output_memcached:
                try:
                    output.to_memcached(storage_share, args.memcached_ip, args.memcached_port)

                except exceptions.DSSMemcachedConnectionError as ERR:
                    _logger.error("[%s]%s", storage_share.id, ERR.debug)
                    storage_share.debug.append("[ERROR]" + ERR.debug)
                    storage_share.status = storage_share.status + "," + "[ERROR]" + ERR.error_code


def process_endpoint_list_results(storage_share_objects):
    """Copy the storage stats from the first StorageShare to the rest in list.

    Copies the results from the first StorageShare in the list whose storagestats
    have been obtained by process_storagestats() to the rest of a StorageEndpoint
    that has multiple StorageShares. Non "api" quotas will NOT be overwritten.

    Arguments:
    storage_share_objects -- list of dynafed_storagestats StorageShare objects.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    # If there is only one storage_share, there is nothing to do!
    if len(storage_share_objects) >= 1:
        for _storage_share in list(range(1, len(storage_share_objects))):
            _logger.info(
                '[%s] Same storage endpoint as "%s". Copying stats.',
                storage_share_objects[_storage_share].id,
                storage_share_objects[0].id
            )

            storage_share_objects[_storage_share].stats['filecount'] = storage_share_objects[0].stats['filecount']
            storage_share_objects[_storage_share].stats['bytesused'] = storage_share_objects[0].stats['bytesused']

            # Check if the plugin settings requests the quota from API. If so,
            # copy it, else use the default or manually setup quota.
            if storage_share_objects[_storage_share].plugin_settings['storagestats.quota'] == 'api':
                storage_share_objects[_storage_share].stats['quota'] = storage_share_objects[0].stats['quota']
                storage_share_objects[_storage_share].stats['bytesfree'] = storage_share_objects[0].stats['bytesfree']

            else:
                storage_share_objects[_storage_share].stats['quota'] = storage_share_objects[_storage_share].plugin_settings['storagestats.quota']
                storage_share_objects[_storage_share].stats['bytesfree'] = storage_share_objects[_storage_share].stats['quota'] - storage_share_objects[_storage_share].stats['bytesused']

            # Might need to append any issues with the configuration settings
            storage_share_objects[_storage_share].status = storage_share_objects[0].status
            storage_share_objects[_storage_share].debug = storage_share_objects[0].debug


def setup_logger(logfile="/tmp/dynafed_storagestats.log", loglevel="WARNING", verbose=False):
    """Setup the logger format to be used throughout the script.

    Arguments:
    logfile -- string defining path to write logs to.
    loglevel -- string defining level to log: "DEBUG, INFO, WARNING, ERROR"
    verbose -- boolean. 'True' prints log messages to stderr.

    Returns:
    logging.Logger object.

    """
    # To capture warnings emitted by modules.
    logging.captureWarnings(True)

    # Create file logger.
    _logger = logging.getLogger("dynafed_storagestats")

    # Set log level to use.
    _num_loglevel = getattr(logging, loglevel.upper())
    _logger.setLevel(_num_loglevel)

    # Set logger format
    _log_format_file = logging.Formatter('%(asctime)s - [%(levelname)s]%(message)s')

    # Set file where to log and the mode to use and set the format to use.
    _log_handler_file = logging.handlers.TimedRotatingFileHandler(
        logfile,
        when="midnight",
        backupCount=15,
    )

    _log_handler_file.setFormatter(_log_format_file)

    # Add the file handler created above.
    _logger.addHandler(_log_handler_file)

    # Create STDERR hanler if verbose is requested and add it to logger.
    if verbose:
        log_format_stderr = logging.Formatter('%(asctime)s - [%(levelname)s]%(message)s')
        log_handler_stderr = logging.StreamHandler()
        log_handler_stderr.setLevel(_num_loglevel)
        log_handler_stderr.setFormatter(log_format_stderr)
        # Add handler
        _logger.addHandler(log_handler_stderr)

    return _logger
