"""Helper functions used by the other modules."""

import logging, logging.handlers
import os

import dynafed_storagestats.exceptions
from dynafed_storagestats import memcache
from dynafed_storagestats import output
from dynafed_storagestats import time

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


def check_connectionstats(storage_share_objects, stats):
    """Check each storage share's offline/online status and flag accordingly.

    Check if each StorageShare in the list has a connection status in the
    "stats" dict passed. When no status is found, default flag "True" in
    storage_share.stats['check'] is respected and therefore assumed online.
    Otherwise, it is updated to represent the status obtained.

    Arguments:
    storage_share_objects -- list of dynafed_storagestats StorageShare objects.
    stats -- dictionary of storage_shares and their status obtained from
             get_cached_connection_stats with return_as='expanded_dictionary'.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    for _storage_share in storage_share_objects:
        try:
            if stats[_storage_share.id] == '2':
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
        # If an endpoint is not found, we catch it here.
        except KeyError:
            _logger.warning(
                "[%s]Endpoint stats not found in cache. Assuming 'Online'",
                _storage_share.id
            )


def check_frequency(storage_share_objects, stats):
    """Compare last check timestamp to defined frequency and flag accordingly.

    Arguments:
    storage_share_objects -- list of dynafed_storagestats StorageShare objects.
    stats -- dictionary of storage_shares and their status obtained from
             get_cached_connection_stats with return_as='expanded_dictionary'.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    for _storage_share in storage_share_objects:
        _logger.info(
            "[%s]Checking if checkpoint has been reached.",
            _storage_share.id
        )

        try:
            if stats[_storage_share.id]:
                if time.is_later(
                    int(stats[_storage_share.id]['timestamp']),
                    int(_storage_share.plugin_settings['storagestats.frequency'])
                ):
                    _logger.info(
                        "[%s]Checkpoint has been reached. Endpoint will be checked",
                        _storage_share.id
                    )
                else:
                    _storage_share.stats['check'] = "PeriodNotReached"
                    _logger.info(
                        "[%s]Checkpoint has not been reached. Endpoint will be skipped.",
                        _storage_share.id
                    )

        except KeyError:
            _logger.warning(
                "[%s]No frequency found. Endpoint will be checked.",
                _storage_share.id
            )


def get_currentstats(storage_share_objects, memcached_ip='127.0.0.1', memcached_port='11211'):
    """Obtain StorageShares' status contained in memcached and return as dict.

    Check if each StorageShare in the list has information about its status in
    memcached, which would have been uploaded by Dynafed's periodic test. Turn
    the string of information into a dictionary of StorageShare ID's and their
    stats, which is then returned.

    Arguments:
    storage_share_objects -- list of dynafed_storagestats StorageShare objects.
    memcached_ip   -- memcached instance IP.
    memcahced_port -- memcached instance Port.

    Returns:
    dictionary
    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    # We try to obtain connection stats from memcache.
    try:
        _connection_stats = get_cached_connection_stats(
                              return_as='expanded_dictionary',
                              memcached_ip=memcached_ip,
                              memcached_port=memcached_port
                            )

    except dynafed_storagestats.exceptions.MemcachedError as ERR:
        _logger.error(
            "Memcache %s Server %s did not return data. All storage_shares will be " \
            "assumed 'Online'.",
            ERR.debug,
            memcached_ip + ':' + memcached_port
        )
        _connection_stats = None

    finally:
        _logger.debug(
            "Dictionary of connection stats found in memcache: %s.",
            _connection_stats
        )

    # Now we try to obtain storage stats from memcache.
    try:
        _storage_stats = get_cached_storage_stats(
                           storage_share_objects,
                           return_as='expanded_dictionary',
                           memcached_ip=memcached_ip,
                           memcached_port=memcached_port

        )

    except dynafed_storagestats.exceptions.MemcachedError as ERR:
        _logger.error(
            "Memcache %s Server %s did not return storage status data.",
            ERR.debug,
            memcached_ip + ':' + memcached_port
        )
        _storage_stats = None

    finally:
        _logger.debug(
            "Dictionary of storage stats found in memcache: %s.",
            _storage_stats
        )

    # else:
    #     _storage_shares_current_stats += _storage_stats


    return _connection_stats, _storage_stats


def get_cached_connection_stats(return_as='string', memcached_ip='127.0.0.1', memcached_port='11211'):
    """Obtain connection status string in memcached and return as requested object.

    Check if memcached has cached connection status for the StorageShares, which
    would have been uploaded by UGR/Dynafed's periodic test. Turn the string of
    information into a dictionary of StorageShare ID's and their stats,
    which is then returned. The returned object can be a sting containing all
    StorageShares, or an array of strings separated by StorageShares,  or a
    dictionary whose keys are the StorageShare ID's and the value is a string
    with all the stats, or a dictionary of dictionaries where each stat has
    a key/value as well.

    This is specified with the 'return_as' argument as:
    'string' -- single sting containing all StorageShares, each delimieted '&&'
    'array'  -- each StorageShare's string is separated and the array returned.
    'dictionary' -- each key is the StorageShare ID's and the value is a string
    with all the stats.
    'expanded_dictionary' -- each key is the StorageShare ID's and each value is a key
    under it with it's own value.

    Arguments:
    memcached_ip   -- memcached instance IP.
    memcahced_port -- memcached instance Port.

    Returns:
    array OR dictionary OR string
    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    _logger.info(
        "Checking memcached server %s:%s for StorageShares connection stats.",
        memcached_ip,
        memcached_port
    )

    # Obtain the latest index number used by UGR and typecast to str if
    # needed. Different versions of memcache module return bytes.
    _idx = memcache.get(
        'Ugrpluginstats_idx',
        memcached_ip,
        memcached_port
    )

    if _idx is None:
        raise dynafed_storagestats.exceptions.MemcachedConnectionError()

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
        raise dynafed_storagestats.exceptions.MemcachedIndexError()

    # Typecast to str if needed. Different versions of memcache module return
    # bytes.
    if isinstance(_connection_stats, bytes):
        _connection_stats = str(_connection_stats, 'utf-8')

    # Remove the \x00 character.
    if '\x00' in _connection_stats:
        _connection_stats = _connection_stats.rsplit('\x00', 1)[0]

    _logger.debug(
        "Connection stats obtained: %s",
        _connection_stats
    )

    # Split the stats per storage_share, delimited by '&&'
    _array_of_stats = []
    _connection_stats = _connection_stats.rsplit('&&')

    for _element in _connection_stats:
        #When the connection status is OK the last element is empty. So we add an 'OK'
        if _element.split("%%")[-1] == '':
            _element = _element + 'OK'

        _discard_duplicate, _element = _element.split("%%", 1)
        _array_of_stats.append(_element)

    # Format and return according to request 'return_as'
    if return_as == 'string':
        return '&&'.join(_array_of_stats)

    elif return_as == 'array':
        return _array_of_stats

    elif return_as == 'dictionary':
        _dictonary_of_stats = {}

        for _element in _array_of_stats:
            _storage_share, _stats = _element.split("%%", 1)
            _dictonary_of_stats[_storage_share] = _stats

        return _dictonary_of_stats

    elif return_as == 'expanded_dictionary':
        _dictonary_of_stats = {}

        for _element in _array_of_stats:
            _storage_share, _timestamp, _status, _latency, _status_code, _error = _element.split("%%")

            _dictonary_of_stats[_storage_share] = {
                'timestamp': _timestamp,
                'status': _status,
                'latency': _latency,
                'status_code': _status_code,
                'error': _error
            }

        return _dictonary_of_stats


def get_cached_storage_stats(storage_share_objects, return_as='string', memcached_ip='127.0.0.1', memcached_port='11211'):
    """Obtain storage status string in memcached and return as requested object.

    Check if memcached has cached storage status for the StorageShares, which
    would have been uploaded by this script previously. Turn the string of
    information into a dictionary of StorageShare ID's and their stats,
    which is then returned. The returned object can be a string containing all
    StorageShares, or an array of strings separated by StorageShares,  or a
    dictionary whose keys are the StorageShare ID's and the value is a string
    with all the stats, or a dictionary of dictionaries where each stat has
    a key/value as well.

    This is specified with the 'return_as' argument as:
    'string' -- single sting containing all StorageShares, each delimieted '&&'
    'array'  -- each StorageShare's string is separated and the array returned.
    'dictionary' -- each key is the StorageShare ID's and the value is a string
    with all the stats.
    'expanded_dictionary' -- each key is the StorageShare ID's and each value is a key
    under it with it's own value.

    Arguments:
    storage_share_objects -- list of dynafed_storagestats StorageShare objects.
    memcached_ip   -- memcached instance IP.
    memcahced_port -- memcached instance Port.

    Returns:
    array OR dictionary OR string
    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################
    _logger.info(
        "Checking memcached server %s:%s for StorageShares storage stats.",
        memcached_ip,
        memcached_port
    )

    _array_of_stats = []

    for _storage_share in storage_share_objects:
        # Index in memcache to look for:
        _idx = 'Ugrstoragestats_' + _storage_share.id
        # Obtain the latest status uploaded by UGR and typecast to str if needed.
        # Different versions of memcache module return bytes.
        _logger.debug(
            "Using memcached storage stats index: %s",
            _idx
        )

        _storage_stats = memcache.get(
            _idx,
            memcached_ip,
            memcached_port
        )

        # Check if we actually got information
        if _storage_stats is None:
            raise dynafed_storagestats.exceptions.MemcachedIndexError()

        # Typecast to str if needed. Different versions of memcache module return
        # bytes.
        if isinstance(_storage_stats, bytes):
            _storage_stats = str(_storage_stats, 'utf-8')

        _array_of_stats.append(_storage_stats)

    _logger.debug(
        "Storage stats obtained: %s",
        _array_of_stats
    )

    # Format and return according to request 'return_as'
    if return_as == 'string':
        return '&&'.join(_array_of_stats)

    elif return_as == 'array':
        return _array_of_stats

    elif return_as == 'dictionary':
        _dictonary_of_stats = {}

        for _element in _array_of_stats:
            _storage_share, _stats = _element.split("%%", 1)
            _dictonary_of_stats[_storage_share] = _stats

        return _dictonary_of_stats

    elif return_as == 'expanded_dictionary':
        _dictonary_of_stats = {}

        for _element in _array_of_stats:
            _storage_share, _protocol, _timestamp, _quota, _bytesused, _bytesfree, _status = _element.split("%%")

            _dictonary_of_stats[_storage_share] = {
                'timestamp': _timestamp,
                'bytesused': _bytesused,
                'bytesfree': _bytesfree,
            }

        return _dictonary_of_stats


def process_storagereports(storage_endpoint, args):
    """Run StorageShare.get_filelist() for storage shares in StorageEndpoint.

    Runs get_filelist() method for the first StorageShare in the list of the
    StorageEndpoint as long as it has not been flagged as offline.

    #It then calls
    #process_endpoint_list_results() to copy the results if there are multiple
    #StorageShares. Then if requested upload the stats to memcached.
    Also handles the exceptions to failures in creating the report file,
    deleting it in case of error.

    Arguments:
    storage_endpoint -- dynafed_storagestats StorageEndpoint.
    args -- args -- argparse object.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    _filepath = args.output_path + '/' + storage_endpoint.storage_shares[0].id + '.txt'

    try:
        _logger.info(
            "[%s]Contacting endpoint.",
            storage_endpoint.storage_shares[0].id
        )

        _logger.info(
            "[%s]Writing file-list report to '%s'",
            storage_endpoint.storage_shares[0].id,
            _filepath
        )

        _logger.debug(
            "[%s]Writing file-list report using options '_filepath': '%s', 'prefix': '%s'",
            storage_endpoint.storage_shares[0].id,
            _filepath,
            args.prefix
        )

        with open(_filepath, 'w') as _report_file:
            storage_endpoint.storage_shares[0].get_filelist(
                delta=args.delta,
                prefix=args.prefix,
                report_file=_report_file,
            )

    except FileNotFoundError as ERR:
        _logger.critical(
            "[%s]Failed to create file. Path does not exist: %s",
             storage_endpoint.storage_shares[0].id,
             args.output_path
        )

    except PermissionError as ERR:
        _logger.critical(
            "[%s]Failed to create file. Permission denied to write at: %s",
             storage_endpoint.storage_shares[0].id,
             args.output_path
        )

    except AttributeError as ERR:
        _logger.error(
            "[%s]Report creation is not supported for plugin type '%s'. " \
            "Skipping storage endpoint.",
            storage_endpoint.storage_shares[0].id,
            storage_endpoint.storage_shares[0].plugin
        )
        _logger.error(
            "[%s]Deleting report file '%s'",
            storage_endpoint.storage_shares[0].id,
            _filepath
        )
        os.remove(_filepath)

    except dynafed_storagestats.exceptions.OfflineEndpointError as ERR:
        _logger.error(
            "[%s]%s",
            storage_endpoint.storage_shares[0].id,
            ERR.debug
        )
        storage_endpoint.storage_shares[0].debug.append("[ERROR]" + ERR.debug)
        storage_endpoint.storage_shares[0].status.append("[ERROR]" + ERR.error_code)
        _logger.error(
            "[%s]Deleting report file '%s'",
            storage_endpoint.storage_shares[0].id,
            _filepath
        )
        os.remove(_filepath)

    except dynafed_storagestats.exceptions.Warning as WARN:
        _logger.warning(
            "[%s]%s",
            storage_endpoint.storage_shares[0].id,
            WARN.debug
        )
        storage_endpoint.storage_shares[0].debug.append("[WARNING]" + WARN.debug)
        storage_endpoint.storage_shares[0].status.append("[WARNING]" + WARN.error_code)

    except dynafed_storagestats.exceptions.Error as ERR:
        _logger.error(
            "[%s]%s",
            storage_endpoint.storage_shares[0].id,
            ERR.debug
        )
        storage_endpoint.storage_shares[0].debug.append("[ERROR]" + ERR.debug)
        storage_endpoint.storage_shares[0].status.append("[ERROR]" + ERR.error_code)
        _logger.error(
            "[%s]Deleting report file '%s'",
            storage_endpoint.storage_shares[0].id,
            _filepath
        )
        os.remove(_filepath)


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

            raise dynafed_storagestats.exceptions.OfflineEndpointError(
                status_code="400",
                error="EndpointOffline"
            )

        else:
            _logger.error(
                "[%s][%s]Bypassing stats check.",
                storage_endpoint.storage_shares[0].id,
                storage_endpoint.storage_shares[0].stats['check']
            )

    except dynafed_storagestats.exceptions.OfflineEndpointError as ERR:
        _logger.error("[%s]%s", storage_endpoint.storage_shares[0].id, ERR.debug)
        storage_endpoint.storage_shares[0].debug.append("[ERROR]" + ERR.debug)
        storage_endpoint.storage_shares[0].status.append("[ERROR]" + ERR.error_code)

    except dynafed_storagestats.exceptions.Warning as WARN:
        _logger.warning("[%s]%s", storage_endpoint.storage_shares[0].id, WARN.debug)
        storage_endpoint.storage_shares[0].debug.append("[WARNING]" + WARN.debug)
        storage_endpoint.storage_shares[0].status.append("[WARNING]" + WARN.error_code)

    except dynafed_storagestats.exceptions.Error as ERR:
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

                except dynafed_storagestats.exceptions.MemcachedConnectionError as ERR:
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

            storage_share_objects[_storage_share].stats['filecount'] = (
                storage_share_objects[0].stats['filecount']
            )
            storage_share_objects[_storage_share].stats['bytesused'] = (
                storage_share_objects[0].stats['bytesused']
            )

            # Check if the plugin settings requests the quota from API. If so,
            # copy it, else use the default or manually setup quota.
            if storage_share_objects[_storage_share].plugin_settings['storagestats.quota'] == 'api':
                storage_share_objects[_storage_share].stats['quota'] = (
                    storage_share_objects[0].stats['quota']
                )
                storage_share_objects[_storage_share].stats['bytesfree'] = (
                    storage_share_objects[0].stats['bytesfree']
                )

            else:
                storage_share_objects[_storage_share].stats['quota'] = (
                    storage_share_objects[_storage_share].plugin_settings['storagestats.quota']
                )
                storage_share_objects[_storage_share].stats['bytesfree'] = (
                    storage_share_objects[_storage_share].stats['quota']
                    - storage_share_objects[_storage_share].stats['bytesused']
                )

            # Might need to append any issues with the configuration settings
            storage_share_objects[_storage_share].status = (
                storage_share_objects[0].status
            )
            storage_share_objects[_storage_share].debug = (
                storage_share_objects[0].debug
            )


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


def update_storage_share_storagestats(storage_share_objects, stats):
    """Fill storage_share's stats with the obtained storage stats from memcache.

    Update storage_share's stats with information obtained from memcache using
    get_cached_storage_stats function.

    Arguments:
    storage_endpoint -- dynafed_storagestats StorageEndpoint.
    stats -- dictionary of storage_shares and their status obtained from
             get_cached_storage_stats with return_as='expanded_dictionary'.
    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    for _storage_share in storage_share_objects:
        try:
            if stats[_storage_share.id]:
                _logger.info(
                    "[%s]Updating storage stats information.",
                    _storage_share.id
                )
                _logger.debug(
                    "[%s]Stats being updated: %s",
                    _storage_share.id,
                    stats[_storage_share.id]
                )
                _storage_share.stats['endtime'] = stats[_storage_share.id]['timestamp']
                _storage_share.stats['bytesused'] = stats[_storage_share.id]['bytesused']
                _storage_share.stats['bytesfree'] = stats[_storage_share.id]['bytesfree']

        except KeyError:
            _logger.info(
                "[%s]Storage stats not found in cache. Skipping.",
                _storage_share.id
            )
