"""Functions to deal with the formatting and handling data to output."""

import os
import logging

import dynafed_storagestats.exceptions
from dynafed_storagestats import memcache
from dynafed_storagestats import json
from dynafed_storagestats import xml

#############
# Functions #
#############


def to_json(storage_endpoints, filename, path):
    """Call helper functions to create a JSON file with storage stats.

    Arguments:
    storage_endpoints -- list of dynafed_storagestats StorageEndpoint objects.
    filename -- Name of the JSON file.
    path -- Path to write the JSON file to.

    """

    _hostname = os.uname()[1]

    # Add if statement in the future if there are more JSON formats to call.

    _json_file = json.format_wlcg(
        storage_endpoints,
        _hostname,
    )

    # Create output path
    _filepath = path + '/' + filename

    with open(_filepath, 'w') as output:
        output.write(_json_file)
        output.close()


def to_memcached(storage_share, memcached_ip='127.0.0.1', memcached_port='11211', ttl_multiplier=10):
    """Upload the StorageShare storage stats to a memcached instance.

    Arguments:
    storage_share  -- dynafed_storagestats StorageShare object.
    memcached_ip   -- memcached instance IP.
    memcahced_port -- memcached instance Port.
    ttl_multiplier -- multiplier to calculate memcache data ttl.

    Memcache output is a string of the following variables concatenated by '%%':
    storage_share.id
    storage_share.storageprotocol
    storage_share.stats['bytesused']
    storage_share.stats['bytesfree']
    storage_share.stats['quota']
    storage_share.stats['starttime']

    """
    # Creating logger
    _logger = logging.getLogger(__name__)

    _memcached_index = "Ugrstoragestats_" + storage_share.id

    # Calculate how long to store data in memcached as a multiple of the check
    # frequency. However the minimum us set to 1 hour.
    _memcached_ttl = int(storage_share.plugin_settings['storagestats.frequency']) * ttl_multiplier
    if _memcached_ttl < 3600:
        _memcached_ttl = 3600

    # Join stats to create the string to upload.
    _storagestats = '%%'.join([
        storage_share.id,
        storage_share.storageprotocol,
        str(storage_share.stats['starttime']),
        str(storage_share.stats['quota']),
        str(storage_share.stats['bytesused']),
        str(storage_share.stats['bytesfree']),
        storage_share.status,
    ])

    _logger.info(
        "[%s]Uploading stats to memcached server: %s",
        storage_share.id,
        memcached_ip + ':' + memcached_port
    )
    _logger.debug(
        "[%s]Using memcached index: %s",
        storage_share.id,
        _memcached_index
    )
    _logger.debug(
        "[%s]String uploading to memcached: %s",
        storage_share.id,
        _storagestats
    )
    _logger.debug(
        "[%s]Time to live for memcached data: %s",
        storage_share.id,
        _memcached_ttl
    )

    memcache.set(
        _memcached_index,
        _storagestats,
        memcached_ip,
        memcached_port,
        _memcached_ttl
    )


def to_plaintext(storage_endpoints, filename, path):
    """Create a single TXT file for all storage_shares passed to this function.

    Arguments:
    storage_endpoints -- list of dynafed_storagestats StorageEndpoint objects.
    filename -- Name of the TXT file.
    path -- Path to write the TXT file to.

    """

    # Initialize total tally
    _dynafed_usedsize = 0
    _dynafed_totalsize = 0

    # Create output path
    _filepath = path + '/' + filename

    # Open file handle to write to
    with open(_filepath, 'w') as output:
        output.write("ID URL MountPoint Protocol Timestamp Quota BytesUsed BytesFree FileCount\n")

        for _storage_endpoint in storage_endpoints:
            for _storage_share in _storage_endpoint.storage_shares:
                _dynafed_usedsize += _storage_share.stats['bytesused']
                _dynafed_totalsize += _storage_share.stats['quota']
                output.write(
                    "%s %s %s %s %d %d %d %d %d\n" % (
                        _storage_share.id,
                        _storage_share.uri['url'],
                        _storage_share.plugin_settings['xlatepfx'].split()[0],
                        _storage_share.storageprotocol,
                        _storage_share.stats['starttime'],
                        _storage_share.stats['quota'],
                        _storage_share.stats['bytesused'],
                        _storage_share.stats['bytesfree'],
                        _storage_share.stats['filecount'],
                    )
                )

    output.close()


def to_xml(storage_endpoints, filename, path):
    """Call helper functions to create a XML file with storage stats.

    Arguments:
    storage_endpoints -- list of dynafed_storagestats StorageEndpoint objects.
    filename -- Name of the XML file.
    path -- Path to write the XML file to.

    """

    # Create output path
    _filepath = path + '/' + filename

    _xml_file = xml.format_StAR(storage_endpoints)

    with open(_filepath, 'w') as output:
        output.write(_xml_file)
        output.close()


def to_stdout(storage_endpoints, args):
    """Print to stdout the storage stats information including memcache indices.

    Does this for each StorageShare belonging to each StorageEndpoint in the
    storage_endpoints list, including the last warning/error. Connects to a
    memcached instance and tries to obtain the storage stats from the index
    belonging to the storage_share's.

    Arguments:
    storage_endpoints -- list of dynafed_storagestats StorageEndpoint objects.
    args -- argparse object.

    Argument flags
    args.debug -- full warning/error information from the exceptions.
    args.memcached_ip -- memcached instance IP.
    args.memcached_port -- memcached instance Port.

    """
    for _storage_endpoint in storage_endpoints:
        for _storage_share in _storage_endpoint.storage_shares:
            _memcached_index = "Ugrstoragestats_" + _storage_share.id

            try:
                _memcached_contents = memcache.get(
                    _memcached_index,
                    args.memcached_ip,
                    args.memcached_port
                )

            except dynafed_storagestats.exceptions.MemcachedIndexError as ERR:
                _memcached_contents = 'No content found or error connecting to memcached service.'
                _storage_share.debug.append("[ERROR]" + ERR.debug)

            print('\n#####', _storage_share.id, '#####'
                  '\n{0:12}{1}'.format('URL:', _storage_share.uri['url']),
                  '\n{0:12}{1}'.format('Protocol:', _storage_share.storageprotocol),
                  '\n{0:12}{1}'.format('Time:', _storage_share.stats['starttime']),
                  '\n{0:12}{1}'.format('Quota:', _storage_share.stats['quota']),
                  '\n{0:12}{1}'.format('Bytes Used:', _storage_share.stats['bytesused']),
                  '\n{0:12}{1}'.format('Bytes Free:', _storage_share.stats['bytesfree']),
                  '\n{0:12}{1}'.format('FileCount:', _storage_share.stats['filecount']),
                  '\n{0:12}{1}'.format('Status:', _storage_share.status),
                  )

            print('\nMemcached:',
                  '\n{0:12}{1}'.format('Index:', _memcached_index),
                  '\n{0:12}{1}'.format('Contents:', _memcached_contents),
                  )

            if args.debug:
                print('\nDebug:')
                for _error in _storage_share.debug:
                    print('{0:12}{1}'.format(' ', _error))
