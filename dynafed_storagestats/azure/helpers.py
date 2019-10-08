"""Helper functions used to contact Azure based API's."""

import datetime
import logging

from azure.storage.blob.baseblobservice import BaseBlobService
import azure.common

import dynafed_storagestats.exceptions
import dynafed_storagestats.helpers
import dynafed_storagestats.time


##############
# Functions #
##############

def list_blobs(storage_share, delta=1, prefix='',
               report_file='/tmp/filelist_report.txt',
               request='storagestats'
               ):
    """Contact Azure endpoint using "list_blobs" method.

    Contacts an Azure blob and uses the "list_blobs" API to recursively obtain
    all the objects in a container and sum their size to obtain total space
    usage.

    Attributes:
    storage_share -- dynafed_storagestats StorageShare object.

    """
    # Creating logger
    _logger = logging.getLogger(__name__)

    _total_bytes = 0
    _total_files = 0

    _base_blob_service = BaseBlobService(
        account_name=storage_share.uri['account'],
        account_key=storage_share.plugin_settings['azure.key'],
        # Set to true if using Azurite storage emulator for testing.
        is_emulated=False
    )

    _container_name = storage_share.uri['container']
    _next_marker = None
    _timeout = int(storage_share.plugin_settings['conn_timeout'])

    _logger.debug(
        "[%s]Requesting storage stats with: URN: %s API Method: %s Account: %s Container: %s",
        storage_share.id, storage_share.uri['url'],
        storage_share.plugin_settings['storagestats.api'].lower(),
        storage_share.uri['account'],
        storage_share.uri['container']
    )

    while True:
        try:
            _blobs = _base_blob_service.list_blobs(
                _container_name,
                marker=_next_marker,
                timeout=_timeout,
                prefix=prefix,
            )

        except azure.common.AzureMissingResourceHttpError as ERR:
            raise dynafed_storagestats.exceptions.ErrorAzureContainerNotFound(
                error='ContainerNotFound',
                status_code="404",
                debug=str(ERR),
                container=_container_name,
            )

        except azure.common.AzureHttpError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionErrorAzureAPI(
                error='ConnectionError',
                status_code="400",
                debug=str(ERR),
                api=storage_share.plugin_settings['storagestats.api'],
            )

        except azure.common.AzureException as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error='ConnectionError',
                status_code="400",
                debug=str(ERR),
            )

        else:
            # Check what type of request is asked being used.
            if request == 'storagestats':
                try:  # Make sure we got a list of objects.
                    _blobs.items
                except AttributeError:
                    storage_share.stats['bytesused'] = 0
                    break
                else:
                    try:
                        for _blob in _blobs:
                            _total_bytes += int(_blob.properties.content_length)
                            _total_files += 1
                    # Investigate
                    except azure.common.AzureHttpError:
                        pass

            elif request == 'filelist':
                try:  # Make sure we got a list of objects.
                    _blobs.items
                except AttributeError:
                    break
                else:
                    for _blob in _blobs:
                        # Output files older than the specified delta.
                        if dynafed_storagestats.time.mask_timestamp_by_delta(_blob.properties.last_modified, delta):
                            report_file.write("%s\n" % _blob.name)
                            _total_files += 1

            # Exit if no "NextMarker" as list is now over.
            if _next_marker:
                _next_marker = _blobs.next_marker
            else:
                break

    # Save time when data was obtained.
    storage_share.stats['endtime'] = int(datetime.datetime.now().timestamp())

    # Process the result for the storage stats.
    if request == 'storagestats':
        storage_share.stats['bytesused'] = int(_total_bytes)

        # Obtain or set default quota and calculate freespace.
        if storage_share.plugin_settings['storagestats.quota'] == 'api':
            storage_share.stats['quota'] = dynafed_storagestats.helpers.convert_size_to_bytes("1TB")
            storage_share.stats['filecount'] = _total_files
            storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']
            raise dynafed_storagestats.exceptions.QuotaWarning(
                error="NoQuotaGiven",
                status_code="098",
                default_quota=storage_share.stats['quota'],
            )

        else:
            storage_share.stats['quota'] = int(storage_share.plugin_settings['storagestats.quota'])
            storage_share.stats['filecount'] = _total_files
            storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']


# def ():
#     """
#
#     """
#     # Creating logger
#     _logger = logging.getLogger(__name__)
#
