"""Helper functions used to contact Azure based API's."""

import logging

from azure.storage.blob.baseblobservice import BaseBlobService
import azure.common

from dynafed_storagestats import exceptions

###############
## Functions ##
###############

def list_blobs(storage_share):
    """Contact Azure endpoint using "list_blobs" method.

    Contacts an Azure blob and uses the "list_blobs" API to recursively obtain
    all the objects in a container and sum their size to obtain total space
    usage.

    Attributes:
    storage_share -- dynafed_storagestats StorageShare object.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    _total_bytes = 0
    _total_files = 0

    _base_blob_service = BaseBlobService(
        account_name=storage_share.uri['account'],
        account_key=storage_share.plugin_settings['azure.key']
    )

    _container_name = storage_share.uri['container']
    _next_marker = None
    _timeout = int(storage_share.plugin_settings['conn_timeout'])

    _logger.debug(
        "[%s]Requesting storage stats with: URN: %s API Method: %s Account: %s Container: %s", \
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
            )

        except azure.common.AzureMissingResourceHttpError as ERR:
            raise exceptions.DSSErrorAzureContainerNotFound(
                error='ContainerNotFound',
                status_code="404",
                debug=str(ERR),
                container=_container_name,
            )

        except azure.common.AzureHttpError as ERR:
            raise exceptions.DSSConnectionErrorAzureAPI(
                error='ConnectionError',
                status_code="400",
                debug=str(ERR),
                api=storage_share.plugin_settings['storagestats.api'],
            )

        except azure.common.AzureException as ERR:
            raise exceptions.DSSConnectionError(
                error='ConnectionError',
                status_code="400",
                debug=str(ERR),
            )

        else:
            try:
                _blobs.items
            except AttributeError:
                storage_share.stats['bytesused'] = 0
                break
            else:
                _next_marker = _blobs.next_marker

                try:
                    for _blob in _blobs:
                        _total_bytes += _blob.properties.content_length
                        _total_files += 1

                except azure.common.AzureHttpError:
                    pass

                if not _next_marker:
                    break

    storage_share.stats['bytesused'] = _total_bytes
    storage_share.stats['quota'] = storage_share.plugin_settings['storagestats.quota']
    storage_share.stats['bytesfree'] = storage_share.stats['quota'] - _total_bytes
    # Not required, but is useful for reporting/accounting:
    storage_share.stats['filecount'] = _total_files


# def ():
#     """
#
#     """
#     ############# Creating loggers ################
#     _logger = logging.getLogger(__name__)
#     ###############################################
