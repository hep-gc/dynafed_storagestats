"""
Module with helper functions used to contact S3 based API's
"""

import logging
import time

import boto3
import botocore.vendored.requests.exceptions as botoRequestsExceptions
import botocore.exceptions as botoExceptions
from botocore.client import Config

import requests
from requests_aws4auth import AWS4Auth

import dynafed_storagestats.helpers
from dynafed_storagestats import exceptions

###############
## Functions ##
###############

def ceph_admin(storage_share):
    """
    # Getting the storage Stats CephS3's Admin API
    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    if storage_share.plugin_settings['s3.alternate'].lower() == 'true'\
    or storage_share.plugin_settings['s3.alternate'].lower() == 'yes':

        _api_url = '{scheme}://{netloc}/admin/bucket?format=json'.format(
            scheme=storage_share.uri['scheme'],
            netloc=storage_share.uri['netloc']
        )

    else:
        _api_url = '{scheme}://{domain}/admin/{bucket}?format=json'.format(
            scheme=storage_share.uri['scheme'],
            domain=storage_share.uri['domain'],
            bucket=storage_share.uri['bucket']
        )

    _payload = {
        'bucket': storage_share.uri['bucket'],
        'stats': 'True'
    }

    _auth = AWS4Auth(
        storage_share.plugin_settings['s3.pub_key'],
        storage_share.plugin_settings['s3.priv_key'],
        storage_share.plugin_settings['s3.region'],
        's3',
    )

    _logger.debug(
        "[%s]Requesting storage stats with: URN: %s API Method: %s Payload: %s",
        storage_share.id,
        _api_url,
        storage_share.plugin_settings['storagestats.api'].lower(),
        _payload
    )

    # We need to initialize "response" to check if it was successful in the
    # finally statement.
    _response = False

    try:
        _response = requests.request(
            method="GET",
            url=_api_url,
            params=_payload,
            auth=_auth,
            verify=storage_share.plugin_settings['ssl_check'],
            timeout=int(storage_share.plugin_settings['conn_timeout'])
        )

        # Save time when data was obtained.
        storage_share.stats['endtime'] = int(time.time())

        #Log contents of response
        _logger.debug(
            "[%s]Endpoint reply: %s",
            storage_share.id,
            _response.text
        )

    except requests.exceptions.InvalidSchema as ERR:
        raise exceptions.DSSConnectionErrorInvalidSchema(
            error='InvalidSchema',
            schema=storage_share.uri['scheme'],
            debug=str(ERR),
        )

    except requests.exceptions.SSLError as ERR:
        # If ca_path is custom, try the default in case
        # a global setting is incorrectly giving the wrong
        # ca's to check against.
        try:
            _response = requests.request(
                method="GET",
                url=_api_url,
                params=_payload,
                auth=_auth,
                verify=True,
                timeout=int(storage_share.plugin_settings['conn_timeout'])
            )

            # Save time when data was obtained.
            storage_share.stats['endtime'] = int(time.time())

            #Log contents of response
            _logger.debug(
                "[%s]Endpoint reply: %s",
                storage_share.id,
                _response.text
            )

        except requests.exceptions.SSLError as ERR:
            raise exceptions.DSSConnectionError(
                error=ERR.__class__.__name__,
                status_code="092",
                debug=str(ERR),
            )

    except requests.ConnectionError as ERR:
        raise exceptions.DSSConnectionError(
            error=ERR.__class__.__name__,
            debug=str(ERR),
        )

    finally:
        if _response:
            # If ceph-admin is accidentally requested for AWS, no JSON content
            # is passed, so we check for that.
            # Review this!
            try:
                _stats = _response.json()

            except ValueError:
                raise exceptions.DSSConnectionErrorS3API(
                    error="NoContent",
                    status_code=_response.status_code,
                    api=storage_share.plugin_settings['storagestats.api'],
                    debug=_response.text,
                )

            # Make sure we get a Bucket Usage information.
            # Fails on empty (in minio) or newly created buckets.
            try:
                _stats['usage']

            except KeyError as ERR:
                raise exceptions.DSSErrorS3MissingBucketUsage(
                    status_code=_response.status_code,
                    error=_stats['Code'],
                    debug=str(_stats)
                )

            else:
                # Even if "_stats['usage']" exists, it might be an empty
                # dict with a new bucket.
                # We deal with that by setting _stats 0.
                if _stats['usage']:
                    storage_share.stats['bytesused'] = _stats['usage']['rgw.main']['size_utilized']
                    storage_share.stats['filecount'] = _stats['usage']['rgw.main']['num_objects']

                else:
                    storage_share.stats['bytesused'] = 0
                    storage_share.stats['filecount'] = 0

                if storage_share.plugin_settings['storagestats.quota'] != 'api':
                    storage_share.stats['quota'] = storage_share.plugin_settings['storagestats.quota']
                    storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']

                else:
                    if _stats['bucket_quota']['enabled'] is True:
                        storage_share.stats['quota'] = _stats['bucket_quota']['max_size']
                        storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']

                    elif _stats['bucket_quota']['enabled'] is False:
                        storage_share.stats['quota'] = dynafed_storagestats.helpers.convert_size_to_bytes("1TB")
                        storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']
                        raise exceptions.DSSCephS3QuotaDisabledWarning(
                            default_quota=storage_share.stats['quota'],
                        )

                    else:
                        storage_share.stats['quota'] = dynafed_storagestats.helpers.convert_size_to_bytes("1TB")
                        storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']
                        raise exceptions.DSSQuotaWarning(
                            error="NoQuotaGiven",
                            status_code="098",
                            default_quota=storage_share.stats['quota'],
                        )


def list_objects(storage_share):
    """
    Contacts an S3 endpoints and uses the "list_objects" API to recursively
    obtain all the objects in a container and sum their size to obtain total
    space usage.
    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    if storage_share.plugin_settings['s3.alternate'].lower() == 'true'\
    or storage_share.plugin_settings['s3.alternate'].lower() == 'yes':

        _api_url = '{scheme}://{netloc}'.format(
            scheme=storage_share.uri['scheme'],
            netloc=storage_share.uri['netloc']
        )

    else:
        _api_url = '{scheme}://{domain}'.format(
            scheme=storage_share.uri['scheme'],
            domain=storage_share.uri['domain']
        )

    _connection = boto3.client(
        's3',
        region_name=storage_share.plugin_settings['s3.region'],
        endpoint_url=_api_url,
        aws_access_key_id=storage_share.plugin_settings['s3.pub_key'],
        aws_secret_access_key=storage_share.plugin_settings['s3.priv_key'],
        use_ssl=True,
        verify=storage_share.plugin_settings['ssl_check'],
        config=Config(
            signature_version=storage_share.plugin_settings['s3.signature_ver'],
            connect_timeout=int(storage_share.plugin_settings['conn_timeout']),
            retries=dict(max_attempts=0)
        ),
    )

    _total_bytes = 0
    _total_files = 0

    # We define the arguments for the API call. Delimiter is set to *
    # to get all keys. This is necessary for AWS to return the "NextMarker"
    # attribute necessary to iterate when there are > 1,000 objects.

    _kwargs = {
        'Bucket': storage_share.uri['bucket'],
        'Delimiter': '*'
    }

    # This loop is needed to obtain all objects as the API can only
    # server 1,000 objects per request. The 'NextMarker' tells where
    # to start the next 1,000. If no 'NextMarker' is received, all
    # objects have been obtained.
    _logger.debug(
        "[%s]Requesting storage stats with: URN: %s API Method: %s Payload: %s",
        storage_share.id,
        _api_url,
        storage_share.plugin_settings['storagestats.api'].lower(),
        _kwargs
    )

    while True:
        try:
            _response = _connection.list_objects(**_kwargs)

        except botoExceptions.ClientError as ERR:
            raise exceptions.DSSConnectionError(
                error=ERR.__class__.__name__,
                status_code=ERR.response['ResponseMetadata']['HTTPStatusCode'],
                debug=str(ERR),
            )

        except botoRequestsExceptions.InvalidSchema as ERR:
            raise exceptions.DSSConnectionErrorInvalidSchema(
                error='InvalidSchema',
                schema=storage_share.uri['scheme'],
                debug=str(ERR),
            )

        except botoRequestsExceptions.SSLError as ERR:
            # If ca_path is custom, try the default in case
            # a global setting is incorrectly giving the wrong
            # ca's to check agains.
            _connection = boto3.client(
                's3',
                region_name=storage_share.plugin_settings['s3.region'],
                endpoint_url=_api_url,
                aws_access_key_id=storage_share.plugin_settings['s3.pub_key'],
                aws_secret_access_key=storage_share.plugin_settings['s3.priv_key'],
                use_ssl=True,
                verify=True,
                config=Config(
                    signature_version=storage_share.plugin_settings['s3.signature_ver'],
                    connect_timeout=int(storage_share.plugin_settings['conn_timeout']),
                    retries=dict(max_attempts=0)
                ),
            )

            try:
                _response = _connection.list_objects(**_kwargs)

            except botoRequestsExceptions.SSLError as ERR:
                raise exceptions.DSSConnectionError(
                    error=ERR.__class__.__name__,
                    status_code="092",
                    debug=str(ERR),
                )

        except botoRequestsExceptions.RequestException as ERR:
            raise exceptions.DSSConnectionError(
                error=ERR.__class__.__name__,
                status_code="400",
                debug=str(ERR),
            )

        except botoExceptions.ParamValidationError as ERR:
            raise exceptions.DSSConnectionError(
                error=ERR.__class__.__name__,
                status_code="095",
                debug=str(ERR),
            )

        except botoExceptions.BotoCoreError as ERR:
            raise exceptions.DSSConnectionError(
                error=ERR.__class__.__name__,
                status_code="400",
                debug=str(ERR),
            )

        else:
            # This outputs a lot of information, might not be necessary.
            # _logger.debug(
            #     "[%s]Endpoint reply: %s",
            #     storage_share.id,
            #     response['Contents']
            # )

            try:
                _response['Contents']
            except KeyError:
                storage_share.stats['bytesused'] = '0'
                break
            else:
                for _file in _response['Contents']:
                    _total_bytes += _file['Size']
                    _total_files += 1

            try:
                _kwargs['Marker'] = _response['NextMarker']
            except KeyError:
                break

    # Save time when data was obtained.
    storage_share.stats['endtime'] = int(time.time())
    storage_share.stats['bytesused'] = _total_bytes

    if storage_share.plugin_settings['storagestats.quota'] == 'api':
        storage_share.stats['quota'] = dynafed_storagestats.helpers.convert_size_to_bytes("1TB")
        storage_share.stats['filecount'] = _total_files
        storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']
        raise exceptions.DSSQuotaWarning(
            error="NoQuotaGiven",
            status_code="098",
            default_quota=storage_share.stats['quota'],
        )

    else:
        storage_share.stats['quota'] = storage_share.plugin_settings['storagestats.quota']
        storage_share.stats['filecount'] = _total_files
        storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']


# def ():
#     """
#
#     """
#     ############# Creating loggers ################
#     _logger = logging.getLogger(__name__)
#     ###############################################