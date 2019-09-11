"""Helper functions used to contact S3 based API's."""

import datetime
import logging

import boto3
import botocore.vendored.requests.exceptions as botoRequestsExceptions
import botocore.exceptions as botoExceptions
from botocore.client import Config

import requests
from requests_aws4auth import AWS4Auth

import dynafed_storagestats.exceptions
import dynafed_storagestats.helpers
import dynafed_storagestats.time

###############
## Functions ##
###############

def ceph_admin(storage_share):
    """Contact S3 endpoint using Ceph's Admin API.

    Obtains the storage stats from CephS3's Admin API as long as the endpoint
    has been configured to allow such requests by the user whose credentials
    are being supplied. User needs bucket "caps". See Ceph's documentation:
    https://docs.ceph.com/docs/master/radosgw/admin/

    Attributes:
    storage_share -- dynafed_storagestats StorageShare object.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    # Generate the API's URL to contact.
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

    # API attributes.
    _payload = {
        'bucket': storage_share.uri['bucket'],
        'stats': 'True'
    }

    # Create authorization object.
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
        storage_share.stats['endtime'] = int(datetime.datetime.now().timestamp())

        #Log contents of response
        _logger.debug(
            "[%s]Endpoint reply: %s",
            storage_share.id,
            _response.text
        )

    except requests.exceptions.InvalidSchema as ERR:
        raise dynafed_storagestats.exceptions.ConnectionErrorInvalidSchema(
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
            storage_share.stats['endtime'] = int(datetime.datetime.now().timestamp())

            #Log contents of response
            _logger.debug(
                "[%s]Endpoint reply: %s",
                storage_share.id,
                _response.text
            )

        except requests.exceptions.SSLError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="092",
                debug=str(ERR),
            )

    except requests.ConnectionError as ERR:
        raise dynafed_storagestats.exceptions.ConnectionError(
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
                raise dynafed_storagestats.exceptions.ConnectionErrorS3API(
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
                raise dynafed_storagestats.exceptions.ErrorS3MissingBucketUsage(
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
                        raise dynafed_storagestats.exceptions.CephS3QuotaDisabledWarning(
                            default_quota=storage_share.stats['quota'],
                        )

                    else:
                        storage_share.stats['quota'] = dynafed_storagestats.helpers.convert_size_to_bytes("1TB")
                        storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']
                        raise dynafed_storagestats.exceptions.QuotaWarning(
                            error="NoQuotaGiven",
                            status_code="098",
                            default_quota=storage_share.stats['quota'],
                        )


def cloudwatch(storage_share):
    """Contact S3 endpoint using AWS Cloudwatch API.

    If the metrics BucketSizeBytes and NumberOfObjects have been set in AWS
    Cloudwatch, this function contacts Cloudwatch's API and obtains those
    "Maximum" numbers for the past day. See AWS documentation for more info:
    https://docs.aws.amazon.com/cloudwatch/index.html

    Attributes:
    storage_share -- dynafed_storagestats StorageShare object.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    _seconds_in_one_day = 86400

    # Generate boto client to query AWS API.
    _connection = get_cloudwatch_boto_client(storage_share)

    # Define cloudwatch metrics to probe. 'Result' key will be used to store the
    # metric. For the other keys, if unsure, try the following in cli to obtain
    # what to add, assuming aws is installed and configured.
    # Change the namespace accordingly:
    # aws cloudwatch list-metrics --namespace AWS/S3
    _metrics = {
        'BucketSizeBytes': {
            'Namespace': 'AWS/S3',
            'Statistics': ['Maximum'],
            'Unit': 'Bytes',
            'Dimensions': [
                {
                    'Name': 'BucketName',
                    'Value': storage_share.uri['bucket']
                },
                {
                    'Name': 'StorageType',
                    'Value': 'StandardStorage'
                }
            ],
            'Result': 0,
        },
        'NumberOfObjects': {
            'Namespace': 'AWS/S3',
            'Statistics': ['Maximum'],
            'Unit': 'Count',
            'Dimensions': [
                {
                    'Name': 'BucketName',
                    'Value': storage_share.uri['bucket']
                },
                {
                    'Name': 'StorageType',
                    'Value': 'AllStorageTypes'
                }
            ],
            'Result': 0,
        }
    }

    _logger.debug(
        "[%s]Requesting storage stats with: API Method: %s",
        storage_share.id,
        storage_share.plugin_settings['storagestats.api'].lower(),
    )

    # Requesting the information for each defined metric.
    for _metric in _metrics:
        _logger.info(
            "[%s]Requesting Cloudwatch metric: %s",
            storage_share.id,
            _metric,
        )
        try:
            _response = _connection.get_metric_statistics(
                Period=_seconds_in_one_day,
                MetricName=_metric,
                Namespace=_metrics[_metric]['Namespace'],
                StartTime=datetime.datetime.utcnow() - datetime.timedelta(days=1),
                EndTime=datetime.datetime.utcnow(),
                Statistics=_metrics[_metric]['Statistics'],
                Unit=_metrics[_metric]['Unit'],
                Dimensions=_metrics[_metric]['Dimensions']
            )

        except botoExceptions.ClientError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code=ERR.response['ResponseMetadata']['HTTPStatusCode'],
                debug=str(ERR),
            )

        except botoRequestsExceptions.SSLError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="092",
                debug=str(ERR),
            )

        except botoRequestsExceptions.RequestException as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="400",
                debug=str(ERR),
            )

        except botoExceptions.ParamValidationError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="095",
                debug=str(ERR),
            )

        except botoExceptions.BotoCoreError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="400",
                debug=str(ERR),
            )

        else:
            _logger.debug(
                "[%s]Response from Cloudwatch metric: %s: %s",
                storage_share.id,
                _metric,
                _response
            )
            # Extract the metric value from the response.
            _metrics[_metric]['Result'] = \
                _response['Datapoints'][0][
                    _metrics[_metric]['Statistics'][0]
                ]


    # Save the timestamp when data was obtained.
    storage_share.stats['endtime'] = int(datetime.datetime.now().timestamp())

    # Save metrics to storage_share.
    storage_share.stats['bytesused'] = int(_metrics['BucketSizeBytes']['Result'])
    storage_share.stats['filecount'] = int(_metrics['NumberOfObjects']['Result'])

    # Obtain or set default quota and calculate freespace.
    if storage_share.plugin_settings['storagestats.quota'] == 'api':
        storage_share.stats['quota'] = dynafed_storagestats.helpers.convert_size_to_bytes("1TB")
        storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']
        raise dynafed_storagestats.exceptions.QuotaWarning(
            error="NoQuotaGiven",
            status_code="098",
            default_quota=storage_share.stats['quota'],
        )

    else:
        storage_share.stats['quota'] = storage_share.plugin_settings['storagestats.quota']
        storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']


def get_cloudwatch_boto_client(storage_share):
        """Return boto client from storage share object.

        Arguments:
        storage_share -- dynafed_storagestats StorageShare object.

        Returns:
        botocore.client.cloudwatch

        """
        # Generate boto client to query AWS API.
        _connection = boto3.client(
            'cloudwatch',
            region_name=storage_share.plugin_settings['s3.region'],
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

        return _connection


def get_s3_boto_client(storage_share):
        """Return boto client from storage share object.

        Arguments:
        storage_share -- dynafed_storagestats StorageShare object.

        Returns:
        botocore.client.S3

        """
        # Generate the API's URL to contact.
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

        # Generate boto client to query S3 endpoint.
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

        return _connection


def list_objects(storage_share, delta=1, prefix='',
                 report_file='/tmp/filelist_report.txt',
                 request='storagestats'
                 ):
    """Contact S3 endpoint using list_objects API.

    Contacts an S3 endpoints and uses the "list_objects" API to recursively
    obtain all the objects in a container and sum their size to obtain total
    space usage.

    Attributes:
    storage_share -- dynafed_storagestats StorageShare object.
    prefix -- string.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    # Generate boto client to query S3 endpoint.
    _connection = get_s3_boto_client(storage_share)

    # Initialize counters.
    _total_bytes = 0
    _total_files = 0

    # We define the arguments for the API call. Delimiter is set to *
    # to get all keys. This is necessary for AWS to return the "NextMarker"
    # attribute necessary to iterate when there are > 1,000 objects.
    _kwargs = {
        'Bucket': storage_share.uri['bucket'],
        'Delimiter': '*',
        'Prefix': prefix,
    }

    _logger.debug(
        "[%s]Requesting storage stats with: URN: %s API Method: %s Payload: %s",
        storage_share.id,
        _connection._endpoint,
        storage_share.plugin_settings['storagestats.api'].lower(),
        _kwargs
    )

    # This loop is needed to obtain all objects as the API can only
    # server 1,000 objects per request. The 'NextMarker' tells where
    # to start the next 1,000. If no 'NextMarker' is received, all
    # objects have been obtained.
    while True:
        _logger.info(
            '[%s]Executing boto client method "%s"',
            storage_share.id,
            'list_objects'
        )
        _logger.debug(
            '[%s]Boto client arguments: %s',
            storage_share.id,
            _kwargs
        )

        _response = run_boto_client(_connection, 'list_objects', _kwargs)

        # This outputs a lot of information, might not be necessary.
        # _logger.debug(
        #     "[%s]Endpoint reply: %s",
        #     storage_share.id,
        #     response['Contents']
        # )

        # Check what type of request is asked being used.
        if request == 'storagestats':
            try:
                # Make sure we got a list of objects.
                _response['Contents']
            except KeyError:
                storage_share.stats['bytesused'] = 0
                break
            else:
                for _file in _response['Contents']:
                    _total_bytes += _file['Size']
                    _total_files += 1

        elif request == 'filelist':
            try:
                # Make sure we got a list of objects.
                _response['Contents']
            except KeyError:
                break
            else:
                for _file in _response['Contents']:
                    # Output files older than the specified delta.
                    if dynafed_storagestats.time.mask_timestamp_by_delta(_file['LastModified'], delta):
                        report_file.write("%s\n" % _file['Key'])
                        _total_files += 1

        # Exit if no "NextMarker" as list is now over.
        try:
            _kwargs['Marker'] = _response['NextMarker']
        except KeyError:
            break

    # Save time when data was obtained.
    storage_share.stats['endtime'] = int(datetime.datetime.now().timestamp())

    # Process the result for the storage stats.
    if request == 'storagestats':
        storage_share.stats['bytesused'] = _total_bytes

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
            storage_share.stats['quota'] = storage_share.plugin_settings['storagestats.quota']
            storage_share.stats['filecount'] = _total_files
            storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']


def run_boto_client(boto_client, method, kwargs):
        """Contact S3 endopint using passed object methods and arguments.


        Returns:
        Dict containing reply from S3 endpoint.

        """
        ############# Creating loggers ################
        _logger = logging.getLogger(__name__)
        ###############################################

        _function = getattr(boto_client, method)

        try:
            result = _function(**kwargs)

        except botoExceptions.ClientError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code=ERR.response['ResponseMetadata']['HTTPStatusCode'],
                debug=str(ERR),
            )

        except botoRequestsExceptions.InvalidSchema as ERR:
            raise dynafed_storagestats.exceptions.ConnectionErrorInvalidSchema(
                error='InvalidSchema',
                schema=storage_share.uri['scheme'],
                debug=str(ERR),
            )

        except botoRequestsExceptions.SSLError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="092",
                debug=str(ERR),
            )

        except botoRequestsExceptions.RequestException as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="400",
                debug=str(ERR),
            )

        except botoExceptions.ParamValidationError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="095",
                debug=str(ERR),
            )

        except botoExceptions.BotoCoreError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="400",
                debug=str(ERR),
            )

        else:
            return result

# def ():
#     """
#
#     """
#     ############# Creating loggers ################
#     _logger = logging.getLogger(__name__)
#     ###############################################
