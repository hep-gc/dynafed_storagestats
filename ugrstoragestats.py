#!/usr/bin/python
"""
Module to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints.

Prerequisites:
    Modules:
    - lxml
    - memcache
    - requests
    - requests_aws4auth

v0.0.1 Works with cephS3 AdminAPI.
v0.0.2 Added AWS list-type2 API to list all objects in bucket and add object size.
       Added memcached format and upload each endpoint obtained.
v0.0.3 Added support for python3
v0.0.4 Changed from single configration file to all *.conf files in given directory.
v0.0.5 Added module import checks.
v0.0.6 StorageStats object class chosen dynamically based on configured plugin.
v0.0.7 Added options
v0.1.0 Changed aws-list to generic and now uses boto3 for generality.
v0.2.0 Added validators key and 'validate_options' function.
v0.2.1 Cleaned up code to PEP8.
v0.2.2 Exception for plugint types not yet implemented.
v0.2.3 Fixed bucket-name issue if not at paths' root and non-standard ports for
        S3 endpoints.
v0.2.4 Added URL schema validator function and updated code. Works for dav,davs.
v0.2.5 Moved schema validator from fuction to class method. Added for S3.
v0.2.6 Tested with Minio
v0.2.7 Added files counted to S3 Generic.
v0.2.8 Changed S3 generic API from list_objects_v2 to list_objects as CephS3
       doesn't have the "NextContinuationToken" directive and thus would only
       list the first 1000. This needs to be updated one Ceph has this as
       v1 is sort of deprecated.
v0.2.9 Added ability to specify S3 signature version.
v0.2.10 Added options for memcached, stdoutput and some debugging.
v0.2.11 Fixed issue with ID names with multiple "."
v0.3.0 Added DAV/Http support.
v0.3.1 Added exceptions and logic when ceph-admin option fails.
v0.3.2 Added bytesfree counts for S3 endpoints and exception for aws
       ceph-admin error.
v0.3.3 Fixed exception handling for python3 syntax.
v0.3.4 Fixed json parsing from requests using it's native json function to
       solve issue with json module of python 3.4.
v0.4.0 Re-wrote the exception classes and how they are treated in code. Added
       warnings.
v0.4.1 Added exceptions and error handling for S3 storagestats ceph-admin.
v0.4.2 Added exceptions and error handling for S3 storagestats, generic.
v0.4.3 Added exceptions for configuration file errors, missing options,
       unsupported plugins.
v0.4.4 Added exceptions and error handling for DAV storagestats.
v0.4.5 Changed error to use the exception names. Works better and cleaner.
v0.4.6 ssl_check now uses the ca_path if declared and ssl_check is true.
v0.4.7 Removed the warnings and instead added a status and debug attribute
       to StorageStats objects. Status appends the last ERROR. Debug appends
       all the ones that occur with more detail if available.
v0.4.8 Improved memcached and status/debug output.
v0.4.9 Added timestamp and execbeat output.
v0.5.0 Added memcached exceptions, error messages. Added option for execbeat
       output.
"""
from __future__ import print_function

__version__ = "v0.5.0"
__author__ = "Fernando Fernandez Galindo"

import re
import warnings
import sys
import os
import time
from io import BytesIO
from optparse import OptionParser, OptionGroup
import glob
import json

IS_PYTHON2 = sys.version_info[0] == 2

if IS_PYTHON2:
    from urlparse import urlsplit
else:
    from urllib.parse import urlsplit

try:
    import boto3
except ImportError:
    print('ImportError: Please install "boto3" modules')
    sys.exit(1)
else:
    from botocore.client import Config
    import botocore.vendored.requests.exceptions as botoRequestsExceptions
    import botocore.exceptions as botoExceptions

try:
    from lxml import etree
except ImportError:
    print('ImportError: Please install "lxml" modules')
    sys.exit(1)

try:
    import memcache
except ImportError:
    print('ImportError: Please install "memcache" modules')
    sys.exit(1)

try:
    import requests
except ImportError:
    print('ImportError: Please install "requests" modules')
    sys.exit(1)

try:
    from requests_aws4auth import AWS4Auth
except ImportError:
    print('ImportError: Please install "requests_aws4auth" modules')
    sys.exit(1)


################
## Help/Usage ##
################

usage = "usage: %prog [options]"
parser = OptionParser(usage)

#parser.add_option('-v', '--verbose', dest='verbose', action='count', help='Increase verbosity level for debugging this script (on stderr)')
parser.add_option('-d', '--dir',
                  dest='configs_directory', action='store', default='/etc/ugr/conf.d',
                  help="Path to UGR's endpoint .conf files."
                 )

group = OptionGroup(parser, "Memcached options")
group.add_option('--memhost',
                 dest='memcached_ip', action='store', default='127.0.0.1',
                 help='IP or hostname of memcached instance. Default: 127.0.0.1'
                )
group.add_option('--memport',
                 dest='memcached_port', action='store', default='11211',
                 help='Port where memcached instances listens on. Default: 11211'
                )

parser.add_option_group(group)

group = OptionGroup(parser, "Output options")
group.add_option('--debug',
                 dest='debug', action='store_true', default=False,
                 help='Declare to enable debug output on stdout.'
                )
group.add_option('-m', '--memcached',
                 dest='output_memcached', action='store_true', default=False,
                 help='Declare to enable uploading information to memcached.'
                )
group.add_option('-e', '--execbeat',
                 dest='output_execbeat', action='store_true', default=False,
                 help='Declare to enable uploading information to execbeat.'
                )
group.add_option('--stdout',
                 dest='output_stdout', action='store_true', default=False,
                 help='Set to output stats on stdout. If no other output option is set, this is enabled by default.'
                )

#group.add_option('-o', '--outputfile',
#                 dest='out_file', action='store', default=None,
#                 help='Change where to ouput the data. Default: None'
#                )
parser.add_option_group(group)

options, args = parser.parse_args()


#######################
## Exception Classes ##
#######################

class UGRBaseException(Exception):
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = "[ERROR] An unkown exception occured processing"
        else:
            self.message = message
        if debug is None:
            self.debug = message
        else:
            self.debug = debug
        super(UGRBaseException, self).__init__(self.message)

### Defining Error Exception Classes
class UGRBaseError(UGRBaseException):
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = "[ERROR][ERROR][000] A unkown error occured."
        else:
            self.message = "[ERROR]" + message
        self.debug = debug
        super(UGRBaseError, self).__init__(self.message, self.debug)

class UGRConfigFileError(UGRBaseError):
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = "[ConfigFileError][000] An unkown error occured reading a configuration file."
        else:
            self.message = message
        self.debug = debug
        super(UGRConfigFileError, self).__init__(self.message, self.debug)

class UGRUnsupportedPluginError(UGRConfigFileError):
    def __init__(self, endpoint, error=None, status_code="000", plugin=None, debug=None):
        self.message ='[%s][%s] StorageStats method for "%s" not implemented yet.' \
                  % (error, status_code, plugin)
        self.debug = debug
        super(UGRUnsupportedPluginError, self).__init__(self.message, self.debug)

class UGRConfigFileErrorIDMismatch(UGRConfigFileError):
    def __init__(self, endpoint, line, error=None, status_code="000", debug=None):
        self.message ='[%s][%s] Failed to match ID in line "%s". Check your configuration.' \
                  % (error, status_code, line)
        self.debug = debug
        super(UGRConfigFileErrorIDMismatch, self).__init__(self.message, self.debug)

class UGRConfigFileErrorMissingRequiredOption(UGRConfigFileError):
    def __init__(self, endpoint, option, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] "%s" is required. Check your configuration.' \
                  % (error, status_code, option)
        self.debug = debug
        super(UGRConfigFileErrorMissingRequiredOption, self).__init__(self.message, self.debug)

class UGRConfigFileErrorInvalidOption(UGRConfigFileError):
    def __init__(self, endpoint, option, valid_options, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Incorrect value given in option "%s". Valid options: %s' \
                  % (error, status_code, option, valid_options)
        self.debug = debug
        super(UGRConfigFileErrorInvalidOption, self).__init__(self.message, self.debug)

class UGRStorageStatsError(UGRBaseError):
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = "[StorageStatsError][000] An unkown error occured obtaning storage stats."
        else:
            self.message = message
        self.debug = debug
        super(UGRStorageStatsError, self).__init__(self.message, self.debug)

class UGRMemcachedError(UGRBaseError):
    def __init__(self, message=None, debug=None):
        if message is None:
            self.message = '[MemcachedError][000] Unknown memcached error.'
        else:
            self.message = message
        self.debug = debug
        super(UGRMemcachedError, self).__init__(self.message, self.debug)

class UGRStorageStatsMemcachedConnectionError(UGRMemcachedError):
    def __init__(self, endpoint, error=None, status_code="400", debug=None):
        self.message = '[%s][%s] Failed to connect to memcached.' \
                       % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsMemcachedConnectionError, self).__init__(self.message, self.debug)

class UGRStorageStatsMemcachedIndexError(UGRMemcachedError):
    def __init__(self, endpoint, error=None, status_code="404", debug=None):
        self.message = '[%s][%s] Unable to get memcached index contents.' \
                       % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsMemcachedIndexError, self).__init__(self.message, self.debug)

class UGRStorageStatsConnectionError(UGRStorageStatsError):
    def __init__(self, endpoint, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Failed to establish a connection.' \
                       % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsConnectionError, self).__init__(self.message, self.debug)

class UGRStorageStatsConnectionErrorS3API(UGRStorageStatsError):
    def __init__(self, endpoint, error=None, status_code="000", api=None, debug=None):
        self.message = '[%s][%s] Error requesting stats using API "%s".' \
                  % (error, status_code, api)
        self.debug = debug
        super(UGRStorageStatsConnectionErrorS3API, self).__init__(self.message, self.debug)

class UGRStorageStatsErrorS3MissingBucketUsage(UGRStorageStatsError):
    def __init__(self, endpoint, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Failed to get bucket usage information.' \
                  % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsErrorS3MissingBucketUsage, self).__init__(self.message, self.debug)

class UGRStorageStatsErrorDAVQuotaMethod(UGRStorageStatsError):
    def __init__(self, endpoint, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] WebDAV Quota Method.' \
                  % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsErrorDAVQuotaMethod, self).__init__(self.message, self.debug)

class UGRStorageStatsConnectionErrorDAVCertPath(UGRStorageStatsError):
    def __init__(self, endpoint, error=None, status_code="000", certfile=None, debug=None):
        self.message = '[%s][%s] Invalid client certificate path "%s".' \
                  % (error, status_code, certfile)
        self.debug = debug
        super(UGRStorageStatsConnectionErrorDAVCertPath, self).__init__(self.message, self.debug)

### Defining Warning Exception Classes
class UGRBaseWarning(UGRBaseException):
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = '[WARN][WARN][000] A unkown error occured.'
        else:
            self.message = '[WARN]' + message
        self.debug = debug
        super(UGRBaseWarning, self).__init__(self.message, self.debug)

class UGRConfigFileWarning(UGRBaseWarning):
    def __init__(self, message=None, error=None, status_code="000", debug=None):
        if message is None:
            # Set some default useful error message
            self.message = '[%s][%s] An unkown error occured reading a configuration file.' \
                           % (error, status_code)
        self.debug = debug
        super(UGRConfigFileWarning, self).__init__(self.message, self.debug)

class UGRConfigFileWarningMissingOption(UGRConfigFileWarning):
    def __init__(self, endpoint, option, option_default, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Unspecified "%s" option. Using default value "%s"' \
                  % (error, status_code, option, option_default)
        self.debug = debug
        super(UGRConfigFileWarningMissingOption, self).__init__(self.message, self.debug)

class UGRStorageStatsWarning(UGRBaseWarning):
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = '[StorageStatsWarning][000] An unkown error occured reading storage stats' \
                           % (error, status_code)
        else:
            self.message = message
        self.debug = debug
        super(UGRStorageStatsWarning, self).__init__(self.message, self.debug)

class UGRStorageStatsQuotaWarning(UGRStorageStatsWarning):
    def __init__(self, endpoint, error="NoQuotaGiven", status_code="000", debug=None):
        self.message = '[%s][%s] No quota obtained from API or configuration file. Using default of 1TB' \
                    % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsQuotaWarning, self).__init__(self.message, self.debug)

class UGRStorageStatsCephS3QuotaDisabledWarning(UGRStorageStatsWarning):
    def __init__(self, endpoint, error="BucketQuotaDisabled", status_code="000", debug=None):
        self.message = '[%s][%s] Bucket quota is disabled. Using default of 1TB' \
                  % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsCephS3QuotaDisabledWarning, self).__init__(self.message, self.debug)


#####################
## Storage Classes ##
#####################

class StorageStats(object):
    """
    Class that will define how data from UGR's configruation file will be stored
    for earch storage endpoint. As well as how to obtain stats and output it.
    """
    def __init__(self, _ep):
        self.stats = {
                      'bytesused': 0,
                      'bytesfree': 0,
                      'files': 0,
                      'quota': 1000**4,
                      'timestamp': int(time.time()),
                     }
        self.id = _ep['id']
        self.options = _ep['options']
        self.plugin = _ep['plugin']
        self.url = _ep['url']

        self.debug = []
        self.status = '[OK][OK][200]'

        self.validators = {
            'quota': {
                'default': 'api',
                'required': False,
            },
            'ssl_check': {
                'boolean': True,
                'default': True,
                'required': False,
                'valid': ['true', 'false', 'yes', 'no']
            },
        }

    def upload_to_memcached(self, memcached_ip='127.0.0.1', memcached_port='11211'):
        """
        Connects to a memcached instance and uploads the endpoints storage stats:
        self.id, self.stats['quota'], self.stats['bytesused']
        """
        memcached_srv = memcached_ip + ':' + memcached_port
        mc = memcache.Client([memcached_srv])
        memcached_index = "Ugrstoragestats_" + self.id
        storagestats = '%%'.join([
                                  self.id,
                                  str(self.stats['timestamp']),
                                  str(self.stats['quota']),
                                  str(self.stats['bytesused']),
                                  str(self.stats['bytesfree']),
                                  self.status,
                                ])
        try:
            if mc.set(memcached_index, storagestats) == 0:
                raise UGRStorageStatsMemcachedConnectionError(endpoint=self.id)

        except UGRStorageStatsMemcachedConnectionError as ERR:
            self.debug.append(ERR.debug)
            self.status = ERR.message

    def get_from_memcached(self, memcached_ip='127.0.0.1', memcached_port='11211'):
        """
        Connects to a memcached instance and tries to obtain the storage stats
        from the index belonging to the endpoint making the call. If no index
        is found, the stats are created in the same style as upload_to_memcached
        with error information for debugging and logging
        """
        mc = memcache.Client([options.memcached_ip + ':' + options.memcached_port])
        memcached_index = "Ugrstoragestats_" + self.id
        try:
            memcached_contents = mc.get(memcached_index)
            if memcached_contents is None:
                raise UGRStorageStatsMemcachedIndexError(
                                                    endpoint = self.id,
                                                    status_code="000",
                                                    error='MemcachedEmptyIndex'
                                                    )

        except UGRStorageStatsMemcachedIndexError as ERR:
            self.debug.append(ERR.debug)
            self.status = ERR.message
            memcached_contents = '%%'.join([
                                            self.id,
                                            str(self.stats['timestamp']),
                                            str(self.stats['quota']),
                                            str(self.stats['bytesused']),
                                            str(self.stats['bytesfree']),
                                            self.status,
                                    ])
        finally:
            return(memcached_contents)

    def get_storagestats(self):
        """
        Method for obtaining contacting a storage endpoint and obtain storage
        stats. Will be re-defined for each sub-class as each storage endpoint
        type requires different API's.
        """
        pass

    def validate_options(self,options):
        """
        Check the endpoints options from UGR's configuration file against the
        set of default and valid options defined under the self.validators dict.
        """
        for ep_option in self.validators:
            # First check if the option has been defined in the config file..
            # If it is missing, check if it is required, and exit if true
            # otherwise set it to the default value and print a warning.
            try:
                self.options[ep_option]

            except KeyError:
                try:
                    if self.validators[ep_option]['required']:
                        self.options.update({ep_option: ''})
                        raise UGRConfigFileErrorMissingRequiredOption(
                                  endpoint=self.id,
                                  error="MissingRequiredOption",
                                  option=ep_option,
                              )
                    else:
                        raise UGRConfigFileWarningMissingOption(
                                  endpoint=self.id,
                                  error="MissingOption",
                                  option=ep_option,
                                  option_default=self.validators[ep_option]['default'],
                              )
                except UGRBaseWarning as WARN:
                    self.debug.append(WARN.debug)
                    self.status = WARN.message
                    self.options.update({ep_option: self.validators[ep_option]['default']})

            # If the ep_option has been defined, check against a list of valid
            # options (if defined, otherwise contiune). Also transform to boolean
            # form those that have the "boolean" key set as true.
            else:
                try:
                    if self.options[ep_option] not in self.validators[ep_option]['valid']:
                        raise UGRConfigFileErrorInvalidOption(
                                endpoint=self.id,
                                error="InvalidOption",
                                option=ep_option,
                                valid_options=self.validators[ep_option]['valid']
                              )
                    else:
                        try:
                            self.validators[ep_option]['boolean']
                        except KeyError:
                            pass
                        else:
                            if self.options[ep_option].lower() == 'false'\
                            or self.options[ep_option].lower() == 'no':
                                self.options.update({ep_option: False})
                            else:
                                self.options.update({ep_option: True})
                except KeyError:
                    # The 'valid' key is not required to exist.
                    pass
        # If user has specified an SSL CA bundle:
        if self.options['ssl_check']:
            try:
                self.options['ssl_check'] = self.options['ca_path']
            except KeyError:
                # The ssl_check will stay True and standard CA bundle will be used.
                pass

        # Check the quota option and transform it into bytes if necessary.
        if self.options['quota'] != "api":
            self.options['quota'] = convert_size_to_bytes(self.options['quota'])



    def validate_schema(self, scheme):
        schema_translator = {
            'dav': 'http',
            'davs': 'https',
        }

        if scheme in schema_translator:
            return (schema_translator[scheme])
        else:
            return (scheme)



class S3StorageStats(StorageStats):
    """
    Subclass that defines methods for obtaining storage stats of S3 endpoints.
    """
    def __init__(self, *args, **kwargs):
        """
        Extend the object's validators unique to the storage type to make sure
        the storage status check can proceed.
        """
        super(S3StorageStats, self).__init__(*args, **kwargs)
        self.validators.update({
            's3.alternate': {
                'default': 'false',
                'required': False,
                'valid': ['true', 'false', 'yes', 'no']
            },
            's3.api': {
                'default': 'generic',
                'required': True,
                'valid': ['ceph-admin', 'generic'],
            },
            's3.priv_key': {
                'required': True,
            },
            's3.pub_key': {
                'required': True,
            },
            's3.region': {
                'required': True,
            },
            's3.signature_ver': {
                'default': 's3v4',
                'required': False,
                'valid': ['s3', 's3v4'],
            },
        })

    def get_storagestats(self):
        """
        Connect to the storage endpoint with the defined or generic API's
        to obtain the storage status.
        """
        # Split the URL in the configuration file for validation and proper
        # formatting according to the method's needs.
        u = urlsplit(self.url)
        scheme = self.validate_schema(u.scheme)

        # Getting the storage Stats CephS3's Admin API
        if self.options['s3.api'].lower() == 'ceph-admin':
            if self.options['s3.alternate'].lower() == 'true'\
            or self.options['s3.alternate'].lower() == 'yes':
                endpoint_url = '{uri_scheme}://{uri.netloc}/admin/bucket?format=json'.format(uri=u, uri_scheme=scheme)
                bucket = u.path.rpartition("/")[-1]
                payload = {'bucket': bucket, 'stats': 'True'}

            else:
                bucket, domain = u.netloc.partition('.')[::2]
                endpoint_url = '{uri_scheme}://{uri_domain}/admin/{uri_bucket}?format=json'.format(uri=u, uri_scheme=scheme, uri_bucket=bucket, uri_domain=domain)
                payload = {'bucket': bucket, 'stats': 'True'}

            auth = AWS4Auth(self.options['s3.pub_key'],
                            self.options['s3.priv_key'],
                            self.options['s3.region'],
                            's3',
                           )
            try:
                r = requests.get(
                                 url=endpoint_url,
                                 params=payload,
                                 auth=auth,
                                 verify=self.options['ssl_check'],
                                )

            except requests.ConnectionError as ERR:
                raise UGRStorageStatsConnectionError(
                                                     endpoint=self.id,
                                                     error=ERR.__class__.__name__,
                                                     status_code="000",
                                                     debug=str(ERR),
                                                    )
            else:
                # If ceph-admin is accidentally requested for AWS, no JSON content
                # is passed, so we check for that.
                # Review this!
                try:
                    stats = r.json()
                except ValueError:
                    raise UGRStorageStatsConnectionErrorS3API(
                                                       endpoint=self.id,
                                                       status_code=r.status_code,
                                                       error="NoContent",
                                                       api=self.options['s3.api'],
                                                       debug=r.content,
                                                      )

                # Make sure we get a Bucket Usage information.
                # Fails on empty (in minio) or newly created buckets.
                try:
                    stats['usage']

                except KeyError as ERR:

                    raise UGRStorageStatsErrorS3MissingBucketUsage(
                                                                    endpoint=self.id,
                                                                    status_code=r.status_code,
                                                                    error=stats['Code'],
                                                                    debug=stats
                                                                   )
                else:
                    if len(stats['usage']) != 0:
                        # If the bucket is emtpy, then just keep going we
                        self.stats['bytesused'] = stats['usage']['rgw.main']['size_utilized']

                        # raise UGRStorageStatsErrorS3MissingBucketUsage(
                        #                                                 endpoint=self.id,
                        #                                                 status_code=r.status_code,
                        #                                                 error="NewEmptyBucket",
                        #                                                 debug=stats
                        #                                                )
                    if self.options['quota'] != 'api':
                        self.stats['quota'] = self.options['quota']
                        self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

                    else:
                        if stats['bucket_quota']['enabled'] == True:
                            self.stats['quota'] = stats['bucket_quota']['max_size']
                            self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

                        elif stats['bucket_quota']['enabled'] == False:
                            self.stats['quota'] = convert_size_to_bytes("1TB")
                            self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']
                            raise UGRStorageStatsCephS3QuotaDisabledWarning(
                                                        endpoint=self.id,
                                                        )
                        else:
                            self.stats['quota'] = convert_size_to_bytes("1TB")
                            self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']
                            raise UGRStorageStatsQuotaWarning(
                                                  endpoint = self.id,
                                                  error="NoQuotaGiven",
                                                  status_code="000",
                            )

        # Getting the storage Stats AWS S3 API
        #elif self.options['s3.api'].lower() == 'aws-cloudwatch':

        # Generic list all objects and add sizes using list-objectsv2 AWS-Boto3
        # API, should work for any compatible S3 endpoint.
        elif self.options['s3.api'].lower() == 'generic':
            if self.options['s3.alternate'].lower() == 'true'\
            or self.options['s3.alternate'].lower() == 'yes':
                endpoint_url = '{uri_scheme}://{uri.netloc}'.format(uri=u, uri_scheme=scheme)
                bucket = u.path.rpartition("/")[-1]

            else:
                bucket, domain = u.netloc.partition('.')[::2]
                endpoint_url = '{uri_scheme}://{uri_domain}'.format(uri=u, uri_scheme=scheme, uri_domain=domain)

            connection = boto3.client('s3',
                                      region_name=self.options['s3.region'],
                                      endpoint_url=endpoint_url,
                                      aws_access_key_id=self.options['s3.pub_key'],
                                      aws_secret_access_key=self.options['s3.priv_key'],
                                      use_ssl=True,
                                      verify=self.options['ssl_check'],
                                      config=Config(signature_version=self.options['s3.signature_ver']),
                                     )
            total_bytes = 0
            total_files = 0
            kwargs = {'Bucket': bucket}
            # This loop is needed to obtain all objects as the API can only
            # server 1,000 objects per request. The 'NextMarker' tells where
            # to start the next 1,000. If no 'NextMarker' is received, all
            # objects have been obtained.
            while True:
                try:
                    response = connection.list_objects(**kwargs)
                except botoExceptions.ClientError as ERR:
                    raise UGRStorageStatsConnectionError(
                                                         endpoint=self.id,
                                                         error=ERR.__class__.__name__,
                                                         status_code=ERR.response['ResponseMetadata']['HTTPStatusCode'],
                                                         debug=str(ERR),
                                                        )
                    break
                except botoRequestsExceptions.RequestException as ERR:
                    raise UGRStorageStatsConnectionError(
                                                         endpoint=self.id,
                                                         error=ERR.__class__.__name__,
                                                         status_code="000",
                                                         debug=str(ERR),
                                                        )
                    break
                except botoExceptions.ParamValidationError as ERR:
                    raise UGRStorageStatsConnectionError(
                                                         endpoint=self.id,
                                                         error=ERR.__class__.__name__,
                                                         status_code="000",
                                                         debug=str(ERR),
                                                        )
                    break
                except botoExceptions.BotoCoreError as ERR:
                    raise UGRStorageStatsConnectionError(
                                                         endpoint=self.id,
                                                         error=ERR.__class__.__name__,
                                                         status_code="000",
                                                         debug=str(ERR),
                                                        )
                    break

                else:
                    try:
                        response['Contents']
                    except KeyError:
                        self.stats['bytesused'] = '0'
                        break
                    else:
                        for content in response['Contents']:
                            total_bytes += content['Size']
                            total_files += 1

                    try:
                        kwargs['Marker'] = response['NextMarker']
                    except KeyError:
                        break

            self.stats['bytesused'] = total_bytes

            if self.options['quota'] == 'api':
                self.stats['quota'] = convert_size_to_bytes("1TB")
                self.stats['files'] = total_files
                self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']
                raise UGRStorageStatsQuotaWarning(
                                      endpoint = self.id,
                                      error="NoQuotaGiven",
                                      status_code="000",
                )

            else:
                self.stats['quota'] = self.options['quota']
                self.stats['files'] = total_files
                self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

    def validate_schema(self, scheme):
        if scheme == 's3':
            if self.options['ssl_check']:
                return ('https')
            else:
                return ('http')
        else:
            return (scheme)


class DAVStorageStats(StorageStats):
    """
    Subclass that defines methods for obtaining storage stats of S3 endpoints.
    """
    def __init__(self, *args, **kwargs):
        """
        Extend the object's validators unique to the storage type to make sure
        the storage status check can proceed.
        """
        super(DAVStorageStats, self).__init__(*args, **kwargs)
        self.validators.update({
            'cli_certificate': {
                'required': True,
            },
            'cli_private_key': {
                'required': True,
            },
        })

    def get_storagestats(self):
        u = urlsplit(self.url)
        scheme = self.validate_schema(u.scheme)
        endpoint_url = '{uri_scheme}://{uri.netloc}{uri.path}'.format(uri=u, uri_scheme=scheme)

        headers = {'Depth': '0',}
        data = create_free_space_request_content()
        try:
            response = requests.request(
                method="PROPFIND",
                url=endpoint_url,
                cert=(self.options['cli_certificate'], self.options['cli_private_key']),
                headers=headers,
                verify=self.options['ssl_check'],
                data=data
            )
        except requests.ConnectionError as ERR:
            raise UGRStorageStatsConnectionError(
                                                 endpoint=self.id,
                                                 error=ERR.__class__.__name__,
                                                 status_code="000",
                                                 debug=str(ERR),
                                                )
        except IOError as ERR:
            #We do some regex magic to get the filepath
            certfile = str(ERR).split(":")[-1]
            certfile = certfile.replace(' ','')
            raise UGRStorageStatsConnectionErrorDAVCertPath(
                                                 endpoint=self.id,
                                                 error="ClientCertError",
                                                 status_code="000",
                                                 certfile=certfile,
                                                 debug=str(ERR),
                                                )

        else:
            tree = etree.fromstring(response.content)
            try:
                node = tree.find('.//{DAV:}quota-available-bytes').text
                if node is not None:
                    pass
                else:
                    raise UGRStorageStatsErrorDAVQuotaMethod(endpoint=self.id,
                                                             error="UnsupportedMethod"
                                                            )
            except UGRStorageStatsError as ERR:
                self.stats['bytesused'] = -1
                self.stats['bytesfree'] = -1
                self.stats['quota'] = -1
                self.debug.append(ERR.debug)
                self.status = ERR.message

            else:
                self.stats['bytesused'] = int(tree.find('.//{DAV:}quota-used-bytes').text)
                self.stats['bytesfree'] = int(tree.find('.//{DAV:}quota-available-bytes').text)
                if self.options['quota'] == 'api':
                    # If quota-available-bytes is reported as '0' is because no quota is
                    # provided, so we use the one from the config file or default.
                    if self.stats['bytesfree'] != 0:
                        self.stats['quota'] = self.stats['bytesused'] + self.stats['bytesfree']
                else:
                    self.stats['quota'] = self.options['quota']
    #        except TypeError:
    #            raise MethodNotSupported(name='free', server=hostname)
    #        except etree.XMLSyntaxError:
    #            return str()


###############
## Functions ##
###############

def get_config(config_dir="/etc/ugr/conf.d/"):
    """
    Function that returns a dictionary in which every key represents a
    storage endpoint defined in the ugr configuration files. These files will
    be any *.conf file defined under the config_dir variable.
    The default directory is "/etc/ugr/conf.d/"
    All the glb.locplugin options defined for each are stored as dictionary keys under
    each parent SE key, and the locplugin as keys for the dictionary "options" under
    each parent SE key.
    """
    endpoints = {}
    os.chdir(config_dir)
    for config_file in glob.glob("*.conf"):
        with open(config_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line.startswith("#"):

                    if "glb.locplugin[]" in line:
                        _plugin, _id, _concurrency, _url = line.split(" ")[1::]
                        endpoints.setdefault(_id, {})
                        endpoints[_id].update({'id':_id.strip()})
                        endpoints[_id].update({'url':_url.strip()})
                        endpoints[_id].update({'plugin':_plugin.split("/")[-1]})

                    elif "locplugin" in line:
                        key, _val = line.partition(":")[::2]
                        # Match an _id in key
                        try:
                            if _id in key:
                                _option = key.split(_id+'.')[-1]
                                endpoints.setdefault(_id, {})
                                endpoints[_id].setdefault('options', {})
                                endpoints[_id]['options'].update({_option:_val.strip()})
                            else:
                                raise UGRConfigFileErrorIDMismatch(
                                                                    endpoint=_id,
                                                                    error="OptionIDMismatch",
                                                                    line=line.split(":")[0],
                                                                   )
                        except UGRConfigFileError as ERR:
                            print(ERR.debug)
                            sys.exit(1)
                            # self.debug.append(ERR.debug)
                            # self.status = ERR.message
                    else:
                        # Ignore any other lines
                        #print( "I don't know what to do with %s", line)
                        pass

    return endpoints

def factory(endpoint, plugin):
    """
    Return object class to use based on the plugin specified in the UGR's
    configuration files.
    """
    plugin_dict = {
        'libugrlocplugin_dav.so': DAVStorageStats,
        'libugrlocplugin_http.so': DAVStorageStats,
        'libugrlocplugin_s3.so': S3StorageStats,
        #'libugrlocplugin_azure.so': AzureStorageStats,
        #'libugrlocplugin_davrucio.so': RucioStorageStats,
        #'libugrlocplugin_dmliteclient.so': DMLiteStorageStats,
    }
    if plugin in plugin_dict:
        return plugin_dict.get(plugin)
    else:
        raise UGRUnsupportedPluginError(
                                         endpoint=endpoint,
                                         error="UnsupportedPlugin",
                                         plugin=plugin,
                                        )


def get_endpoints(options):
    """
    Returns list of storage endpoint objects whose class represents each storage
    endpoint configured in UGR's configuration files.
    """
    storage_objects = []
    endpoints = get_config(options.configs_directory)
    for endpoint in endpoints:
        try:
            ep = factory(endpoint, endpoints[endpoint]['plugin'])(endpoints[endpoint])

        except UGRUnsupportedPluginError as ERR:
            ep = StorageStats(endpoints[endpoint])
            ep.debug.append(ERR.debug)
            ep.status = ERR.message


        try:
            ep.validate_options(options)
        except UGRConfigFileError as ERR:
            print(ERR.debug)
            ep.debug.append(ERR.debug)
            ep.status = ERR.message

        storage_objects.append(ep)

    return(storage_objects)

def create_free_space_request_content():
    """Creates an XML for requesting of free space on remote WebDAV server.

    :return: the XML string of request content.
    """
    root = etree.Element("propfind", xmlns="DAV:")
    prop = etree.SubElement(root, "prop")
    etree.SubElement(prop, "quota-available-bytes")
    etree.SubElement(prop, "quota-used-bytes")
    tree = etree.ElementTree(root)
    buff = BytesIO()
    tree.write(buff, xml_declaration=True, encoding='UTF-8')
    return buff.getvalue()

def convert_size_to_bytes(size):
    """
    Converts given sizse into bytes.
    """
    multipliers = {
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

    for suffix in multipliers:
        if size.lower().endswith(suffix):
            return int(size[0:-len(suffix)]) * multipliers[suffix]
    else:
        if size.lower().endswith('b'):
            return int(size[0:-1])

    try:
        return int(size)
    except ValueError: # for example "1024x"
        print('Malformed input!')
        exit()

def warning_on_one_line(message, category, filename, lineno, file=None, line=None):
    #return '%s:%s: %s: %s\n' % (filename, lineno, category.__name__, message)
    return '%s\n' % (message)

#############
# Self-Test #
#############

if __name__ == '__main__':
    # Warning messages are disabled by default.
    if options.debug is False:
        warnings.simplefilter("ignore")
    warnings.formatwarning = warning_on_one_line

    endpoints = get_endpoints(options)
    execbeat_output = []

    for endpoint in endpoints:
        try:
            endpoint.get_storagestats()
        except UGRStorageStatsWarning as WARN:
            endpoint.debug.append(WARN.debug)
            endpoint.status = WARN.message
        except UGRStorageStatsError as ERR:
            endpoint.debug.append(ERR.debug)
            endpoint.status = ERR.message

        # finally: # Here add code to tadd the logs/debug attributes.

        # Upload Storagestats into memcached.
        if options.output_memcached:
            endpoint.upload_to_memcached(options.memcached_ip, options.memcached_port)

        if options.output_execbeat:
            # Fill memcached and execbeat vars
            mc = memcache.Client([options.memcached_ip + ':' + options.memcached_port])
            memcached_index = "Ugrstoragestats_" + endpoint.id
            memcached_contents = endpoint.get_from_memcached(options.memcached_ip, options.memcached_port)
            execbeat_output.append(memcached_contents)


        if options.output_stdout:
            print('\nSE:', endpoint.id, \
                  '\nURL:', endpoint.url, \
                  '\nTime:', endpoint.stats['timestamp'], \
                  '\nQuota:', endpoint.stats['quota'], \
                  '\nBytes Used:', endpoint.stats['bytesused'], \
                  '\nBytes Free:', endpoint.stats['bytesfree'], \
                  '\nStatus:', endpoint.status, \
                 )

        if options.debug:
            print('###########')
            print('Debug:', endpoint.debug)

            if options.output_memcached:
                if memcached_contents is None:
                    memcached_debug = 'Memcached Index [%s]: No Content Found. Possible error connecting to memcached service.' %(memcached_index)
                else:
                    memcached_debug = 'Memcached Index [%s]: %s' %(memcached_index, memcached_contents)
                print(memcached_debug, '\n')

    if options.output_execbeat:
        print ('&&'.join(execbeat_output))
