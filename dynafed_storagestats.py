#!/usr/bin/python3
"""
Module to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints.

Prerequisites:
    Modules:
        - azure-storage >= 0.36.0
        - boto3 >= 1.6.1
        - lxml >= 4.2.1
        - python-memcache >= 1.59
        - requests >= 2.12.5
        - requests_aws4auth >= 0.9
"""
from __future__ import print_function

__version__ = "v0.8.9"

import os
import sys
import time
import uuid
import logging
import collections
from io import BytesIO
from optparse import OptionParser, OptionGroup
import copy
import glob
import json

IS_PYTHON2 = sys.version_info[0] == 2

if IS_PYTHON2:
    from urlparse import urlsplit
else:
    from urllib.parse import urlsplit

try:
    from azure.storage.blob import BlockBlobService, PublicAccess
except ImportError:
    print('ImportError: Please install "azure-storage" modules')
    sys.exit(1)
else:
    import azure.common

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
group.add_option('--json',
                 dest='output_json', action='store_true', default=False,
                 help='Set to output json file with storage stats.'
                )
group.add_option('-o', '--output_dir',
                 dest='output_dir', action='store', default='/tmp',
                 help='Directory to output storage stat files. Defautl: /tmp'
                 )
group.add_option('--plain',
                 dest='output_plain', action='store_true', default=False,
                 help='Set to output stats to plain txt file.'
                )
group.add_option('--stdout',
                 dest='output_stdout', action='store_true', default=False,
                 help='Set to output stats on stdout.'
                )
# parser.add_option('-v', '--verbose',
#                   dest='verbose', action='count',
#                   help='Increase verbosity level for debugging this script (on stderr)'
#                   )
group.add_option('--xml',
                 dest='output_xml', action='store_true', default=False,
                 help='Set to output xml file with StAR format.'
                )
parser.add_option_group(group)

group = OptionGroup(parser, "Logging options")
group.add_option('--logfile',
                dest='logfile', action='store', default='/tmp/dynafed_storagestats.log',
                help='Change where to ouput logs. Default: /tmp/dynafed_storagestats.log'
                )
group.add_option('--loglevel',
                dest='loglevel', action='store', default='WARNING',
                help='Set file log level. Default: WARNING. Valid: DEBUG, INFO, WARNING, ERROR'
                )
parser.add_option_group(group)

options, args = parser.parse_args()


#######################
## Exception Classes ##
#######################

### Log Handler Classes
class TailLogHandler(logging.Handler):
    """
    Logger Handler used in conjuction with the TailLogger class which allows
    to grab log messages genereated by the logging module into variables.
    """
    def __init__(self, log_queue):
        logging.Handler.__init__(self)
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.append(self.format(record))


class TailLogger(object):
    """
    Creates Logger with the TailLogHandler and sets how many lines to keep
    with maxlen.
    """
    def __init__(self, maxlen):
        self._log_queue = collections.deque(maxlen=maxlen)
        self._log_handler = TailLogHandler(self._log_queue)

    def contents(self):
        return '\n'.join(self._log_queue)

    @property
    def log_handler(self):
        return self._log_handler

### Exception Classes
class UGRBaseException(Exception):
    """
    Base exception class for dynafed_storagestats module.
    """
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = "[ERROR] An unkown exception occured processing"
        else:
            self.message = message
        if debug is None:
            self.debug = message
        else:
            self.debug = message + ' ' + debug
        super(UGRBaseException, self).__init__(self.message)

### Defining Error Exception Classes
class UGRBaseError(UGRBaseException):
    """
    Base error exception Subclass which will add the [ERROR] tag to all error
    subclasses.
    """
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = "[ERROR][000] A unkown error occured."
        else:
            self.message = message
        self.debug = debug
        super(UGRBaseError, self).__init__(message=self.message, debug=self.debug)

class UGRConfigFileError(UGRBaseError):
    """
    Base error exception subclass for anything relating to the config file(s).
    """
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = "[ConfigFileError][000] An unkown error occured reading a configuration file."
        else:
            self.message = message
        self.debug = debug
        super(UGRConfigFileError, self).__init__(message=self.message, debug=self.debug)

class UGRUnsupportedPluginError(UGRConfigFileError):
    """
    Exception error when an endpoint of an unsuprted type/protocol plugin
    is detected.
    """
    def __init__(self, error=None, status_code="000", plugin=None, debug=None):
        self.message = '[%s][%s] StorageStats method for "%s" not implemented yet.' \
                       % (error, status_code, plugin)
        self.debug = debug
        super(UGRUnsupportedPluginError, self).__init__(message=self.message, debug=self.debug)

class UGRConfigFileErrorIDMismatch(UGRConfigFileError):
    """
    Exception error when a line in the configuration file under a specific
    endpoint does not match the given endpoint ID. Usually a typo.
    """
    def __init__(self, line, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Failed to match ID in line "%s". Check your configuration.' \
                       % (error, status_code, line)
        self.debug = debug
        super(UGRConfigFileErrorIDMismatch, self).__init__(message=self.message, debug=self.debug)

class UGRConfigFileErrorMissingRequiredSetting(UGRConfigFileError):
    """
    Exception error when an setting required by this module to obtain the Storage
    Stats is missing from the config files for the endpoint being processed.
    """
    def __init__(self, setting, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] "%s" is required. Check your configuration.' \
                  % (error, status_code, setting)
        self.debug = debug
        super(UGRConfigFileErrorMissingRequiredSetting, self).__init__(message=self.message, debug=self.debug)

class UGRConfigFileErrorInvalidSetting(UGRConfigFileError):
    """
    Exception error when the value given for an setting in the configuration file
    does not match the 'valid' values specified in the 'validators' attribute.
    """
    def __init__(self, setting, valid_plugin_settings, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Incorrect value given in setting "%s". Valid plugin_settings: %s' \
                  % (error, status_code, setting, valid_plugin_settings)
        self.debug = debug
        super(UGRConfigFileErrorInvalidSetting, self).__init__(message=self.message, debug=self.debug)

class UGRMemcachedError(UGRBaseError):
    """
    Base error exception subclass for issues deailng with memcached
    communication.
    """
    def __init__(self, message=None, debug=None):
        if message is None:
            self.message = '[MemcachedError][000] Unknown memcached error.'
        else:
            self.message = message
        self.debug = debug
        super(UGRMemcachedError, self).__init__(message=self.message, debug=self.debug)

class UGRMemcachedConnectionError(UGRMemcachedError):
    """
    Exception error when script cannot connect to a memcached instance as
    requested.
    """
    def __init__(self, error=None, status_code="400", debug=None):
        self.message = '[%s][%s] Failed to connect to memcached.' \
                       % (error, status_code)
        self.debug = debug
        super(UGRMemcachedConnectionError, self).__init__(message=self.message, debug=self.debug)

class UGRMemcachedIndexError(UGRMemcachedError):
    """
    Exception error when the requested index in memcached cannot be found.
    """
    def __init__(self, error=None, status_code="404", debug=None):
        self.message = '[%s][%s] Unable to get memcached index contents.' \
                       % (error, status_code)
        self.debug = debug
        super(UGRMemcachedIndexError, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsError(UGRBaseError):
    """
    Base error exception subclass for issues deailng when failing to obtain
    the endpoint's storage stats.
    """
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = "[StorageStatsError][000] An unkown error occured obtaning storage stats."
        else:
            self.message = message
        self.debug = debug
        super(UGRStorageStatsError, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsConnectionError(UGRStorageStatsError):
    """
    Exception error when there is an issue connecting to the endpoint's URN.
    """
    def __init__(self, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Failed to establish a connection.' \
                       % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsConnectionError, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsConnectionErrorInvalidSchema(UGRStorageStatsError):
    """
    Exception error when there is an issue connecting to an S3 endpoint's API.
    """
    def __init__(self, error=None, status_code="000", schema=None, debug=None):
        self.message = '[%s][%s] Invalid schema "%s".' \
                  % (error, status_code, schema)
        self.debug = debug
        super(UGRStorageStatsConnectionErrorInvalidSchema, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsConnectionErrorAzureAPI(UGRStorageStatsError):
    """
    Exception error when there is an issue connecting to an S3 endpoint's API.
    """
    def __init__(self, error=None, status_code="000", api=None, debug=None):
        self.message = '[%s][%s] Error requesting stats using API "%s".' \
                  % (error, status_code, api)
        self.debug = debug
        super(UGRStorageStatsConnectionErrorAzureAPI, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsErrorAzureContainerNotFound(UGRStorageStatsError):
    """
    Exception error when no bucket usage stats could be found.
    """
    def __init__(self, error=None, status_code="000", debug=None, container=''):
        self.message = '[%s][%s] Container tried: %s' \
                  % (error, status_code, container)
        self.debug = debug
        super(UGRStorageStatsErrorAzureContainerNotFound, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsConnectionErrorS3API(UGRStorageStatsError):
    """
    Exception error when there is an issue connecting to an S3 endpoint's API.
    """
    def __init__(self, error=None, status_code="000", api=None, debug=None):
        self.message = '[%s][%s] Error requesting stats using API "%s".' \
                  % (error, status_code, api)
        self.debug = debug
        super(UGRStorageStatsConnectionErrorS3API, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsErrorS3MissingBucketUsage(UGRStorageStatsError):
    """
    Exception error when no bucket usage stats could be found.
    """
    def __init__(self, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Failed to get bucket usage information.' \
                  % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsErrorS3MissingBucketUsage, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsErrorDAVQuotaMethod(UGRStorageStatsError):
    """
    Exception error when the DAV endpoint does not support the RFC 4331 method.
    """
    def __init__(self, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] WebDAV Quota Method.' \
                  % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsErrorDAVQuotaMethod, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsConnectionErrorDAVCertPath(UGRStorageStatsError):
    """
    Exception caused when there is an issue reading a client X509 certificate
    as configured in the config files for the endpoint being processed.
    """
    def __init__(self, error=None, status_code="000", certfile=None, debug=None):
        self.message = '[%s][%s] Invalid client certificate path "%s".' \
                  % (error, status_code, certfile)
        self.debug = debug
        super(UGRStorageStatsConnectionErrorDAVCertPath, self).__init__(message=self.message, debug=self.debug)

### Defining Warning Exception Classes
class UGRBaseWarning(UGRBaseException):
    """
    Base error exception Subclass which will add the [WARN] tag to all warning
    subclasses.
    """
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = '[WARNING][000] A unkown error occured.'
        else:
            self.message = message
        self.debug = debug
        super(UGRBaseWarning, self).__init__(message=self.message, debug=self.debug)

class UGRConfigFileWarning(UGRBaseWarning):
    """
    Base warning exception subclass for anything relating to the config file(s).
    """
    def __init__(self, message=None, error=None, status_code="000", debug=None):
        if message is None:
            # Set some default useful error message
            self.message = '[%s][%s] An unkown error occured reading a configuration file.' \
                           % (error, status_code)
        self.debug = debug
        super(UGRConfigFileWarning, self).__init__(message=self.message, debug=self.debug)

class UGRConfigFileWarningMissingSetting(UGRConfigFileWarning):
    """
    Exception warning when an setting not flaggged as required by this module
    to obtain the storage stats is missing from the config file(s). Prints
    out the default setting given by the 'validators' attribute that will be used
    in this absence.
    """
    def __init__(self, setting, setting_default, error=None, status_code="000", debug=None):
        self.message = '[%s][%s] Unspecified "%s" setting. Using default value "%s"' \
                  % (error, status_code, setting, setting_default)
        self.debug = debug
        super(UGRConfigFileWarningMissingSetting, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsWarning(UGRBaseWarning):
    """
    Base warning exception subclass for issues deailng when non-critical errors
    are detected when trying to obtain the endpoint's storage stats.
    """
    def __init__(self, message=None, debug=None):
        if message is None:
            # Set some default useful error message
            self.message = '[StorageStatsWarning][000] An unkown error occured reading storage stats'
        else:
            self.message = message
        self.debug = debug
        super(UGRStorageStatsWarning, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsQuotaWarning(UGRStorageStatsWarning):
    """
    Exception warning when no quota has been found either from information
    provided by the endpoint's API (as probably excepceted) nor found in the
    config file(s). Prints out the default being used as specified in the
    StorageStats object class's attribute self.stats['quota']
    """
    def __init__(self, error="NoQuotaGiven", status_code="000", debug=None):
        self.message = '[%s][%s] No quota obtained from API or configuration file. Using default of 1TB' \
                       % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsQuotaWarning, self).__init__(message=self.message, debug=self.debug)

class UGRStorageStatsCephS3QuotaDisabledWarning(UGRStorageStatsWarning):
    """
    Exception warning when contacting a Ceph S3 Admin API and it is detected
    that now quota has been enabled for the endpoint bucket.
    """
    def __init__(self, error="BucketQuotaDisabled", status_code="000", debug=None):
        self.message = '[%s][%s] Bucket quota is disabled. Using default of 1TB' \
                  % (error, status_code)
        self.debug = debug
        super(UGRStorageStatsCephS3QuotaDisabledWarning, self).__init__(message=self.message, debug=self.debug)


#####################
## Storage Classes ##
#####################

class StorageStats(object):
    """
    Class that will define how data from UGR's configruation file will be stored
    for earch storage endpoint. As well as how to obtain stats and output it.
    """
    def __init__(self, _ep):
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        self.stats = {
            'bytesused': 0,
            'bytesfree': 0,
            'endtime': 0,
            'filecount': 0,
            'quota': 1000**4,
            'starttime': int(time.time()),
            }

        self.id = _ep['id']
        self.storageprotocol = 'Undefined'
        self.plugin_settings = _ep['plugin_settings']
        # We add the url form the conf file to the plugin_settings as the one
        # in the uri attribute below will be modified depending on the enpoint's
        # protocol.
        self.plugin_settings.update({'url': _ep['url']})
        self.plugin = _ep['plugin']

        _url = urlsplit(_ep['url'])
        self.uri = {
            'hostname': _url.hostname,
            'netloc':   _url.netloc,
            'path':     _url.path,
            'port':     _url.port,
            'scheme':   _url.scheme,
            'url':      _ep['url'],
            }

        self.debug = []
        self.status = '[OK][OK][200]'

        self.validators = {
            'storagestats.quota': {
                'default': 'api',
                'required': False,
            },
            'storagestats.api': {
                'default': 'generic',
                'required': True,
                'valid': ['generic'],
            },
            'ssl_check': {
                'boolean': True,
                'default': True,
                'required': False,
                'valid': ['true', 'false', 'yes', 'no']
            },
        }
        # Initialize StAR fields dict to use in xml output.
        self.star_fields = {
            'storageshare': '',
        }


    def upload_to_memcached(self, memcached_ip='127.0.0.1', memcached_port='11211'):
        """
        Connects to a memcached instance and uploads the endpoints storage stats:
        self.id, self.stats['quota'], self.stats['bytesused']
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        memcached_srv = memcached_ip + ':' + memcached_port
        mc = memcache.Client([memcached_srv])
        memcached_index = "Ugrstoragestats_" + self.id
        storagestats = '%%'.join([
            self.id,
            self.storageprotocol,
            str(self.stats['starttime']),
            str(self.stats['quota']),
            str(self.stats['bytesused']),
            str(self.stats['bytesfree']),
            self.status,
            ])
        flogger.info("[%s]Uploading stats to memcached server: %s" % (self.id, memcached_srv))
        flogger.debug("[%s]Using memcached index: %s" % (self.id, memcached_index))
        flogger.debug("[%s]String uploading to memcached: %s" % (self.id, storagestats))
        try:
            if mc.set(memcached_index, storagestats) == 0:
                raise UGRMemcachedConnectionError(
                    status_code = "400",
                    error="MemcachedConnectionError",
                )

        except UGRMemcachedConnectionError as ERR:
            flogger.error("[%s]%s" % (self.id, ERR.debug))
            mlogger.error("%s" % (ERR.message))
            self.debug.append(ERR.debug)
            self.status = memcached_logline.contents()

    def get_from_memcached(self, memcached_ip='127.0.0.1', memcached_port='11211'):
        """
        Connects to a memcached instance and tries to obtain the storage stats
        from the index belonging to the endpoint making the call. If no index
        is found, the stats are created in the same style as upload_to_memcached
        with error information for debugging and logging
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        mc = memcache.Client([memcached_ip + ':' + memcached_port])
        memcached_index = "Ugrstoragestats_" + self.id
        try:
            memcached_contents = mc.get(memcached_index)
            if memcached_contents is None:
                raise UGRMemcachedIndexError(
                    status_code="000",
                    error='MemcachedEmptyIndex'
                    )

        except UGRMemcachedIndexError as ERR:
            flogger.error("[%s]%s" % (self.id, ERR.debug))
            mlogger.error("%s" % (ERR.message))
            self.debug.append(ERR.debug)
            self.status = memcached_logline.contents()
            memcached_contents = '%%'.join([
                self.id,
                self.storageprotocol,
                str(self.stats['starttime']),
                str(self.stats['quota']),
                str(self.stats['bytesused']),
                str(self.stats['bytesfree']),
                self.status,
                ])
        finally:
            return memcached_contents

    def get_storagestats(self):
        """
        Method for obtaining contacting a storage endpoint and obtain storage
        stats. Will be re-defined for each sub-class as each storage endpoint
        type requires different API's.
        """
        pass

    def validate_plugin_settings(self):
        """
        Check the endpoints plugin_settings from UGR's configuration file against the
        set of default and valid plugin_settings defined under the self.validators dict.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        flogger.info("[%s]Validating configured settings." % (self.id))
        for ep_setting in self.validators:
            flogger.debug("[%s]Validating setting: %s" % (self.id, ep_setting))
            # First check if the setting has been defined in the config file..
            # If it is missing, check if it is required, and exit if true
            # otherwise set it to the default value and print a warning.
            try:
                self.plugin_settings[ep_setting]

            except KeyError:
                try:
                    if self.validators[ep_setting]['required']:
                        self.plugin_settings.update({ep_setting: ''})
                        raise UGRConfigFileErrorMissingRequiredSetting(
                            error="MissingRequiredSetting",
                            setting=ep_setting,
                            )
                    else:
                        raise UGRConfigFileWarningMissingSetting(
                            error="MissingSetting",
                            setting=ep_setting,
                            setting_default=self.validators[ep_setting]['default'],
                            )
                except UGRBaseWarning as WARN:
                    flogger.warn("[%s]%s" % (self.id, WARN.debug))
                    mlogger.warn("%s" % (WARN.message))
                    self.debug.append(WARN.debug)
                    self.status = memcached_logline.contents()
                    self.plugin_settings.update({ep_setting: self.validators[ep_setting]['default']})

            # If the ep_setting has been defined, check against a list of valid
            # plugin_settings (if defined, otherwise contiune). Also transform to boolean
            # form those that have the "boolean" key set as true.
            else:
                try:
                    if self.plugin_settings[ep_setting] not in self.validators[ep_setting]['valid']:
                        raise UGRConfigFileErrorInvalidSetting(
                            error="InvalidSetting",
                            setting=ep_setting,
                            valid_plugin_settings=self.validators[ep_setting]['valid']
                            )
                    else:
                        try:
                            self.validators[ep_setting]['boolean']
                        except KeyError:
                            pass
                        else:
                            if self.plugin_settings[ep_setting].lower() == 'false'\
                            or self.plugin_settings[ep_setting].lower() == 'no':
                                self.plugin_settings.update({ep_setting: False})
                            else:
                                self.plugin_settings.update({ep_setting: True})
                except KeyError:
                    # The 'valid' key is not required to exist.
                    pass
        # If user has specified an SSL CA bundle:
        if self.plugin_settings['ssl_check']:
            try:
                self.plugin_settings['ssl_check'] = self.plugin_settings['ca_path']
            except KeyError:
                # The ssl_check will stay True and standard CA bundle will be used.
                pass

        # Check the quota setting and transform it into bytes if necessary.
        if self.plugin_settings['storagestats.quota'] != "api":
            self.plugin_settings['storagestats.quota'] = convert_size_to_bytes(self.plugin_settings['storagestats.quota'])



    def validate_schema(self):
        """
        Used to validate the URN's schema. SubClasses can have their own.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        flogger.debug("[%s]Validating URN schema: %s" % (self.id, scheme))

    def output_to_stdout(self, options):
        """
        Prints all the storage stats information for each endpont, including
        the last warning/error, and if proper flags set, memcached indices and
        contents and full warning/error debug information from the exceptions.
        """
        mc = memcache.Client([options.memcached_ip + ':' + options.memcached_port])
        memcached_index = "Ugrstoragestats_" + self.id
        memcached_contents = self.get_from_memcached(options.memcached_ip, options.memcached_port)
        if memcached_contents is None:
            memcached_contents = 'No Content Found. Possible error connecting to memcached service.'

        print('\n#####', self.id, '#####' \
              '\n{0:12}{1}'.format('URL:', self.uri['url']), \
              '\n{0:12}{1}'.format('Protocol:', self.storageprotocol), \
              '\n{0:12}{1}'.format('Time:', self.stats['starttime']), \
              '\n{0:12}{1}'.format('Quota:', self.stats['quota']), \
              '\n{0:12}{1}'.format('Bytes Used:', self.stats['bytesused']), \
              '\n{0:12}{1}'.format('Bytes Free:', self.stats['bytesfree']), \
              '\n{0:12}{1}'.format('FileCount:', self.stats['filecount']), \
              '\n{0:12}{1}'.format('Status:', self.status), \
              )
        print('\n{0:12}{1}'.format('Memcached:', memcached_index), \
              '\n{0:12}{1}'.format('Contents:', memcached_contents), \
             )
        if options.debug:
            print('\nDebug:')
            for error in self.debug:
                print('{0:12}{1}'.format(' ', error))

    def create_StAR_xml(self):
        """
        Creates XML object wtih storage stats in the StAR format.
        Heavily based on the star-accounting.py script by Fabrizion Furano
        http://svnweb.cern.ch/world/wsvn/lcgdm/lcg-dm/trunk/scripts/StAR-accounting/star-accounting.py
        """
        SR_namespace = "http://eu-emi.eu/namespaces/2011/02/storagerecord"
        SR = "{%s}" % SR_namespace
        NSMAP = {"sr": SR_namespace}
        xmlroot = etree.Element(SR+"StorageUsageRecords", nsmap=NSMAP)

        # update XML
        rec = etree.SubElement(xmlroot, SR+'StorageUsageRecord')
        rid = etree.SubElement(rec, SR+'RecordIdentity')
        rid.set(SR+"createTime", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time())))

        # StAR StorageShare field (Optional)
        if self.star_fields['storageshare']:
            sshare = etree.SubElement(rec, SR+"StorageShare")
            sshare.text = self.star_fields['storageshare']

        #StAR StorageSystem field (Required)
        if self.uri['hostname']:
            ssys = etree.SubElement(rec, SR+"StorageSystem")
            ssys.text = self.uri['hostname']

        # StAR recordID field (Required)
        recid = self.id+"-"+str(uuid.uuid1())
        rid.set(SR+"recordId", recid)

    #    subjid = etree.SubElement(rec, SR+'SubjectIdentity')

    #    if endpoint.group:
    #      grouproles = endpoint.group.split('/')
    #      # If the last token is Role=... then we fetch the role and add it to the record
    #    tmprl = grouproles[-1]
    #    if tmprl.find('Role=') != -1:
    #      splitroles = tmprl.split('=')
    #      if (len(splitroles) > 1):
    #        role = splitroles[1]
    #        grp = etree.SubElement(subjid, SR+"GroupAttribute" )
    #        grp.set( SR+"attributeType", "role" )
    #        grp.text = role
    #      # Now drop this last token, what remains is the vo identifier
    #      grouproles.pop()
    #
    #    # The voname is the first token
    #    voname = grouproles.pop(0)
    #    grp = etree.SubElement(subjid, SR+"Group")
    #    grp.text = voname
    #
    #    # If there are other tokens, they are a subgroup
    #    if len(grouproles) > 0:
    #      subgrp = '/'.join(grouproles)
    #      grp = etree.SubElement(subjid, SR+"GroupAttribute" )
    #      grp.set( SR+"attributeType", "subgroup" )
    #      grp.text = subgrp
    #
    #    if endpoint.user:
    #      usr = etree.SubElement(subjid, SR+"User")
    #      usr.text = endpoint.user

        # StAR Site field (Optional)
        ## Review
        # if endpoint.site:
        #     st = etree.SubElement(subjid, SR+"Site")
        #     st.text = endpoint.site

        # StAR StorageMedia field (Optional)
        # too many e vars here below, wtf?
        ## Review
        # if endpoint.storagemedia:
        #     e = etree.SubElement(rec, SR+"StorageMedia")
        #     e.text = endpoint.storagemedia

        # StAR StartTime field (Required)
        e = etree.SubElement(rec, SR+"StartTime")
        e.text = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.stats['starttime']))

        # StAR EndTime field (Required)
        e = etree.SubElement(rec, SR+"EndTime")
        e.text = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.stats['endtime']))

        # StAR FileCount field (Optional)
        if self.stats['filecount']:
            e = etree.SubElement(rec, SR+"FileCount")
            e.text = str(self.stats['filecount'])

        # StAR ResourceCapacityUsed (Required)
        e1 = etree.SubElement(rec, SR+"ResourceCapacityUsed")
        e1.text = str(self.stats['bytesused'])

        # StAR ResourceCapacityAllocated (Optional)
        e3 = etree.SubElement(rec, SR+"ResourceCapacityAllocated")
        e3.text = str(self.stats['quota'])

        # if not endpoint.logicalcapacityused:
        #     endpoint.logicalcapacityused = 0
        #
        # e2 = etree.SubElement(rec, SR+"LogicalCapacityUsed")
        # e2.text = str(endpoint.logicalcapacityused)
        return xmlroot



class AzureStorageStats (StorageStats):
    """
    Define the type of storage endpoint this subclass will interface with
    and any API settings it can use.
    """
    def __init__(self, *args, **kwargs):
        """
        Extend or replace any object attributes specific to the type of
        storage endpoint. Below are the most common ones, but add as necessary.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        # First we call the super function to initialize the initial atributes
        # given by the StorageStats class.
        super().__init__(*args, **kwargs)
        self.storageprotocol = "Azure"
        self.validators.update({
            'azure.key': {
                'required': True,
            },
        })

        # Invoke the validate_plugin_settings() method
        try:
            self.validate_plugin_settings()
        except UGRConfigFileError as ERR:
            print(ERR.debug)
            self.debug.append(ERR.debug)
            self.status = ERR.message

        # Invoke the validate_schema() method
        self.validate_schema()

        # Obtain account name and domain from URN
        self.uri['account'], self.uri['domain'] = self.uri['netloc'].partition('.')[::2]
        self.uri['container'] = self.uri['path'].strip('/')

    def get_storagestats(self):
        """
        Uses Azure's API to obtain the storage usage.
        "generic": parses every blob in the account and sums the content-length.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################

        if self.plugin_settings['storagestats.api'].lower() == 'generic':
            total_bytes = 0
            total_files = 0

            block_blob_service = BlockBlobService(account_name=self.uri['account'], account_key=self.plugin_settings['azure.key'])
            container_name = self.uri['container']
            flogger.debug("[%s]Requesting storage stats with: URN: %s API Method: %s Account: %s Container: %s" % (self.id, self.uri['url'], self.plugin_settings['storagestats.api'].lower(), self.uri['account'], self.uri['container']))
            try:
                blobs = block_blob_service.list_blobs(container_name, timeout=10)
            except azure.common.AzureMissingResourceHttpError as ERR:
                raise UGRStorageStatsErrorAzureContainerNotFound(
                    error='ContainerNotFound',
                    status_code=404,
                    debug=str(ERR),
                    container=container_name,
                    )
            except azure.common.AzureHttpError as ERR:
                raise UGRStorageStatsConnectionErrorAzureAPI(
                    error='ConnectionError',
                    status_code=400,
                    debug=str(ERR),
                    api=self.plugin_settings['storagestats.api'],
                    )
            except azure.common.AzureException as ERR:
                raise UGRStorageStatsConnectionError(
                    error='ConnectionError',
                    status_code=400,
                    debug=str(ERR),
                    )
            else:
                for blob in blobs:
                    total_bytes += blob.properties.content_length
                    total_files += 1

                self.stats['bytesused'] = total_bytes
                self.stats['quota'] = self.plugin_settings['storagestats.quota']
                self.stats['bytesfree'] = self.stats['quota'] - total_bytes
                # Not required, but is useful for reporting/accounting:
                self.stats['filecount'] = total_files

class S3StorageStats(StorageStats):
    """
    Subclass that defines methods for obtaining storage stats of S3 endpoints.
    """
    def __init__(self, *args, **kwargs):
        """
        Extend the object's validators unique to the storage type to make sure
        the storage status check can proceed.
        Extend the uri attribute with S3 specific attributes like bucket.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        super(S3StorageStats, self).__init__(*args, **kwargs)
        self.storageprotocol = "S3"
        self.validators.update({
            's3.alternate': {
                'default': 'false',
                'required': False,
                'valid': ['true', 'false', 'yes', 'no']
            },
            'storagestats.api': {
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
                'default': 'us-east-1',
                'required': False,
            },
            's3.signature_ver': {
                'default': 's3v4',
                'required': False,
                'valid': ['s3', 's3v4'],
            },
        })

        # Invoke the validate_plugin_settings() method
        try:
            self.validate_plugin_settings()
        except UGRConfigFileError as ERR:
            flogger.error("[%s]%s" % (self.id, ERR.debug))
            mlogger.error("%s" % (ERR.message))
            print(ERR.debug)
            self.debug.append(ERR.debug)
            self.status = memcached_logline.contents()

        # Invoke the validate_schema() method
        self.validate_schema()

        if self.plugin_settings['s3.alternate'].lower() == 'true'\
        or self.plugin_settings['s3.alternate'].lower() == 'yes':
            self.uri['bucket'] = self.uri['path'].rpartition("/")[-1]

        else:
            self.uri['bucket'], self.uri['domain'] = self.uri['netloc'].partition('.')[::2]

    def get_storagestats(self):
        """
        Connect to the storage endpoint with the defined or generic API's
        to obtain the storage status.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################

        # Getting the storage Stats CephS3's Admin API
        if self.plugin_settings['storagestats.api'].lower() == 'ceph-admin':

            if self.plugin_settings['s3.alternate'].lower() == 'true'\
            or self.plugin_settings['s3.alternate'].lower() == 'yes':
                api_url = '{scheme}://{netloc}/admin/bucket?format=json'.format(scheme=self.uri['scheme'], netloc=self.uri['netloc'])
            else:
                api_url = '{scheme}://{domain}/admin/{bucket}?format=json'.format(scheme=self.uri['scheme'], domain=self.uri['domain'], bucket=self.uri['bucket'])

            payload = {'bucket': self.uri['bucket'], 'stats': 'True'}

            auth = AWS4Auth(
                self.plugin_settings['s3.pub_key'],
                self.plugin_settings['s3.priv_key'],
                self.plugin_settings['s3.region'],
                's3',
                )
            flogger.debug("[%s]Requesting storage stats with: URN: %s API Method: %s Payload: %s" % (self.id, api_url, self.plugin_settings['storagestats.api'].lower(), payload))
            try:
                r = requests.get(
                    url=api_url,
                    params=payload,
                    auth=auth,
                    verify=self.plugin_settings['ssl_check'],
                    )
                # Save time when data was obtained.
                self.stats['endtime'] = int(time.time())

                #Log contents of response
                flogger.debug("[%s]Endpoint reply: %s" % (self.id, r.text))

            except requests.ConnectionError as ERR:
                raise UGRStorageStatsConnectionError(
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
                        error="NoContent",
                        status_code=r.status_code,
                        api=self.plugin_settings['storagestats.api'],
                        debug=r.text,
                        )

                # Make sure we get a Bucket Usage information.
                # Fails on empty (in minio) or newly created buckets.
                try:
                    stats['usage']

                except KeyError as ERR:

                    raise UGRStorageStatsErrorS3MissingBucketUsage(
                        status_code=r.status_code,
                        error=stats['Code'],
                        debug=stats
                        )
                else:
                    if len(stats['usage']) != 0:
                        # If the bucket is emtpy, then just keep going
                        self.stats['bytesused'] = stats['usage']['rgw.main']['size_utilized']
                        self.stats['filecount'] = stats['usage']['rgw.main']['num_objects']

                    if self.plugin_settings['storagestats.quota'] != 'api':
                        self.stats['quota'] = self.plugin_settings['storagestats.quota']
                        self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

                    else:
                        if stats['bucket_quota']['enabled'] is True:
                            self.stats['quota'] = stats['bucket_quota']['max_size']
                            self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

                        elif stats['bucket_quota']['enabled'] is False:
                            self.stats['quota'] = convert_size_to_bytes("1TB")
                            self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']
                            raise UGRStorageStatsCephS3QuotaDisabledWarning()

                        else:
                            self.stats['quota'] = convert_size_to_bytes("1TB")
                            self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']
                            raise UGRStorageStatsQuotaWarning(
                                error="NoQuotaGiven",
                                status_code="000",
                                )

        # Getting the storage Stats AWS S3 API
        #elif self.plugin_settings['storagestats.api'].lower() == 'aws-cloudwatch':

        # Generic list all objects and add sizes using list-objectsv2 AWS-Boto3
        # API, should work for any compatible S3 endpoint.
        elif self.plugin_settings['storagestats.api'].lower() == 'generic':

            if self.plugin_settings['s3.alternate'].lower() == 'true'\
            or self.plugin_settings['s3.alternate'].lower() == 'yes':
                api_url = '{scheme}://{netloc}'.format(scheme=self.uri['scheme'], netloc=self.uri['netloc'])

            else:
                api_url = '{scheme}://{domain}'.format(scheme=self.uri['scheme'], domain=self.uri['domain'])

            connection = boto3.client('s3',
                                      region_name=self.plugin_settings['s3.region'],
                                      endpoint_url=api_url,
                                      aws_access_key_id=self.plugin_settings['s3.pub_key'],
                                      aws_secret_access_key=self.plugin_settings['s3.priv_key'],
                                      use_ssl=True,
                                      verify=self.plugin_settings['ssl_check'],
                                      config=Config(signature_version=self.plugin_settings['s3.signature_ver']),
                                     )
            total_bytes = 0
            total_files = 0
            kwargs = {'Bucket': self.uri['bucket']}
            # This loop is needed to obtain all objects as the API can only
            # server 1,000 objects per request. The 'NextMarker' tells where
            # to start the next 1,000. If no 'NextMarker' is received, all
            # objects have been obtained.
            flogger.debug("[%s]Requesting storage stats with: URN: %s API Method: %s Payload: %s" % (self.id, api_url, self.plugin_settings['storagestats.api'].lower(), kwargs))
            while True:
                try:
                    response = connection.list_objects(**kwargs)
                except botoExceptions.ClientError as ERR:
                    raise UGRStorageStatsConnectionError(
                        error=ERR.__class__.__name__,
                        status_code=ERR.response['ResponseMetadata']['HTTPStatusCode'],
                        debug=str(ERR),
                        )
                except botoRequestsExceptions.RequestException as ERR:
                    raise UGRStorageStatsConnectionError(
                        error=ERR.__class__.__name__,
                        status_code="000",
                        debug=str(ERR),
                        )
                except botoExceptions.ParamValidationError as ERR:
                    raise UGRStorageStatsConnectionError(
                        error=ERR.__class__.__name__,
                        status_code="000",
                        debug=str(ERR),
                        )
                except botoExceptions.BotoCoreError as ERR:
                    raise UGRStorageStatsConnectionError(
                        error=ERR.__class__.__name__,
                        status_code="000",
                        debug=str(ERR),
                        )

                else:
                    # This produces a lot of information, migh not be necessary.
                    flogger.debug("[%s]Endpoint reply: %s" % (self.id, response['Contents']))
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

            # Save time when data was obtained.
            self.stats['endtime'] = int(time.time())

            self.stats['bytesused'] = total_bytes

            if self.plugin_settings['storagestats.quota'] == 'api':
                self.stats['quota'] = convert_size_to_bytes("1TB")
                self.stats['filecount'] = total_files
                self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']
                raise UGRStorageStatsQuotaWarning(
                    error="NoQuotaGiven",
                    status_code="000",
                )

            else:
                self.stats['quota'] = self.plugin_settings['storagestats.quota']
                self.stats['filecount'] = total_files
                self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

    def validate_schema(self):
        """
        Used to translate s3 into http/https since requests doesn't
        support the former schema.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        flogger.debug("[%s]Validating URN schema: %s" % (self.id, self.uri['scheme']))
        if self.uri['scheme'] == 's3':
            if self.plugin_settings['ssl_check']:
                flogger.debug("[%s]Using URN schema: https" % (self.id))
                self.uri['scheme'] = 'https'
            else:
                flogger.debug("[%s]Using URN schema: http" % (self.id))
                self.uri['scheme'] = 'http'
        else:
            flogger.debug("[%s]Using URN schema: %s" % (self.id, self.uri['scheme']))

    def output_StAR_xml(self, output_dir="/tmp"):
        """
        Overriding or setting fields needed for the StAR XML format.
        """
        self.star_fields['storageshare'] = self.uri['bucket']

        super(S3StorageStats, self).output_StAR_xml()


class DAVStorageStats(StorageStats):
    """
    Subclass that defines methods for obtaining storage stats of S3 endpoints.
    """
    def __init__(self, *args, **kwargs):
        """
        Extend the object's validators unique to the storage type to make sure
        the storage status check can proceed.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        super(DAVStorageStats, self).__init__(*args, **kwargs)
        self.storageprotocol = "DAV"
        self.validators.update({
            'cli_certificate': {
                'required': True,
            },
            'cli_private_key': {
                'required': True,
            },
            'storagestats.api': {
                'default': 'rfc4331',
                'required': False,
                'valid': ['generic', 'rfc4331'],
            },
        })

        # Invoke the validate_plugin_settings() method
        try:
            self.validate_plugin_settings()
        except UGRConfigFileError as ERR:
            flogger.error("[%s]%s" % (self.id, ERR.debug))
            mlogger.error("%s" % (ERR.message))
            print(ERR.debug)
            self.debug.append(ERR.debug)
            self.status = memcached_logline.contents()

        # Invoke the validate_schema() method
        self.validate_schema()

    def get_storagestats(self):
        """
        Connect to the storage endpoint and will try WebDAV's quota and bytesfree
        method as defined by RFC 4331 if "api" setting is selected. Or use PROPFIND
        with Depth: Infinity to scan all files and add the contentlegth.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        api_url = '{scheme}://{netloc}{path}'.format(scheme=self.uri['scheme'], netloc=self.uri['netloc'], path=self.uri['path'])
        if self.plugin_settings['storagestats.api'].lower() == 'generic':
            headers = {'Depth': 'infinity',}
            data = ''

        elif self.plugin_settings['storagestats.api'].lower() == 'rfc4331':
            headers = {'Depth': '0',}
            data = create_free_space_request_content()

        # flogger.debug("[%s]Requesting storage stats with:\nURN: %s\nAPI Method: %s\nHeaders: %s\nData: %s" % (self.id, api_url, self.plugin_settings['storagestats.api'].lower(), headers, data ))
        flogger.debug("[%s]Requesting storage stats with: URN: %s API Method: %s Headers: %s Data: %s" % (self.id, api_url, self.plugin_settings['storagestats.api'].lower(), headers, data ))

        try:
            response = requests.request(
                method="PROPFIND",
                url=api_url,
                cert=(self.plugin_settings['cli_certificate'], self.plugin_settings['cli_private_key']),
                headers=headers,
                verify=self.plugin_settings['ssl_check'],
                data=data
            )
            # Save time when data was obtained.
            self.stats['endtime'] = int(time.time())

            #Log contents of response
            flogger.debug("[%s]Endpoint reply: %s" % (self.id, response.text))

        except requests.exceptions.InvalidSchema as ERR:
            raise UGRStorageStatsConnectionErrorInvalidSchema(
                error='InvalidSchema',
                status_code="000",
                schema=self.uri['scheme'],
                debug=str(ERR),
                )
        except requests.ConnectionError as ERR:
            raise UGRStorageStatsConnectionError(
                error=ERR.__class__.__name__,
                status_code="000",
                debug=str(ERR),
                )
        except IOError as ERR:
            #We do some regex magic to get the filepath
            certfile = str(ERR).split(":")[-1]
            certfile = certfile.replace(' ', '')
            raise UGRStorageStatsConnectionErrorDAVCertPath(
                error="ClientCertError",
                status_code="000",
                certfile=certfile,
                debug=str(ERR),
                )

        else:
            # Check that we did not get an erorr code:
            if response.status_code < 400:
                if self.plugin_settings['storagestats.api'].lower() == 'generic':
                    self.stats['bytesused'], self.stats['filecount'] = add_xml_getcontentlength(response.content)
                    self.stats['quota'] = self.plugin_settings['storagestats.quota']
                    self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

                elif self.plugin_settings['storagestats.api'].lower() == 'rfc4331':
                    tree = etree.fromstring(response.content)
                    try:
                        node = tree.find('.//{DAV:}quota-available-bytes').text
                        if node is not None:
                            pass
                        else:
                            raise UGRStorageStatsErrorDAVQuotaMethod(
                                error="UnsupportedMethod"
                                )
                    except UGRStorageStatsError as ERR:
                        flogger.error("[%s]%s" % (self.id, ERR.debug))
                        mlogger.error("%s" % (ERR.message))
                        self.stats['bytesused'] = -1
                        self.stats['bytesfree'] = -1
                        self.stats['quota'] = -1
                        self.debug.append(ERR.debug)
                        self.status = memcached_logline.contents()

                    else:
                        self.stats['bytesused'] = int(tree.find('.//{DAV:}quota-used-bytes').text)
                        self.stats['bytesfree'] = int(tree.find('.//{DAV:}quota-available-bytes').text)
                        if self.plugin_settings['storagestats.quota'] == 'api':
                            # If quota-available-bytes is reported as '0' is because no quota is
                            # provided, so we use the one from the config file or default.
                            if self.stats['bytesfree'] != 0:
                                self.stats['quota'] = self.stats['bytesused'] + self.stats['bytesfree']
                        else:
                            self.stats['quota'] = self.plugin_settings['storagestats.quota']
            #        except TypeError:
            #            raise MethodNotSupported(name='free', server=hostname)
            #        except etree.XMLSyntaxError:
            #            return str()
            else:
                raise UGRStorageStatsConnectionError(
                    error='ConnectionError',
                    status_code=response.status_code,
                    debug=response.text,
                )

    def validate_schema(self):
        """
        Used to translate dav/davs into http/https since requests doesn't
        support the former schema.
        """
        ############# Creating loggers ################
        flogger = logging.getLogger(__name__)
        mlogger = logging.getLogger(__name__+'memcached_logger')
        # memcached_logline = TailLogger(1)
        ###############################################
        schema_translator = {
            'dav': 'http',
            'davs': 'https',
        }

        flogger.debug("[%s]Validating URN schema: %s" % (self.id, self.uri['scheme']))
        if self.uri['scheme'] in schema_translator:
            flogger.debug("[%s]Using URN schema: %s" % (self.id, schema_translator[self.uri['scheme']]))
            self.uri['scheme'] = schema_translator[self.uri['scheme']]
        else:
            flogger.debug("[%s]Using URN schema: %s" % (self.id, self.uri['scheme']))

###############
## Functions ##
###############

def get_config(config_dir="/etc/ugr/conf.d/"):
    """
    Function that returns a dictionary in which every key represents a
    storage endpoint defined in the ugr configuration files. These files will
    be any *.conf file defined under the config_dir variable.
    The default directory is "/etc/ugr/conf.d/"
    All the glb.locplugin settings defined for each are stored as dictionary keys under
    each parent SE key, and the locplugin as keys for the dictionary "plugin_settings" under
    each parent SE key.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################
    endpoints = {}
    global_settings = {}
    os.chdir(config_dir)
    for config_file in sorted(glob.glob("*.conf")):
        flogger.info("Reading file '%s'" % (os.path.realpath(config_file)))
        with open(config_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line.startswith("#"):

                    if "glb.locplugin[]" in line:
                        _plugin, _id, _concurrency, _url = line.split()[1::]
                        endpoints.setdefault(_id, {})
                        endpoints[_id].update({'id':_id.strip()})
                        endpoints[_id].update({'url':_url.strip()})
                        endpoints[_id].update({'plugin':_plugin.split("/")[-1]})
                        flogger.info("Found endpoint '%s' using plugin '%s'. Reading configuration." % (endpoints[_id]['id'], endpoints[_id]['plugin']))

                    elif "locplugin" in line:
                        key, value = line.partition(":")[::2]
                        # Match an _id in key
                        try:
                            if '*' in key:
                            # Add any global settings to its own key.
                                _setting = key.split('*'+'.')[-1]
                                global_settings.update({_setting:value.strip()})
                                flogger.info("Found global setting '%s': %s." %(key, value))
                            elif _id in key:
                                _setting = key.split(_id+'.')[-1]
                                endpoints.setdefault(_id, {})
                                endpoints[_id].setdefault('plugin_settings', {})
                                endpoints[_id]['plugin_settings'].update({_setting:value.strip()})
                            else:
                                raise UGRConfigFileErrorIDMismatch(
                                    error="SettingIDMismatch",
                                    line=line.split(":")[0],
                                    )
                        except UGRConfigFileError as ERR:
                            flogger.error("[%s]%s" % (self.id, ERR.debug))
                            mlogger.error("%s" % (ERR.message))
                            print(ERR.debug)
                            sys.exit(1)
                            # self.debug.append(ERR.debug)
                            # self.status = memcached_logline.contents()
                    else:
                        # Ignore any other lines
                        #print( "I don't know what to do with %s", line)
                        pass
    # If any global settings were found, apply them to any endpoint missing
    # that particular setting. Endpoint specific settings supersede global ones.
    for setting, value in global_settings.items():
        for endpoint in endpoints:
            if setting not in endpoints[endpoint]['plugin_settings']:
                endpoints[endpoint]['plugin_settings'].update({setting:value})
                flogger.debug("[%s]Applying global setting '%s': %s" % (endpoints[endpoint]['id'], setting, value))

    return endpoints

def factory(plugin):
    """
    Return object class to use based on the plugin specified in the UGR's
    configuration files.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################
    plugin_dict = {
        'libugrlocplugin_dav.so': DAVStorageStats,
        'libugrlocplugin_http.so': DAVStorageStats,
        'libugrlocplugin_s3.so': S3StorageStats,
        'libugrlocplugin_azure.so': AzureStorageStats,
        #'libugrlocplugin_davrucio.so': RucioStorageStats,
        #'libugrlocplugin_dmliteclient.so': DMLiteStorageStats,
    }
    if plugin in plugin_dict:
        return plugin_dict.get(plugin)
    else:
        raise UGRUnsupportedPluginError(
            error="UnsupportedPlugin",
            plugin=plugin,
            )


def get_endpoints(config_dir="/etc/ugr/conf.d/"):
    """
    Returns list of storage endpoint objects whose class represents each storage
    endpoint configured in UGR's configuration files.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################
    storage_objects = []
    flogger.info("Looking for storage endpoint configuration files in '%s'" % (config_dir))
    endpoints = get_config(config_dir)
    for endpoint in endpoints:
        flogger.debug("[%s]Requesting object class" % (endpoints[endpoint]['id']))
        try:
            ep = factory(endpoints[endpoint]['plugin'])(endpoints[endpoint])
            flogger.debug("[%s]Object class returned: %s" % (endpoints[endpoint]['id'], type(ep)))
            flogger.debug("[%s]Object.plugin: %s" % (endpoints[endpoint]['id'], endpoints[endpoint]['plugin']))
            flogger.debug("[%s]Object.plugin_settings: %s" % (endpoints[endpoint]['id'], endpoints[endpoint]['plugin_settings']))
        except UGRUnsupportedPluginError as ERR:
            flogger.error("[%s]%s" % (endpoints[endpoint]['id'], ERR.debug))
            mlogger.error("%s" % (ERR.message))
            ep = StorageStats(endpoints[endpoint])
            ep.debug.append(ERR.debug)
            ep.status = memcached_logline.contents()

        storage_objects.append(ep)

    return storage_objects

def create_free_space_request_content():
    """
    Creates an XML for requesting of free space on remote WebDAV server.

    :return: the XML string of request content.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################
    root = etree.Element("propfind", xmlns="DAV:")
    prop = etree.SubElement(root, "prop")
    etree.SubElement(prop, "quota-available-bytes")
    etree.SubElement(prop, "quota-used-bytes")
    tree = etree.ElementTree(root)
    buff = BytesIO()
    tree.write(buff, xml_declaration=True, encoding='UTF-8')
    return buff.getvalue()

def add_xml_getcontentlength(content):
    """
    Iterates and sums through all the "contentlegth sub-elements" returing the
    total byte count.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################
    xml = etree.fromstring(content)
    bytesused = 0
    filescount = 0
    for tags in xml.iter('{DAV:}getcontentlength'):
        if isinstance(tags.text, str):
            bytesused += int(tags.text)
            filescount += 1
    return (bytesused, filescount)

def convert_size_to_bytes(size):
    """
    Converts given sizse into bytes.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################
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
        print('Malformed input for setting: "storagestats.quota"')
        exit()

def output_StAR_xml(endpoints, output_dir="/tmp"):
    """
    Create a single StAR XML file for all endpoints passed to this function.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################
    SR_namespace = "http://eu-emi.eu/namespaces/2011/02/storagerecord"
    SR = "{%s}" % SR_namespace
    NSMAP = {"sr": SR_namespace}
    xmlroot = etree.Element(SR+"StorageUsageRecords", nsmap=NSMAP)

    for endpoint in endpoints:
        data = endpoint.create_StAR_xml()
        root = data.getroottree().getroot()
        sub_element = copy.deepcopy(root[0])
        xmlroot.append(sub_element)

    xml_output = etree.tostring(xmlroot, pretty_print=True, encoding='unicode')
    filename = output_dir + '/' + 'dynafed_storagestats' + '.xml'
    with open(filename, 'w') as output:
        output.write(xml_output)
        output.close()

def output_json(endpoints, output_dir="/tmp"):
    """
    Create a single json file for all endpoints passed to this function.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################

    #Create the json structure in python terms
    dynafed_usedsize = 0
    dynafed_totalsize = 0
    skeleton = {}
    storageservice = {}
    storageendpoints = []

    for endpoint in endpoints:
        dynafed_usedsize += endpoint.stats['bytesused']
        dynafed_totalsize += endpoint.stats['quota']
        storageendpoint = {
            "name": endpoint.id,
            # "id": 'tbd',
            "endpointurl": endpoint.uri['url'],
            "interfacetype": endpoint.storageprotocol,
            "timestamp": endpoint.stats['starttime'],
            "totalsize": endpoint.stats['quota'],
            "usedsize": endpoint.stats['bytesused'],
            "numberoffiles": endpoint.stats['filecount'],
            "path": endpoint.plugin_settings['xlatepfx'].split()[0]
        }
        storageendpoints.append(storageendpoint)

    storageservice = {
        'name': os.uname()[1],
        # 'id': 'tbd',
        # 'servicetype': 'tbd',
        'implementation': 'dynafed',
        # 'implementationversion': 'tbd',
        'latesupdate': int(time.time()),
        "totalsize": dynafed_totalsize,
        "usedsize": dynafed_usedsize,
        'storageendpoints': storageendpoints,
        }
    skeleton = {"storageservice": storageservice}

    filename = output_dir + '/' + 'dynafed_storagestats' + '.json'
    with open(filename, 'w') as output:
        output.write(json.dumps(skeleton, indent=4))
        output.close()

def output_plain(endpoints, output_dir="/tmp"):
    """
    Create a single txt file for all endpoints passed to this function.
    """
    ############# Creating loggers ################
    flogger = logging.getLogger(__name__)
    mlogger = logging.getLogger(__name__+'memcached_logger')
    # memcached_logline = TailLogger(1)
    ###############################################

    # Initialize total tally
    dynafed_usedsize = 0
    dynafed_totalsize = 0

    # Open file handle to write to
    filename = output_dir + '/' + 'dynafed_storagestats' + '.txt'
    with open(filename, 'w') as output:
        output.write("ID URL MountPoint Protocol Timestamp Quota BytesUsed BytesFree FileCount\n")

        for endpoint in endpoints:
            dynafed_usedsize += endpoint.stats['bytesused']
            dynafed_totalsize += endpoint.stats['quota']
            output.write("%s %s %s %s %d %d %d %d %d\n" % (
                endpoint.id,
                endpoint.uri['url'],
                endpoint.plugin_settings['xlatepfx'].split()[0],
                endpoint.storageprotocol,
                endpoint.stats['starttime'],
                endpoint.stats['quota'],
                endpoint.stats['bytesused'],
                endpoint.stats['bytesfree'],
                endpoint.stats['filecount'],
                )
            )
    output.close()

def setup_logger( logfile="/tmp/dynafed_storagestats.log", loglevel="WARNING"):
    """
    Setup the loggers to be used throughout the script. We need at least two,
    one to log onto a logfile and a second with the TailLogger class defined
    above to log onto attribute StorageStats.status which will be logged onto
    memcached.
    """
    ## To capture warnings emitted by modules.
    logging.captureWarnings(True)
    ## create file logger
    flogger = logging.getLogger(__name__)
    num_loglevel = getattr(logging, loglevel.upper())
    flogger.setLevel(num_loglevel)
    # Set file logger format
    log_format_file = logging.Formatter('%(asctime)s - [%(levelname)s]%(message)s')
    # Create file handler and set level from cli or default to settings.log
    log_handler_file = logging.FileHandler(logfile, mode='a')
    log_handler_file.setFormatter(log_format_file)
    # Add handlers
    flogger.addHandler(log_handler_file)

    ## create memcached logger
    # Create TailLogger
    memcached_logline = TailLogger(1) #We just want one line at a time.
    mlogger = logging.getLogger(__name__+'memcached_logger')
    mlogger.setLevel(logging.WARNING)
    # Set memcached logger format
    log_format_memcached = logging.Formatter('[%(levelname)s]%(message)s')

    log_handler_memcached = memcached_logline.log_handler
    log_handler_memcached.setFormatter(log_format_memcached)
    # Add handlers
    mlogger.addHandler(log_handler_memcached)

    return flogger, mlogger, memcached_logline

#############
# Self-Test #
#############

if __name__ == '__main__':

    # Setup loggers
    flogger, mlogger, memcached_logline = setup_logger(
        logfile=options.logfile,
        loglevel=options.loglevel,
        )

    # Create list of StorageStats objects, one for each configured endpoint.
    endpoints = get_endpoints(options.configs_directory)

    # Call get_storagestats method for each endpoint to obtain Storage Stats.
    for endpoint in endpoints:
        flogger.info("[%s] Contacting endpoint." % (endpoint.id))
        try:
            endpoint.get_storagestats()
        except UGRStorageStatsWarning as WARN:
            flogger.warn("[%s]%s" % (endpoint.id, WARN.debug))
            mlogger.warn("%s" % (WARN.message))
            endpoint.debug.append(WARN.debug)
            endpoint.status = memcached_logline.contents()
        except UGRStorageStatsError as ERR:
            flogger.error("[%s]%s" % (endpoint.id, ERR.debug))
            mlogger.error("%s" % (ERR.message))
            endpoint.debug.append(ERR.debug)
            endpoint.status = memcached_logline.contents()

        # finally: # Here add code to tadd the logs/debug attributes.

        # Upload Storagestats into memcached.
        if options.output_memcached:
            endpoint.upload_to_memcached(options.memcached_ip, options.memcached_port)

        # Print Storagestats to the standard output.
        if options.output_stdout:
            endpoint.output_to_stdout(options)

    # Create StAR Storagestats XML files for each endpoint.
    if options.output_xml:
        output_StAR_xml(endpoints, options.output_dir)

    # Create json file with storagestats
    if options.output_json:
        output_json(endpoints, options.output_dir)

    # Create txt file with storagestats
    if options.output_plain:
        output_plain(endpoints, options.output_dir)
