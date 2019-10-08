"""Exceptions classes."""


############
# Classes ##
############

class BaseException(Exception):
    """
    Base exception class for dynafed_storagestats module. Formats the message
    and debug attributes with the variables passed from the SubClasses.
    """
    def __init__(self, error="ERROR", status_code="000", message=None, debug=None):

        self.error_code = "[%s][%s]" % (error, status_code)

        if message is None:
            # Set some default useful error message
            self.message = self.error_code + "An unknown exception occurred processing"
        else:
            self.message = self.error_code + message

        if debug is None:
            self.debug = self.message
        else:
            self.debug = self.message + debug

        super().__init__(self.message)


# Defining Error Exception Classes

class BaseError(BaseException):
    """
    Base error exception Subclass.
    """
    def __init__(self, error="ERROR", status_code="000", message=None, debug=None):

        if message is None:
            # Set some default useful error message
            self.message = "A unknown error occurred."
        else:
            self.message = message
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ChecksumError(BaseError):
    """
    Base error exception subclass for anything relating to the file/object checksums.
    """
    def __init__(self, error="ChecksumError", status_code="001", message=None, debug=None):

        if message is None:
            # Set some default useful error message
            self.message = "An unknown error occurred dealing with file/object checksum."
        else:
            self.message = message
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ChecksumErrorMissingEndpoint(ChecksumError):
    """
    Exception error when the endpoint ID is missing form the request.
    """
    def __init__(self, storage_share, error="ChecksumErrorMissingEndpoint", status_code="00?", debug=None):

        self.storage_share = storage_share

        self.message = 'Missing endpoint ID. Check your request.'

        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ChecksumErrorMissingArgument(ChecksumError):
    """
    Exception error when required arguments are missing.
    """
    def __init__(self, storage_share, error="ChecksumErrorMissingEndpoint", status_code="00?", debug=None):

        self.storage_share = storage_share

        self.message = 'Missing required argument.'

        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConfigFileError(BaseError):
    """
    Base error exception subclass for anything relating to the config file(s).
    """
    def __init__(self, error="ConfigFileError", status_code="001", message=None, debug=None):

        if message is None:
            # Set some default useful error message
            self.message = "An unknown error occurred reading a configuration file."
        else:
            self.message = message
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConfigFileErrorIDMismatch(ConfigFileError):
    """
    Exception error when a line in the configuration file under a specific
    endpoint does not match the given endpoint ID. Usually a typo.
    """
    def __init__(self, storage_share, config_file, line_number, line, error="SettingIDMismatch", status_code="002", debug=None):

        self.storage_share = storage_share

        self.message = 'Failed to match ID file: "%s" @ line: [%i] "%s". Check your configuration.' \
                       % (config_file, line_number, line)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConfigFileErrorInvalidSetting(ConfigFileError):
    """
    Exception error when the value given for an setting in the configuration file
    does not match the 'valid' values specified in the 'validators' attribute.
    """
    def __init__(self, setting, valid_plugin_settings, error="InvalidSetting", status_code="001", debug=None):

        self.message = 'Incorrect value given in setting "%s". Valid plugin_settings: %s' \
                       % (setting, valid_plugin_settings)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConfigFileErrorMissingRequiredSetting(ConfigFileError):
    """
    Exception error when a setting required by this module to obtain the Storage
    Stats is missing from the config files for the endpoint being processed.
    """
    def __init__(self, setting, error="MissingRequiredSetting", status_code="001", debug=None):

        self.message = '"%s" is required. Check your configuration.' \
                       % (setting)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConfigFileErrorNoConfigFilesFound(ConfigFileError):
    """
    Exception error when no configuration files have been found.
    """
    def __init__(self, config_path, error="NoConfigFilesFound", status_code="002", debug=None):

        self.message = 'No configuration files found in the path(s): %s' \
                       % (config_path)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConfigFileErrorNoEndpointsFound(ConfigFileError):
    """
    Exception error when no endpoints are found in the configuration files.
    """
    def __init__(self, config_path, error="NoEndpointsFound", status_code="002", debug=None):

        self.message = 'No endpoints found in configuration file(s): %s' \
                       % (config_path)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class MemcachedError(BaseError):
    """
    Base error exception subclass for issues deailng with memcached
    communication.
    """
    def __init__(self, error="MemcachedError", status_code="080", message=None, debug=None):

        if message is None:
            self.message = 'Unknown memcached error.'
        else:
            self.message = message
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class MemcachedConnectionError(MemcachedError):
    """
    Exception error when script cannot connect to a memcached instance as
    requested.
    """
    def __init__(self, error="MemcachedConnectionError", status_code="081", debug=None):

        self.message = 'Failed to connect to memcached.'
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class MemcachedIndexError(MemcachedError):
    """
    Exception error when the requested index in memcached cannot be found.
    """
    def __init__(self, error="MemcachedIndexError", status_code="082", debug=None):

        self.message = 'Unable to get memcached index contents.'
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class Error(BaseError):
    """
    Base error exception subclass for issues dealing when failing to obtain
    the endpoint's storage stats.
    """
    def __init__(self, error="StorageStatsError", status_code="090", message=None, debug=None):

        if message is None:
            # Set some default useful error message
            self.message = "An unknown error occurred while obtaining storage stats."
        else:
            self.message = message
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConnectionError(Error):
    """
    Exception error when there is an issue connecting to the endpoint's URN.
    """
    def __init__(self, error="ConnectionError", status_code="400", debug=None):

        self.message = 'Failed to establish a connection.'
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConnectionErrorAzureAPI(Error):
    """
    Exception error when there is an issue connecting to Azure's API.
    """
    def __init__(self, error="ConnectionError", status_code="400", api=None, debug=None):

        self.message = 'Error requesting stats using API "%s".' \
                       % (api)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConnectionErrorDAVCertPath(Error):
    """
    Exception caused when there is an issue reading a client X509 certificate
    as configured in the config files for the endpoint being processed.
    """
    def __init__(self, error="ClientCertError", status_code="091", certfile=None, debug=None):

        self.message = 'Invalid client certificate path "%s".' \
                       % (certfile)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConnectionErrorInvalidSchema(Error):
    """
    Exception error when the URN's schema does not match any valid options.
    """
    def __init__(self, error="InvalidSchema", status_code="008", schema=None, debug=None):

        self.message = 'Invalid schema "%s".' \
                       % (schema)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConnectionErrorS3API(Error):
    """
    Exception error when there is an issue connecting to an S3 API.
    """
    def __init__(self, error="ConnectionError", status_code="400", api=None, debug=None):

        self.message = 'Error requesting stats using API "%s".' \
                       % (api)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ErrorAzureContainerNotFound(Error):
    """
    Exception error when the Azure container requested could not be found.
    """
    def __init__(self, error="ContainerNotFound", status_code="404", debug=None, container=''):

        self.message = 'Container tried: %s' \
                       % (container)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ErrorDAVQuotaMethod(Error):
    """
    Exception error when the DAV endpoint does not support the RFC 4331 method.
    """
    def __init__(self, error="UnsupportedMethod", status_code="096", debug=None):

        self.message = 'WebDAV Quota Method.'
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ErrorS3MissingBucketUsage(Error):
    """
    Exception error when no bucket usage stats were returned.
    """
    def __init__(self, error="MissingBucketUsage", status_code="098", debug=None):

        self.message = '[%s][%s] Failed to get bucket usage information.'
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class OfflineEndpointError(Error):
    """
    Exception error when and endpoint is detected to have been flagged as offline.
    """
    def __init__(self, error="EndpointOffline", status_code="503", debug=None):

        self.message = 'Dynafed has flagged this endpoint as offline.'
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class UnsupportedPluginError(ConfigFileError):
    """
    Exception error when an endpoint of an unsupported type/protocol plugin
    is detected.
    """
    def __init__(self, error="UnsupportedPlugin", status_code="009", plugin=None, debug=None):

        self.message = 'StorageStats method for "%s" not implemented yet.' \
                       % (plugin)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


# Defining Warning Exception Classes

class BaseWarning(BaseException):
    """
    Base error exception Subclass.
    """
    def __init__(self, error="WARNING", status_code="000", message=None, debug=None):

        if message is None:
            # Set some default useful error message
            self.message = 'A unknown warning occurred.'
        else:
            self.message = message
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ChecksumWarning(BaseWarning):
    """
    Base error exception subclass for anything relating to the file/object checksums.
    """
    def __init__(self, error="ChecksumError", status_code="001", message=None, debug=None):

        if message is None:
            # Set some default useful error message
            self.message = "An unknown error occurred dealing with file/object checksum."
        else:
            self.message = message
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ChecksumWarningMissingChecksum(ChecksumWarning):
    """
    Exception error when the endpoint ID is missing form the request.
    """
    def __init__(self, storage_share, error="ChecksumWarningMissingChecksum", status_code="00?", debug=None):

        self.storage_share = storage_share

        self.message = 'No checksum found.'

        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConfigFileWarning(BaseWarning):
    """
    Base warning exception subclass for anything relating to the config file(s).
    """
    def __init__(self, error="ConfigFileWarning", status_code="001", message=None, debug=None):

        if message is None:
            # Set some default useful error message
            self.message = 'An unknown warning occurred reading a configuration file.'
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class ConfigFileWarningMissingSetting(ConfigFileWarning):
    """
    Exception warning when an setting not flagged as required by this module
    to obtain the storage stats is missing from the config file(s). Prints
    out the default setting given by the 'validators' attribute that will be used
    in this absence.
    """
    def __init__(self, setting, setting_default, error="MissingSetting", status_code="001", debug=None):

        self.message = 'Unspecified "%s" setting. Using default value "%s"' \
                       % (setting, setting_default)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class Warning(BaseWarning):
    """
    Base warning exception subclass for issues dealing when non-critical errors
    are detected when trying to obtain the endpoint's storage stats.
    """
    def __init__(self, error="StorageStatsWarning", status_code="090", message=None, debug=None):

        if message is None:
            # Set some default useful error message
            self.message = 'An unknown error occurred reading storage stats'
        else:
            self.message = message
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class QuotaWarning(Warning):
    """
    Exception warning when no quota has been provided by the endpoint's API
    when requested. Prints out the default being used as specified in the
    StorageStats object class's attribute self.stats['quota']
    """
    def __init__(self, error="NoQuotaGiven", status_code="098", debug=None, default_quota="1TB"):

        self.message = 'No quota obtained from API or configuration file. Using default of %s' \
                       % (default_quota)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class CephS3QuotaDisabledWarning(Warning):
    """
    Exception warning when contacting a Ceph S3 Admin API and it is detected
    that no quota has been enabled for the bucket.
    """
    def __init__(self, error="BucketQuotaDisabled", status_code="099", debug=None, default_quota="1TB"):

        self.message = 'Bucket quota is disabled. Using default of %s' \
                       % (default_quota)
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)


class DAVZeroQuotaWarning(Warning):
    """
    Exception warning when a DAV based endpoint utilizing RFC4331 returns
    quota-available-bytes as 0. It well could mean there is no more space
    in the endpoint, or, more likely, the quota is not properly configured.
    We raise a warning to let the operator know.
    """
    def __init__(self, error="ZeroAvailableBytes", status_code="098", debug=None):

        self.message = 'RFC4331 reports quota-available-bytes as "0". '       \
                       'While the endpoint could be full, this could also '   \
                       'indicate an issue with the back-end configuration or '\
                       'lack of support returning this information. '         \
                       'If necessary input a quota manually in the configuration file.'
        self.debug = debug

        super().__init__(error=error, status_code=status_code, message=self.message, debug=self.debug)
