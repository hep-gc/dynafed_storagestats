"""Define the base Classes StorageEndpoint and StorageShares."""

import datetime
import logging

from urllib.parse import urlsplit

import dynafed_storagestats.helpers
import dynafed_storagestats.exceptions


############
# Classes ##
############

class StorageEndpoint():
    """Base class representing unique URL storage endpoint.

    Will be completed with one or several StorageShare objects that share the
    same URL, as a list in self.storage_shares.

    """

    def __init__(self, url):
        """Create storage_shares and url attributes.

        Arguments:
        url -- string.

        """
        self.interface_type = 'https'
        self.storage_shares = []
        self.url = url


    def add_storage_share(self, storage_share):
        """Add StorageShare object to self.storage_share list.

        Arguments:
        storage_share  -- dynafed_storagestats StorageShare object.

        """
        self.storage_shares.append(storage_share)


class StorageShare():
    """Base class representing unique storage share.

    Contains all the settings and general StorageShare methods. It is meant to
    be extended with protocol specific methods and settings.

    """

    def __init__(self, storage_share):
        """Create attributes from UGR's endpoint settings and defaults.

        Arguments:
        storage_share  -- dict containing single storage share and its settings.
                          Produced by configloader.parse_conf_files().

        """
        # ID used in UGR's configuration file to define the StorageShare/endpoint.
        self.id = storage_share['id']

        # All settings obtained from UGR's configuration file.
        # Any missing settings will be added with specified defaults.
        self.plugin_settings = storage_share['plugin_settings']

        # We add the url form the conf file to the plugin_settings as the one
        # in the uri attribute below will be modified depending on the
        # StorageShare/endpoint protocol.
        self.plugin_settings.update({'url': storage_share['url']})

        # URL split needed to simplify the formation of the form needed to make
        # protocol specific API requests.
        _url = urlsplit(storage_share['url'])
        self.uri = {
            'hostname': _url.hostname,
            'netloc':   _url.netloc,
            'path':     _url.path,
            'port':     _url.port,
            'scheme':   _url.scheme,
            'url':      storage_share['url'],
        }

        # UGR's plugin used to communicate withe  StorageShare/endpoint.
        self.plugin = storage_share['plugin']

        # Attributes that will hold error/warning status messages.
        self.debug = []
        self.status = []

        # Initialize StAR fields dict to use in xml output.
        self.star_fields = {
            'storageshare': '',
        }

        # Protocol used to communicate with the StorageShare/endpoint.
        self.storageprotocol = 'Undefined'

        # Initialize stat variables. -1 will represent a failure or missing stat.
        self.stats = {
            'bytesused': -1,
            'bytesfree': -1,
            'endtime': 0,
            'filecount': -1,
            'quota': 1000**4,
            'starttime': int(datetime.datetime.now().timestamp()),
            'check': True, # To flag whether this endpoint should be contacted.
        }

        # Setting validators used across all SubClasses.
        self.validators = {
            'conn_timeout': {
                'default': 10,
                'required': False,
                'status_code': '005',
                'type': 'int',
            },
            'storagestats.api': {
                'default': 'generic',
                'required': False,
                'status_code': '070',
                'valid': ['generic'],
            },
            'storagestats.frequency': {
                'default': '600',
                'required': False,
                'status_code': '072'
            },
            'storagestats.quota': {
                'default': 'api',
                'required': False,
                'status_code': '071',
            },
            'ssl_check': {
                'boolean': True,
                'default': True,
                'required': False,
                'status_code': '006',
                'valid': ['true', 'false', 'yes', 'no']
            },
        }


    def get_storagestats(self):
        """Contact a storage endpoint and obtain storage stats.

        Method(s) for each protocol are added via their respective sub-classes
        to deal with the different API's.

        """
        pass


    def validate_plugin_settings(self):
        """Validate the StorageShare plugin_settings against self.validators.

        Handles exceptions arising from missing/incorrect settings and logging.

        """
        # Creating logger
        _logger = logging.getLogger(__name__)


        _logger.info("[%s]Validating configured settings.", self.id)

        for _setting in self.validators:
            _logger.debug(
                "[%s]Validating setting: %s",
                self.id,
                _setting
            )

            # First check if the _setting has been defined in the config file..
            # If it is missing, check if it is required to be defined, and exit
            # if true, otherwise set it to the default value and print a warning.
            try:
                self.plugin_settings[_setting]

            except KeyError:
                try:
                    if self.validators[_setting]['required']:
                        raise dynafed_storagestats.exceptions.ConfigFileErrorMissingRequiredSetting(
                            error="MissingRequiredSetting",
                            setting=_setting,
                            status_code=self.validators[_setting]['status_code'],
                        )

                    else:
                        raise dynafed_storagestats.exceptions.ConfigFileWarningMissingSetting(
                            error="MissingSetting",
                            setting=_setting,
                            setting_default=self.validators[_setting]['default'],
                            status_code=self.validators[_setting]['status_code'],
                        )

                except dynafed_storagestats.exceptions.ConfigFileErrorMissingRequiredSetting as ERR:
                    # Mark StorageShare/endpoint to be skipped with a reason.
                    self.stats['check'] = 'MissingRequiredSetting'
                    self.plugin_settings.update({_setting: ''})

                    _logger.error("[%s]%s", self.id, ERR.debug)
                    self.debug.append("[ERROR]" + ERR.debug)
                    self.status.append("[ERROR]" + ERR.error_code)

                except dynafed_storagestats.exceptions.ConfigFileWarningMissingSetting as WARN:
                    # Set the default value for this setting.
                    self.plugin_settings.update({_setting: self.validators[_setting]['default']})

                    _logger.warning("[%s]%s", self.id, WARN.debug)
                    self.debug.append("[WARNING]" + WARN.debug)
                    self.status.append("[WARNING]" + WARN.error_code)

            # If the _setting has been defined, check against a list of valid
            # plugin_settings if defined. Also, typecast to boolean form those
            # that have the "boolean" key set as true.
            else:
                try:
                    if self.plugin_settings[_setting] not in self.validators[_setting]['valid']:
                        # Mark StorageShare/endpoint to be skipped with a reason.
                        self.stats['check'] = 'InvalidSetting'
                        raise dynafed_storagestats.exceptions.ConfigFileErrorInvalidSetting(
                            error="InvalidSetting",
                            setting=_setting,
                            status_code=self.validators[_setting]['status_code'],
                            valid_plugin_settings=self.validators[_setting]['valid']
                        )

                    else:
                        try:
                            self.validators[_setting]['boolean']

                        except KeyError:
                            pass

                        else:
                            if self.plugin_settings[_setting].lower() == 'false'\
                            or self.plugin_settings[_setting].lower() == 'no':
                                self.plugin_settings.update({_setting: False})
                            else:
                                self.plugin_settings.update({_setting: True})

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
            self.plugin_settings['storagestats.quota'] = dynafed_storagestats.helpers.convert_size_to_bytes(self.plugin_settings['storagestats.quota'])


    def validate_schema(self):
        """Validate the URN's protocol schema.

        Method for each protocol is added via their respective sub-classes
        to deal with the different schema's to be checked.

        """
        # Creating logger
        _logger = logging.getLogger(__name__)


        _logger.debug(
            "[%s]Validating URN schema: %s",
            self.id,
            self.uri['scheme']
        )
