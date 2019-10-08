"""Defines DAV's StorageShare sub-class."""

import logging

import dynafed_storagestats.base
import dynafed_storagestats.dav.helpers as davhelpers


############
# Classes #
###########

class DAVStorageShare(dynafed_storagestats.base.StorageShare):
    """StorageShare sub-class for DAV.

    Subclass that defines methods and validators for DAV endpoints.

    """

    def __init__(self, *args, **kwargs):
        """Extend StorageShare class attributes."""
        # First we call the super function to initialize the initial attributes
        # given by the StorageShare class.
        super().__init__(*args, **kwargs)

        self.storageprotocol = "DAV"

        self.validators.update({
            'cli_certificate': {
                'required': True,
                'status_code': '003',
            },
            'cli_private_key': {
                'required': True,
                'status_code': '004',
            },
            'storagestats.api': {
                'default': 'rfc4331',
                'required': False,
                'status_code': '070',
                'valid': ['generic', 'list-objects', 'rfc4331'],
            },
        })

        # Invoke the validate_plugin_settings() method
        self.validate_plugin_settings()

        # Invoke the validate_schema() method
        self.validate_schema()

    def get_storagestats(self):
        """Contact endpoint using requested method."""

        if (
            self.plugin_settings['storagestats.api'].lower() == 'generic'
            or self.plugin_settings['storagestats.api'].lower() == 'list-objects'
        ):
            davhelpers.list_files(self)

        elif self.plugin_settings['storagestats.api'].lower() == 'rfc4331':
            davhelpers.rfc4331(self)

    def validate_schema(self):
        """Translate dav/davs into http/https."""
        # Creating logger
        _logger = logging.getLogger(__name__)

        _schema_translator = {
            'dav': 'http',
            'davs': 'https',
        }

        _logger.debug(
            "[%s]Validating URN schema: %s",
            self.id,
            self.uri['scheme']
        )

        if self.uri['scheme'] in _schema_translator:

            _logger.debug(
                "[%s]Using URN schema: %s",
                self.id,
                _schema_translator[self.uri['scheme']]
            )

            self.uri['scheme'] = _schema_translator[self.uri['scheme']]

        else:
            _logger.debug(
                "[%s]Using URN schema: %s",
                self.id,
                self.uri['scheme']
            )
