"""Defines Azure's StorageShare sub-class."""

import dynafed_storagestats.base
import dynafed_storagestats.azure.helpers as azurehelpers


############
# Classes #
############

class AzureStorageShare(dynafed_storagestats.base.StorageShare):
    """StorageShare sub-class for Azure.

    Subclass that defines methods and validators for Azure endpoints.

    """

    def __init__(self, *args, **kwargs):
        """Extend StorageShare class attributes."""
        # First we call the super function to initialize the initial attributes
        # given by the StorageShare class.
        super().__init__(*args, **kwargs)

        self.storageprotocol = "Azure"

        self.validators.update({
            'azure.key': {
                'required': True,
                'status_code': '010',
            },
            'storagestats.api': {
                'default': 'generic',
                'required': False,
                'status_code': '070',
                'valid': ['generic', 'list-blobs', 'metrics'],
            },
        })

        # Invoke the validate_plugin_settings() method
        self.validate_plugin_settings()

        # Invoke the validate_schema() method
        self.validate_schema()

        # Obtain account name and domain from URN
        self.uri['account'], self.uri['domain'] = self.uri['netloc'].partition('.')[::2]
        self.uri['container'] = self.uri['path'].strip('/')

    def get_storagestats(self):
        """Contact endpoint using requested method."""

        if (
            self.plugin_settings['storagestats.api'].lower() == 'generic'
            or self.plugin_settings['storagestats.api'].lower() == 'list-blobs'
        ):
            azurehelpers.list_blobs(self)

    def get_filelist(self, delta=1, prefix='', report_file='/tmp/filelist_report.txt'):
        """Contact endpoint and generate a file-list.

        Generates a list using the prefix var to select specific keys.

        """

        azurehelpers.list_blobs(
            self,
            delta,
            prefix,
            report_file=report_file,
            request='filelist'
        )
