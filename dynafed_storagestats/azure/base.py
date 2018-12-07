"""
Module intended to hold SubClasses, methods and functions to deal with storage
shares in Azure.
"""

import dynafed_storagestats.base
import dynafed_storagestats.azure.helpers as azurehelpers

#############
## Classes ##
#############

class AzureStorageShare(dynafed_storagestats.base.StorageShare):
    """
    Subclass that defines methods for obtaining storage stats of Azure endpoints.
    """
    def __init__(self, *args, **kwargs):

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
        """
        Uses Azure's API to obtain the storage usage.
        "generic": parses every blob in the account and sums the content-length.
        """
        ############# Creating loggers ################

        ###############################################

        if self.plugin_settings['storagestats.api'].lower() == 'generic' \
        or self.plugin_settings['storagestats.api'].lower() == 'list-blobs':
            azurehelpers.list_blobs(self)
