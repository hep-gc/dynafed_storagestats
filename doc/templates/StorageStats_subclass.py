"""
The purpose of this file is to give the layout for a new sublcass of the
StorageStats class, which would be needed when adding support to a new
type of storage endpoint.
It uses the python3 format.
"""

class StorageStatsNewType (StorageStats):
    """
    Define the type of storage endpoint this subclass will interface with
    and any API options it can use.
    """
    def __init__(self, *args, **kwargs):
        """
        Extend or replace any object attributes specific to the type of
        storage endpoint. Below are the most common ones, but add as necessary.
        """
        # First we call the super function to initialize the initial atributes
        # given by the StorageStats class.
        super().__init__(*args, **kwargs)

        # Update the name of the storage protocol. e.g: S3, Azure, DAV.
        self.storageprotocol = "Protocol"

        # Add any validators specific to the storage type so the script can
        # check that all the necessary options are in place in the endpoints.conf
        # files. Define any required, valid and/or default options here using
        # to following format. Note that the only required key is "required".
        self.validators.update({
            'option.name': {
                'default': '', # Default value to use if option is missing.
                'required': True/False, # Wheter this option must be present.
                'valid': ['', ''], # List of valid values to validate against.
            },
        })

        # Invoke the validate_plugin_options() method
        try:
            self.validate_plugin_options()
        except UGRConfigFileError as ERR:
            print(ERR.debug)
            self.debug.append(ERR.debug)
            self.status = ERR.message

        # Add any other attributes needed for this subclass.

        def get_storagestats(self):
            """
            Here goes all the necessary logic to query the storage endpoint
            to obtain the storage stats. Check existing SubClasses for examples.
            Ideally we need to assing values to the following attributes, either
            obtained from the endpoint, from the endpoints.conf file or defaults
            """
            self.bytesfree = 0
            self.bytesused = 0
            self.quota = 0
            # Not required, but is useful for reporting/accounting:
            self.filecount = 0

        def validate_schema(self, scheme):
            """
            This might not be necessary, but if the protocol uses a unique
            protocol schema in the URN that requires some logic to figure out.
            Example below if for the DAVStorageStats.
            """
            schema_translator = {
                'dav': 'http',
                'davs': 'https',
            }

            if scheme in schema_translator:
                return (schema_translator[scheme])
            else:
                return (scheme)
