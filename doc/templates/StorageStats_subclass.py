"""
The purpose of this file is to give the layout for a new sublcass of the
StorageStats class, which would be needed when adding support to a new
type of storage endpoint.
It uses the python3 format.
"""

class NewTypeStorageStats (StorageStats):
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
        mlogger = logging.getLogger('memcached_logger')
        ###############################################
        # First we call the super function to initialize the initial atributes
        # given by the StorageStats class.
        super().__init__(*args, **kwargs)

        # Update the name of the storage protocol. e.g: S3, Azure, DAV.
        self.storageprotocol = "Protocol"

        # Add any validators specific to the storage type so the script can
        # check that all the necessary settings are in place in the endpoints.conf
        # files. Define any required, valid and/or default settings here using
        # to following format. Note that the only required key is "required".
        self.validators.update({
            'setting.name': {
                'default': '', # Default value to use if setting is missing.
                'required': True/False, # Wheter this setting must be present.
                'valid': ['', ''], # List of valid values to validate against.
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

        # Add any other attributes needed for this subclass.

        def get_storagestats(self):
            """
            Here goes all the necessary logic to query the storage endpoint
            to obtain the storage stats. Check existing SubClasses for examples.
            Ideally we need to assing values to the following attributes, either
            obtained from the endpoint, from the endpoints.conf file or defaults
            """
            ############# Creating loggers ################
            flogger = logging.getLogger(__name__)
            mlogger = logging.getLogger('memcached_logger')
            ###############################################
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
            ############# Creating loggers ################
            flogger = logging.getLogger(__name__)
            mlogger = logging.getLogger('memcached_logger')
            ###############################################

            schema_translator = {
                'dav': 'http',
                'davs': 'https',
            }

            flogger.debug("[%s]Validating URN schema: %s" % (self.id, scheme))
            if scheme in schema_translator:
                flogger.debug("[%s]Using URN schema: %s" % (self.id, schema_translator[scheme]))
                return schema_translator[scheme]
            else:
                flogger.debug("[%s]Using URN schema: %s" % (self.id, scheme))
                return scheme
