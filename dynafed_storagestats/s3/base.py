"""Defines S3's StorageShare sub-class."""

import logging
from urllib.parse import urlsplit
import os

import dynafed_storagestats.base
import dynafed_storagestats.s3.helpers as s3helpers

#############
## Classes ##
#############

class S3StorageShare(dynafed_storagestats.base.StorageShare):
    """StorageShare sub-class for S3.

    Subclass that defines methods and validators for S3 endpoints.

    """

    def __init__(self, *args, **kwargs):
        """Extend StorageShare class attributes."""
        # First we call the super function to initialize the initial attributes
        # given by the StorageShare class.
        super().__init__(*args, **kwargs)

        self.storageprotocol = "S3"

        self.validators.update({
            's3.alternate': {
                'default': 'false',
                'required': False,
                'status_code': '020',
                'valid': ['true', 'false', 'yes', 'no']
            },
            'storagestats.api': {
                'default': 'generic',
                'required': False,
                'status_code': '070',
                'valid': ['ceph-admin', 'cloudwatch', 'generic', 'list-objects'],
            },
            's3.priv_key': {
                'required': True,
                'status_code': '021',
            },
            's3.pub_key': {
                'required': True,
                'status_code': '022',
            },
            's3.region': {
                'default': 'us-east-1',
                'required': False,
                'status_code': '023',
            },
            's3.signature_ver': {
                'default': 's3v4',
                'required': False,
                'status_code': '024',
                'valid': ['s3', 's3v4'],
            },
        })

        # Invoke the validate_plugin_settings() method
        self.validate_plugin_settings()

        # Invoke the validate_schema() method
        self.validate_schema()

        # Obtain bucket name
        if self.plugin_settings['s3.alternate'].lower() == 'true'\
        or self.plugin_settings['s3.alternate'].lower() == 'yes':
            self.uri['bucket'] = self.uri['path'].rpartition("/")[-1]

        else:
            self.uri['bucket'], self.uri['domain'] = self.uri['netloc'].partition('.')[::2]

        self.star_fields['storage_share'] = self.uri['bucket']


    def get_object_metadata(self, object_url):
        """Check if the object contains checksum metadata and return it.

        Arguments:
        object_url -- URL location for requested object.

        """
        ############# Creating loggers ################
        _logger = logging.getLogger(__name__)
        ###############################################
        # We obtain the path to the object
        object_path = urlsplit(object_url).path

        # Generate boto client to query S3 endpoint.
        _connection = s3helpers.get_s3_boto_client(self)

        object_key = object_path.split('/')[1::]
        if self.uri['bucket'] in object_key:
            object_key.remove(self.uri['bucket'])
        object_key = '/'.join(object_key)

        _kwargs = {
            'Bucket': self.uri['bucket'],
            'Key': object_key,
        }

        #Ugly way, find better one to deal with empty result.
        ## result = {}

        try:
            _logger.info(
                '[%s]Obtaining metadata of object "%s"',
                self.id,
                object_path
            )

            result = s3helpers.run_boto_client(_connection, 'head_object', _kwargs)

        except dynafed_storagestats.exceptions.Warning as WARN:
            _logger.warning("[%s]%s", self.id, WARN.debug)
            self.debug.append("[WARNING]" + WARN.debug)
            self.status.append("[WARNING]" + WARN.error_code)
            return {}

        except dynafed_storagestats.exceptions.Error as ERR:
            _logger.error("[%s]%s", self.id, ERR.debug)
            self.debug.append("[ERROR]" + ERR.debug)
            self.status.append("[ERROR]" + ERR.error_code)
            return {}

        else:
            _logger.info(
                "[%s]Custom Metadata found for object %s/%s: %s",
                self.id,
                self.uri['bucket'],
                object_key,
                result['Metadata']
            )
            _logger.debug(
                "[%s]Full HEAD response for object %s/%s: %s",
                self.id,
                self.uri['bucket'],
                object_key,
                result
            )

        ##finally:

            try:
                return result['Metadata']

            except KeyError:
                return {}


    def get_storagestats(self):
        """Contact endpoint using requested method."""
        ############# Creating loggers ################

        ###############################################

        # Getting the storage stats CephS3's Admin API
        if self.plugin_settings['storagestats.api'].lower() == 'ceph-admin':
            s3helpers.ceph_admin(self)

        # Getting the storage stats AWS S3 API
        #elif self.plugin_settings['storagestats.api'].lower() == 'aws-cloudwatch':

        # Getting the storage stats using AWS-Boto3 list-objects API, should
        # work for any compatible S3 endpoint.
        elif self.plugin_settings['storagestats.api'].lower() == 'generic' \
        or   self.plugin_settings['storagestats.api'].lower() == 'list-objects':
            s3helpers.list_objects(self)

        # Getting the storage stats using AWS Cloudwatch
        elif self.plugin_settings['storagestats.api'].lower() == 'cloudwatch':
            s3helpers.cloudwatch(self)


    def get_filelist(self, delta=1, prefix='', report_file='/tmp/filelist_report.txt'):
        """Contact endpoint and generate a file-list.

        Generates a list using the prefix var to select specific keys.

        """

        s3helpers.list_objects(
            self,
            delta,
            prefix,
            report_file=report_file,
            request='filelist'
        )


    def set_object_metadata(self, metadata, object_url):
        """Use boto3 copy_object to add checksum metadata to object in S3 storage.

        Arguments:
        object_url: URL location for requested object.

        Returns:
        Nothing is returned.

        """
        ############# Creating loggers ################
        _logger = logging.getLogger(__name__)
        ###############################################

        # We obtain the path to the object
        object_path = urlsplit(object_url).path

        # Generate boto client to query S3 endpoint.
        _connection = s3helpers.get_s3_boto_client(self)

        object_key = object_path.split('/')[1::]
        if self.uri['bucket'] in object_key:
            object_key.remove(self.uri['bucket'])
        object_key = '/'.join(object_key)

        # Preparing _kwargs
        _kwargs = {
            'Bucket': self.uri['bucket'],
            'CopySource': {
                'Bucket': self.uri['bucket'],
                'Key': object_key,
            },
            'Key': object_key,
            'Metadata': metadata,
            'MetadataDirective': 'REPLACE',
        }

        try:
            assert len(metadata) != 0
            # We copy the object on itself to update the metadata.
            _logger.info(
                '[%s]Updating metadata of object "%s"',
                self.id,
                object_path
            )
            _logger.debug(
                '[%s]Metadata being uploaded: "%s"',
                self.id,
                metadata
            )

            result = s3helpers.run_boto_client(_connection, 'copy_object', _kwargs)

        except AssertError as INFO:
            _logger.info("[%s]Empty metadata. Skipping API request.")

        except dynafed_storagestats.exceptions.Warning as WARN:
            _logger.warning("[%s]%s", self.id, WARN.debug)
            self.debug.append("[WARNING]" + WARN.debug)
            self.status.append("[WARNING]" + WARN.error_code)

        except dynafed_storagestats.exceptions.Error as ERR:
            _logger.error("[%s]%s", self.id, ERR.debug)
            self.debug.append("[ERROR]" + ERR.debug)
            self.status.append("[ERROR]" + ERR.error_code)


    def validate_schema(self):
        """Translate s3 into http/https."""
        ############# Creating loggers ################
        _logger = logging.getLogger(__name__)
        ###############################################

        _logger.debug(
            "[%s]Validating URN schema: %s",
            self.id,
            self.uri['scheme']
        )

        if self.uri['scheme'] == 's3':
            if self.plugin_settings['ssl_check']:
                _logger.debug(
                    "[%s]Using URN schema: https",
                    self.id
                )
                self.uri['scheme'] = 'https'

            else:
                _logger.debug(
                    "[%s]Using URN schema: http",
                    self.id
                )
                self.uri['scheme'] = 'http'

        else:
            _logger.debug(
                "[%s]Using URN schema: %s",
                self.id,
                self.uri['scheme']
            )
