"""Defines S3's StorageShare sub-class."""

import logging
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
