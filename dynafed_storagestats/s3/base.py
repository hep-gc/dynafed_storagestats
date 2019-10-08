"""Defines S3's StorageShare sub-class."""

import logging
from urllib.parse import urlsplit
import sys

import dynafed_storagestats.base
import dynafed_storagestats.s3.helpers as s3helpers


###########
# Classes #
###########


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
                'valid': ['ceph-admin', 'cloudwatch', 'generic', 'list-objects', 'minio_prometheus'],
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
        if (
            self.plugin_settings['s3.alternate'].lower() == 'true'
            or self.plugin_settings['s3.alternate'].lower() == 'yes'
        ):
            self.uri['bucket'] = self.uri['path'].rpartition("/")[-1]

        else:
            self.uri['bucket'], self.uri['domain'] = self.uri['netloc'].partition('.')[::2]

        self.star_fields['storage_share'] = self.uri['bucket']

    def get_object_checksum(self, hash_type, object_url):
        """Run process to obtain checksum from object's metadata if it exists.

        Arguments:
        hash_type -- String with type of checksum requested.
        object_url -- String with URL location for requested object.

        Returns:
        String Checksum if found, an exception is raised otherwise.

        """
        # Creating logger
        _logger = logging.getLogger(__name__)

        _logger.info(
            '[%s]Obtaining object metadata: "%s"',
            self.id,
            hash_type
        )

        _metadata = self.get_object_metadata(object_url)

        try:
            _logger.info(
                '[%s]Checking if metadata contains checksum: "%s"',
                self.id,
                hash_type
            )
            _logger.debug(
                '[%s]Metadata being checked: "%s"',
                self.id,
                _metadata
            )

            return _metadata[hash_type]

        except KeyError as ERR:
            raise dynafed_storagestats.exceptions.ChecksumWarningMissingChecksum(
                error='MissingChecksum',
                status_code='999',
                debug=str(ERR),
                storage_share=self
            )

    def get_object_metadata(self, object_url):
        """Check if the object contains checksum metadata and return it.

        Uses boto s3 client method "head_object" to obtain the metadata dict
        of the given object form the provided URL. A connection error will cause
        the process to exit, while a warning or a failure to find any metadata
        will return and empty dict.

        The metadata dictionary keys are returned in lowercase.

        Arguments:
        object_url -- URL location for requested object.

        Returns:
        Dict containing metadata from S3 object API call result.

        """
        # Creating logger
        _logger = logging.getLogger(__name__)

        # We obtain the object's path.
        _object_path = urlsplit(object_url).path

        # We obtain the object's key.
        _object_key = _object_path.split('/')[1::]
        if self.uri['bucket'] in _object_key:
            _object_key.remove(self.uri['bucket'])
        _object_key = '/'.join(_object_key)

        # Generate boto client to query S3 endpoint.
        _connection = s3helpers.get_s3_boto_client(self)

        # Generate Key Arguments needed for the boto client method request.
        _kwargs = {
            'Bucket': self.uri['bucket'],
            'Key': _object_key,
        }

        try:
            _logger.info(
                '[%s]Obtaining metadata of object "%s"',
                self.id,
                _object_path
            )

            _result = s3helpers.run_boto_client(_connection, 'head_object', _kwargs)

        except dynafed_storagestats.exceptions.Warning as WARN:
            _logger.warning("[%s]%s", self.id, WARN.debug)
            self.debug.append("[WARNING]" + WARN.debug)
            self.status.append("[WARNING]" + WARN.error_code)

            return {}

        except dynafed_storagestats.exceptions.Error as ERR:
            if "Not Found" and "HeadObject" in ERR.debug:
                _logger.error(
                    "[%s]%s. Object: %s",
                    self.id,
                    ERR.debug,
                    _object_key
                )

                print(
                    "[ERROR][%s]%s. Object: %s" % (
                        self.id,
                        ERR.debug,
                        _object_key
                    )
                )

            else:
                _logger.error("[%s]%s", self.id, ERR.debug)
                print("[ERROR][%s]%s" % (self.id, ERR.debug))

            # We exit because in this case if there is an error in connection,
            # there is nothing else to do.
            sys.exit(1)

        else:
            _logger.info(
                "[%s]Custom Metadata found for object %s/%s: %s",
                self.id,
                self.uri['bucket'],
                _object_key,
                _result['Metadata']
            )
            _logger.debug(
                "[%s]Full HEAD response for object %s/%s: %s",
                self.id,
                self.uri['bucket'],
                _object_key,
                _result
            )

            try:
                # We set all keys to lowercase.
                _metadata = {k.lower(): v for k, v in _result['Metadata'].items()}
                return _metadata

            except KeyError:
                return {}

    def get_storagestats(self):
        """Contact endpoint using requested method."""

        # Getting the storage stats CephS3's Admin API
        if self.plugin_settings['storagestats.api'].lower() == 'ceph-admin':
            s3helpers.ceph_admin(self)

        # Getting the storage stats AWS S3 API
        # elif self.plugin_settings['storagestats.api'].lower() == 'aws-cloudwatch':

        # Getting the storage stats using AWS-Boto3 list-objects API, should
        # work for any compatible S3 endpoint.
        elif (
            self.plugin_settings['storagestats.api'].lower() == 'generic'
            or self.plugin_settings['storagestats.api'].lower() == 'list-objects'
        ):
            s3helpers.list_objects(self)

        # Getting the storage stats using AWS Cloudwatch
        elif self.plugin_settings['storagestats.api'].lower() == 'cloudwatch':
            s3helpers.cloudwatch(self)

        # Getting the storage stats from Minio's Prometheus URL
        elif self.plugin_settings['storagestats.api'].lower() == 'minio_prometheus':
            s3helpers.minio_prometheus(self)

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

    def put_object_checksum(self, checksum, hash_type, object_url, force=False):
        """Run process to add checksum from object's metadata if it is missing.

        This method will first obtain the metadata (if it exists) using the
        the get_object_metadata() method. The reason for that is not only to
        check if the checksum we are trying to put already exists thus saving
        an extra API call, but also to preserve any existing metadata as the S3
        endpoints don't allow to simply add new keys here. The whole object is
        overwritten with any new metadata we set here so all information needs
        to be aggregated.

        If there is new checksum information to be uploaded then it runs the
        put_object_metadata() method.

        Arguments:
        checksum -- String containing checksum.
        hash_type -- String with type of checksum.
        object_url -- String with URL location for requested object.

        """
        # Creating logger
        _logger = logging.getLogger(__name__)

        _metadata = self.get_object_metadata(object_url)

        # Only run set_checksum if the object don't already contain that hash.
        if hash_type not in _metadata:

            # Add a new key-value to _metdatata dict:
            # hash_type: checksum
            _metadata.setdefault(hash_type, checksum)

            _logger.info(
                "[%s]New metadata detected, calling API to upload: %s",
                self.id,
                _metadata
            )

            self.put_object_metadata(_metadata, object_url)

        # Unless the 'force' flag is given
        elif force:
            # Update checksum key's value.
            _metadata[hash_type] = checksum

            _logger.info(
                "[%s]Force flag detected, calling API to upload: %s",
                self.id,
                _metadata
            )

            self.put_object_metadata(_metadata, object_url)

        else:
            _logger.info(
                "[%s]No new metadata detected, no need to call API.",
                self.id
            )
            _logger.debug(
                "[%s]Metadata: %s",
                self.id,
                _metadata
            )

    def put_object_metadata(self, metadata, object_url):
        """Use boto3 copy_object to add metadata to object in S3 storage.

        Arguments:
        metadata -- Dictionary of data to put..
        object_url -- URL location for requested object.

        """
        # Creating logger
        _logger = logging.getLogger(__name__)

        # We obtain the object's path.
        _object_path = urlsplit(object_url).path

        # We obtain the object's key.
        _object_key = _object_path.split('/')[1::]
        if self.uri['bucket'] in _object_key:
            _object_key.remove(self.uri['bucket'])
        _object_key = '/'.join(_object_key)

        # Generate boto client to query S3 endpoint.
        _connection = s3helpers.get_s3_boto_client(self)

        # Generate Key Arguments needed for the boto client method request.
        # We need copy the object on itself to update the metadata so we use
        # the 'REPLACE' MetadataDirective.
        _kwargs = {
            'Bucket': self.uri['bucket'],
            'CopySource': {
                'Bucket': self.uri['bucket'],
                'Key': _object_key,
            },
            'Key': _object_key,
            'Metadata': metadata,
            'MetadataDirective': 'REPLACE',
        }

        try:
            assert len(metadata) != 0

            _logger.info(
                '[%s]Updating metadata of object "%s"',
                self.id,
                _object_path
            )
            _logger.debug(
                '[%s]Metadata being uploaded: "%s"',
                self.id,
                metadata
            )

            _result = s3helpers.run_boto_client(_connection, 'copy_object', _kwargs)

        except AssertionError as INFO:
            _logger.info(
                "[%s]Empty metadata. Skipping API request. %s",
                self.id,
                INFO
            )

        except dynafed_storagestats.exceptions.Warning as WARN:
            _logger.warning("[%s]%s", self.id, WARN.debug)
            self.debug.append("[WARNING]" + WARN.debug)
            self.status.append("[WARNING]" + WARN.error_code)

        except dynafed_storagestats.exceptions.Error as ERR:
            if "Not Found" and "HeadObject" in ERR.debug:
                _logger.error(
                    "[%s]%s. Object: %s",
                    self.id,
                    ERR.debug,
                    _object_key
                )
                print(
                    "[ERROR][%s]%s. Object: %s" % (
                        self.id,
                        ERR.debug,
                        _object_key
                    )
                )
            else:
                _logger.error("[%s]%s", self.id, ERR.debug)
                print("[ERROR][%s]%s" % (self.id, ERR.debug))

            # We exit because in this case if there is an error in connection,
            # there is nothing else to do.
            sys.exit(1)

    def validate_schema(self):
        """Translate s3 into http/https."""
        # Creating logger
        _logger = logging.getLogger(__name__)

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
