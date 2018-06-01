#!/usr/bin/python
"""
Module to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints.

Prerequisites:
    Modules:
    - lxml
    - memcache
    - requests
    - requests_aws4auth

v0.0.1 Works with cephS3 AdminAPI.
v0.0.2 Added AWS list-type2 API to list all objects in bucket and add object size.
       Added memcached format and upload each endpoint obtained.
v0.0.3 Added support for python3
v0.0.4 Changed from single configration file to all *.conf files in given directory.
v0.0.5 Added module import checks.
v0.0.6 StorageStats object class chosen dynamically based on configured plugin.
v0.0.7 Added options
v0.1.0 Changed aws-list to generic and now uses boto3 for generality.
v0.2.0 Added validators key and 'validate_ep_options' function.
v0.2.1 Cleaned up code to PEP8.
v0.2.2 Exception for plugint types not yet implemented.
v0.2.3 Fixed bucket-name issue if not at paths' root and non-standard ports for
        S3 endpoints.
v0.2.4 Added URL schema validator function and updated code. Works for dav,davs.
v0.2.5 Moved schema validator from fuction to class method. Added for S3.
v0.2.6 Tested with Minio
v0.2.7 Added files counted to S3 Generic.
v0.2.8 Changed S3 generic API from list_objects_v2 to list_objects as CephS3
       doesn't have the "NextContinuationToken" directive and thus would only
       list the first 1000. This needs to be updated one Ceph has this as
       v1 is sort of deprecated.
v0.2.9 Added ability to specify S3 signature version.
v0.2.10 Added options for memcached, stdoutput and some debugging.
v0.2.11 Fixed issue with ID names with multiple "."
v0.3.0 Added DAV/Http support.
v0.3.1 Added exceptions and logic when ceph-admin option fails.
v0.3.2 Added bytesfree counts for S3 endpoints and exception for aws
       ceph-admin error.
v0.3.3 Fixed exception handling for python3 syntax.
v0.3.4 Fixed json parsing from requests using it's native json function to
       solve issue with json module of python 3.4.
v0.4.0 Re-wrote the exception classes and how they are treated in code. Added
       warnings.
"""
from __future__ import print_function

__version__ = "v0.4.0"
__author__ = "Fernando Fernandez Galindo"

import warnings
import sys
import os
from io import BytesIO
from optparse import OptionParser, OptionGroup
import glob
import json

IS_PYTHON2 = sys.version_info[0] == 2

if IS_PYTHON2:
    from urlparse import urlsplit
else:
    from urllib.parse import urlsplit

try:
    import boto3
except ImportError:
    print('ImportError: Please install "boto3" modules')
    sys.exit(1)
else:
    from botocore.client import Config

try:
    from lxml import etree
except ImportError:
    print('ImportError: Please install "lxml" modules')
    sys.exit(1)

try:
    import memcache
except ImportError:
    print('ImportError: Please install "memcache" modules')
    sys.exit(1)

try:
    import requests
except ImportError:
    print('ImportError: Please install "requests" modules')
    sys.exit(1)

try:
    from requests_aws4auth import AWS4Auth
except ImportError:
    print('ImportError: Please install "requests_aws4auth" modules')
    sys.exit(1)


################
## Help/Usage ##
################

usage = "usage: %prog [options]"
parser = OptionParser(usage)

#parser.add_option('-v', '--verbose', dest='verbose', action='count', help='Increase verbosity level for debugging this script (on stderr)')
parser.add_option('-d', '--dir',
                  dest='configs_directory', action='store', default='/etc/ugr/conf.d',
                  help="Path to UGR's endpoint .conf files."
                 )

group = OptionGroup(parser, "Memcached options")
group.add_option('--memhost',
                 dest='memcached_ip', action='store', default='127.0.0.1',
                 help='IP or hostname of memcached instance. Default: 127.0.0.1'
                )
group.add_option('--memport',
                 dest='memcached_port', action='store', default='11211',
                 help='Port where memcached instances listens on. Default: 11211'
                )

parser.add_option_group(group)

group = OptionGroup(parser, "Output options")
group.add_option('--debug',
                 dest='debug', action='store_true', default=False,
                 help='Declare to enable debug output on stdout.'
                )
group.add_option('-m', '--memcached',
                 dest='output_memcached', action='store_true', default=False,
                 help='Declare to enable uploading information to memcached.'
                )
group.add_option('--stdout',
                 dest='output_stdout', action='store_true', default=False,
                 help='Set to output stats on stdout. If no other output option is set, this is enabled by default.'
                )

#group.add_option('-o', '--outputfile',
#                 dest='out_file', action='store', default=None,
#                 help='Change where to ouput the data. Default: None'
#                )
parser.add_option_group(group)

options, args = parser.parse_args()


#######################
## Exception Classes ##
#######################

class UGRBaseException(Exception):
    def __init__(self, message=None):
        if message is None:
            # Set some default useful error message
            self.message = "[ERROR] An unkown exception occured processing"
        else:
            self.message = message
        super(UGRBaseException, self).__init__(self.message)

### Defining Error Exception Classes
class UGRBaseError(UGRBaseException):
    def __init__(self, message=None):
        if message is None:
            # Set some default useful error message
            self.message = "[ERROR] A unkown error occured."
        else:
            self.message = "[ERROR] " + message
        super(UGRBaseError, self).__init__(self.message)

class UGRConfigFileError(UGRBaseError):
    def __init__(self, message=None):
        if message is None:
            # Set some default useful error message
            self.message = "An unkown error occured reading a configuration file."
        super(UGRConfigFileError, self).__init__(self.message)

class UGRConfigFileErrorIDMismatch(UGRConfigFileError):
    def __init__(self, endpoint, line):
        self.message ='Failed to match ID "%s" in line "%s". Check your configuration.\n' \
                  % (endpoint, line)
        super(UGRConfigFileErrorIDMismatch, self).__init__(self.message)


class UGRConfigFileErrorMissingRequiredOption(UGRConfigFileError):
    def __init__(self, endpoint, option):
        self.message = 'Option "%s" is required, please check configuration for "%s"\n' \
                  % (option, endpoint)
        super(UGRConfigFileErrorMissingRequiredOption, self).__init__(self.message)

class UGRConfigFileErrorInvalidOption(UGRConfigFileError):
    def __init__(self, endpoint, option, valid_options):
        self.message = 'Incorrect value given in option "%s" for "%s". Please check configuration.\nValid options: %s' \
                  % (option, endpoint, valid_options)
        super(UGRConfigFileErrorInvalidOption, self).__init__(self.message)

class UGRStorageStatsError(UGRBaseError):
    def __init__(self, message=None):
        if message is None:
            # Set some default useful error message
            self.message = "An unkown error occured obtaning storage stats."
        else:
            self.message = message
        super(UGRStorageStatsError, self).__init__(self.message)

class UGRStorageStatsErrorS3Method(UGRStorageStatsError):
    def __init__(self, endpoint, option, status_code, error):
        self.message = '"%s" does not support option "%s".\nConnection Code: "%s"\nConnection Error: "%s".\n' \
                  % (endpoint, option, status_code, error)
        super(UGRStorageStatsErrorS3Method, self).__init__(self.message)

class UGRStorageStatsErrorDAVQuotaMethod(UGRStorageStatsError):
    def __init__(self, endpoint):
        self.message = '"%s" does not support "WebDAV Quota Method".' \
                  % (endpoint)
        super(UGRStorageStatsErrorDAVQuotaMethod, self).__init__(self.message)


#####################
## Storage Classes ##
#####################

class StorageStats(object):
    """
    Class that will define how data from UGR's configruation file will be stored
    for earch storage endpoint. As well as how to obtain stats and output it.
    """
    def __init__(self, _ep):
        self.stats = {
                      'bytesused': 0,
                      'bytesfree': 0,
                      'files': 0,
                      'quota': 10000000000000,
                     }
        self.id = _ep['id']
        self.options = _ep['options']
        self.plugin = _ep['plugin']
        self.url = _ep['url']

        self.validators = {
            'ssl_check': {
                'boolean': True,
                'default': True,
                'required': False,
                'valid': ['true', 'false', 'yes', 'no']
            },
        }

    def upload_to_memcached(self, memcached_ip='127.0.0.1', memcached_port='11211'):
        """
        Connects to a memcached instance and uploads the endpoints storage stats:
        self.id, self.stats['quota'], self.stats['bytesused']
        """
        memcached_srv = memcached_ip + ':' + memcached_port
        mc = memcache.Client([memcached_srv])
        index = "Ugrstoragestats_" + self.id
        storagestats = '%%'.join([self.id, str(self.stats['quota']), str(self.stats['bytesused'])])
        mc.set(index, storagestats)
        return 0

    def get_storagestats(self):
        """
        Method for obtaining contacting a storage endpoint and obtain storage
        stats. Will be re-defined for each sub-class as each storage endpoint
        type requires different API's.
        """
        pass

    def validate_ep_options(self,options):
        """
        Check the endpoints options from UGR's configuration file against the
        set of default and valid options defined under the self.validators dict.
        """
        for ep_option in self.validators:
            # First check if the option has been defined in the config file..
            # If it is missing, check if it is required, and exit if true
            # otherwise set it to the default value and print a warning.
            try:
                self.options[ep_option]

            except KeyError:
                if self.validators[ep_option]['required']:
                    raise UGRConfigFileErrorMissingRequiredOption(
                              endpoint=self.id,
                              option=ep_option,
                          )
                else:
                    warnings.warn('No "%s" specified for "%s". Setting it to default value "%s"\n' \
                                   % (ep_option, self.id, self.validators[ep_option]['default']),
                                 )
                    self.options.update({ep_option: self.validators[ep_option]['default']})

            # If the ep_option has been defined, check against a list of valid
            # options (if defined, otherwise contiune). Also transform to boolean
            # form those that have the "boolean" key set as true.
            else:
                try:
                    self.validators[ep_option]['valid']
                except KeyError:
                    pass
                else:
                    if self.options[ep_option] not in self.validators[ep_option]['valid']:
                        raise UGRConfigFileErrorInvalidOption(
                                  endpoint=self.id,
                                  option=ep_option,
                                  valid_options=self.validators[ep_option]['valid']
                              )
                    else:
                        try:
                            self.validators[ep_option]['boolean']
                        except KeyError:
                            pass
                        else:
                            ### Review this!
                            if self.options['ssl_check'].lower() == 'false'\
                            or self.options['ssl_check'].lower() == 'no':
                                self.options.update({'ssl_check': False})
                            else:
                                self.options.update({'ssl_check': True})

    def validate_schema(self, scheme):
        schema_translator = {
            'dav': 'http',
            'davs': 'https',
        }

        if scheme in schema_translator:
            return (schema_translator[scheme])
        else:
            return (scheme)



class S3StorageStats(StorageStats):
    """
    Subclass that defines methods for obtaining storage stats of S3 endpoints.
    """
    def __init__(self, *args, **kwargs):
        """
        Extend the object's validators unique to the storage type to make sure
        the storage status check can proceed.
        """
        super(S3StorageStats, self).__init__(*args, **kwargs)
        self.validators.update({
            's3.alternate': {
                'default': 'false',
                'required': False,
                'valid': ['true', 'false', 'yes', 'no']
            },
            's3.api': {
                'default': 'generic',
                'required': True,
                'valid': ['ceph-admin', 'generic'],
            },
            's3.priv_key': {
                'required': True,
            },
            's3.pub_key': {
                'required': True,
            },
            's3.region': {
                'required': True,
            },
            's3.signature_ver': {
                'default': 's3v4',
                'required': False,
                'valid': ['s3', 's3v4'],
            },
        })

    def get_storagestats(self):
        """
        Connect to the storage endpoint with the defined or generic API's
        to obtain the storage status.
        """
        # Split the URL in the configuration file for validation and proper
        # formatting according to the method's needs.
        u = urlsplit(self.url)
        scheme = self.validate_schema(u.scheme)

        # Getting the storage Stats CephS3's Admin API
        if self.options['s3.api'].lower() == 'ceph-admin':
            if self.options['s3.alternate'].lower() == 'true'\
            or self.options['s3.alternate'].lower() == 'yes':
                endpoint_url = '{uri_scheme}://{uri.netloc}/admin/bucket?format=json'.format(uri=u, uri_scheme=scheme)
                bucket = u.path.rpartition("/")[-1]
                payload = {'bucket': bucket, 'stats': 'True'}

            else:
                bucket, domain = u.netloc.partition('.')[::2]
                endpoint_url = '{uri_scheme}://{uri_domain}/admin/{uri_bucket}?format=json'.format(uri=u, uri_scheme=scheme, uri_bucket=bucket, uri_domain=domain)
                payload = {'bucket': bucket, 'stats': 'True'}

            auth = AWS4Auth(self.options['s3.pub_key'],
                            self.options['s3.priv_key'],
                            self.options['s3.region'],
                            's3',
                            verify=self.options['ssl_check']
                           )
            r = requests.get(endpoint_url,
                             params=payload,
                             auth=auth,
                             verify=self.options['ssl_check']
                            )

            # If ceph-admin is accidentally requested for AWS, no JSON content
            # is passed, so we check for that.
            # Review this!
            try:
                stats = r.json()
            except ValueError:
                stats = {'Code': r.content}

            # Make sure we get a 200 "OK" from the endpoint.
            try:
                if r.status_code != 200:
                    raise UGRStorageStatsErrorS3Method(
                            endpoint=self.id,
                            option=self.options['s3.api'],
                            status_code=r.status_code,
                            error=stats['Code'],
                    )
            except UGRStorageStatsError as ERR:
                warnings.warn(ERR.message)
            else:
                self.stats['quota'] = stats['bucket_quota']['max_size']
                self.stats['bytesused'] = stats['usage']['rgw.main']['size_utilized']
                self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

        # Getting the storage Stats AWS S3 API
        #elif self.options['s3.api'].lower() == 'aws-cloudwatch':

        # Generic list all objects and add sizes using list-objectsv2 AWS-Boto3
        # API, should work for any compatible S3 endpoint.
        elif self.options['s3.api'].lower() == 'generic':
            if self.options['s3.alternate'].lower() == 'true'\
            or self.options['s3.alternate'].lower() == 'yes':
                endpoint_url = '{uri_scheme}://{uri.netloc}'.format(uri=u, uri_scheme=scheme)
                bucket = u.path.rpartition("/")[-1]

            else:
                bucket, domain = u.netloc.partition('.')[::2]
                endpoint_url = '{uri_scheme}://{uri_domain}'.format(uri=u, uri_scheme=scheme, uri_domain=domain)

            connection = boto3.client('s3',
                                      region_name=self.options['s3.region'],
                                      endpoint_url=endpoint_url,
                                      aws_access_key_id=self.options['s3.pub_key'],
                                      aws_secret_access_key=self.options['s3.priv_key'],
                                      use_ssl=True,
                                      verify=self.options['ssl_check'],
                                      config=Config(signature_version=self.options['s3.signature_ver']),
                                     )

            total_bytes = 0
            total_files = 0
            kwargs = {'Bucket': bucket}
            while True:
                response = connection.list_objects(**kwargs)
                try:
                    response['Contents']
                except KeyError:
                    self.stats['bytesused'] = '0'
                    break
                else:
                    for content in response['Contents']:
                        total_bytes += content['Size']
                        total_files += 1

                try:
                    kwargs['Marker'] = response['NextMarker']
                except KeyError:
                    break

            self.stats['bytesused'] = total_bytes
            self.stats['files'] = total_files
            self.stats['bytesfree'] = self.stats['quota'] - self.stats['bytesused']

    def validate_schema(self, scheme):
        if scheme == 's3':
            if self.options['ssl_check']:
                return ('https')
            else:
                return ('http')
        else:
            return (scheme)


class DAVStorageStats(StorageStats):
    """
    Subclass that defines methods for obtaining storage stats of S3 endpoints.
    """
    def __init__(self, *args, **kwargs):
        """
        Extend the object's validators unique to the storage type to make sure
        the storage status check can proceed.
        """
        super(DAVStorageStats, self).__init__(*args, **kwargs)
        self.validators.update({
            'ca_path': {
                'default': False,
                'required': False,
            },
            'cli_certificate': {
                'required': True,
            },
            'cli_private_key': {
                'required': True,
            },
        })

    def get_storagestats(self):
        u = urlsplit(self.url)
        scheme = self.validate_schema(u.scheme)
        endpoint_url = '{uri_scheme}://{uri.netloc}{uri.path}'.format(uri=u, uri_scheme=scheme)

        headers = {'Depth': '0',}
        data = create_free_space_request_content()
        try:
            print("Trying to connect to: ", self.id)
            response = requests.request(
                method="PROPFIND",
                url=endpoint_url,
                cert=(self.options['cli_certificate'], self.options['cli_private_key']),
                headers=headers,
                verify=self.options['ca_path'],
                data=data
            )
        except requests.exceptions.SSLError:
            print("Some SSL Error")
        except IOError:
            print("Issue reading credential/proxy/cert file")

        else:
            tree = etree.fromstring(response.content)
            try:
                node = tree.find('.//{DAV:}quota-available-bytes').text
                if node is not None:
                    pass
                else:
                    raise UGRStorageStatsErrorDAVQuotaMethod(endpoint=self.id)
            except UGRStorageStatsError as ERR:
                warnings.warn(ERR.message)

            else:
                self.stats['bytesused'] = int(tree.find('.//{DAV:}quota-used-bytes').text)
                self.stats['bytesfree'] = int(tree.find('.//{DAV:}quota-available-bytes').text)

                # If quota-available-bytes is reported as '0' is because no quota is
                # provided, so we use the one from the config file or default.
                if self.stats['bytesfree'] != 0:
                    self.stats['quota'] = self.stats['bytesused'] + self.stats['bytesfree']
    #        except TypeError:
    #            raise MethodNotSupported(name='free', server=hostname)
    #        except etree.XMLSyntaxError:
    #            return str()


###############
## Functions ##
###############

def get_config(config_dir="/etc/ugr/conf.d/"):
    """
    Function that returns a dictionary in which every key represents a
    storage endpoint defined in the ugr configuration files. These files will
    be any *.conf file defined under the config_dir variable.
    The default directory is "/etc/ugr/conf.d/"
    All the glb.locplugin options defined for each are stored as dictionary keys under
    each parent SE key, and the locplugin as keys for the dictionary "options" under
    each parent SE key.
    """
    endpoints = {}
    os.chdir(config_dir)
    for config_file in glob.glob("*.conf"):
        with open(config_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line.startswith("#"):

                    if "glb.locplugin[]" in line:
                        _plugin, _id, _concurrency, _url = line.split(" ")[1::]
                        endpoints.setdefault(_id, {})
                        endpoints[_id].update({'id':_id.strip()})
                        endpoints[_id].update({'url':_url.strip()})
                        endpoints[_id].update({'plugin':_plugin.split("/")[-1]})

                    elif "locplugin" in line:
                        key, _val = line.partition(":")[::2]
                        # Match an _id in key
                        try:
                            if _id in key:
                                _option = key.split(_id+'.')[-1]
                                endpoints.setdefault(_id, {})
                                endpoints[_id].setdefault('options', {})
                                endpoints[_id]['options'].update({_option:_val.strip()})
                            else:
                                raise UGRConfigFileErrorIDMismatch(endpoint=_id, line=line)
                        except UGRConfigFileError as ERR:
                            warnings.warn(ERR.message)
                    else:
                        # Ignore any other lines
                        #print( "I don't know what to do with %s", line)
                        pass

    return endpoints

def factory(plugin_type):
    """
    Return object class to use based on the plugin specified in the UGR's
    configuration files.
    """
    switcher = {
        'libugrlocplugin_dav.so': DAVStorageStats,
        'libugrlocplugin_http.so': DAVStorageStats,
        'libugrlocplugin_s3.so': S3StorageStats,
        #'libugrlocplugin_azure.so': AzureStorageStats,
        #'libugrlocplugin_davrucio.so': RucioStorageStats,
        #'libugrlocplugin_dmliteclient.so': DMLiteStorageStats,
    }
    return switcher.get(plugin_type, "nothing")


def get_endpoints(options):
    """
    Returns list of storage endpoint objects whose class represents each storage
    endpoint configured in UGR's configuration files.
    """
    storage_objects = []
    endpoints = get_config(options.configs_directory)
    for endpoint in endpoints:
        try:
            if options.debug:
                print("Working on endpoint %s" % (endpoint))
            ep = factory(endpoints[endpoint]['plugin'])(endpoints[endpoint])
        except TypeError:
            print('Storage Endpoint Type "%s" not implemented yet. Skipping %s'
                  % (endpoints[endpoint]['plugin'], endpoint)
                 )
        else:
            ep.validate_ep_options(options)
            storage_objects.append(ep)

    return(storage_objects)

def create_free_space_request_content():
    """Creates an XML for requesting of free space on remote WebDAV server.

    :return: the XML string of request content.
    """
    root = etree.Element("propfind", xmlns="DAV:")
    prop = etree.SubElement(root, "prop")
    etree.SubElement(prop, "quota-available-bytes")
    etree.SubElement(prop, "quota-used-bytes")
    tree = etree.ElementTree(root)
    buff = BytesIO()
    tree.write(buff, xml_declaration=True, encoding='UTF-8')
    return buff.getvalue()

def warning_on_one_line(message, category, filename, lineno, file=None, line=None):
    #return '%s:%s: %s: %s\n' % (filename, lineno, category.__name__, message)
    return '%s: %s\n' % (category.__name__, message)

#############
# Self-Test #
#############

if __name__ == '__main__':
    # Warning messages are disabled by default.
    if options.debug is False:
        warnings.simplefilter("ignore")
    warnings.formatwarning = warning_on_one_line

    endpoints = get_endpoints(options)

    for endpoint in endpoints:
#        print(endpoint.stats)
#        print(endpoint.validators)
        #print('\n', type(endpoint), '\n')
        #print('\n', endpoint.options, '\n')
        endpoint.get_storagestats()
        if options.output_memcached:
            endpoint.upload_to_memcached(options.memcached_ip, options.memcached_port)

            if options.debug:
                # Print out the contents of each index created to check stats
                # were uploaded.
                mc = memcache.Client([options.memcached_ip + ':' + options.memcached_port])
                index = "Ugrstoragestats_" + endpoint.id
                print('Probing memcached index:', index)
                index_contents = mc.get(index)
                if index_contents is None:
                    print('No content found at index: %s\n' %(index))
                else:
                    print(index_contents, '\n')
        else:
            options.output_stdout = True

        if options.output_stdout:
            print('\nSE:', endpoint.id, '\nURL:', endpoint.url, \
                  '\nQuota:', endpoint.stats['quota'], \
                  '\nBytes Used:', endpoint.stats['bytesused'], \
                  '\nBytes Free:', endpoint.stats['bytesfree'], \
                  '\n'
                 )
