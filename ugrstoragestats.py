#!/usr/bin/python
"""
Set of libraries to interact with UGR's configuration files in order to obtain
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
v.0.0.7 Added options
v.0.1.0 Changed aws-list to generic and now uses boto3 for generality.
v.0.2.0 Added validators key and 'validate_options' function.
v.0.2.1 Cleaned up code to PEP8.
v.0.2.2 Exception for plugint types not yet implemented.
v.0.2.3 Fixed bucket-name issue if not at paths' root and non-standard ports for
        S3 endpoints.
"""
from __future__ import print_function

__version__ = "0.2.3"
__author__ = "Fernando Fernandez Galindo"

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

usage = "usage: %prog [options] api-hostname"
parser = OptionParser(usage)

#parser.add_option('-v', '--verbose', dest='verbose', action='count', help='Increase verbosity level for debugging this script (on stderr)')
parser.add_option('-d', '--dir',
                  dest='directory', action='store', default='/etc/ugr/conf.d',
                  help="Path to UGR's endpoint .conf files."
                 )

group = OptionGroup(parser, "Memcached options")
group.add_option('--memhost',
                 dest='memhost', action='store', default='127.0.0.1',
                 help="IP or hostname of memcached instance. Default: 127.0.0.1"
                )
group.add_option('--memport',
                 dest='memport', action='store', default='11211',
                 help="Port tof memcached instance. Default: 11211"
                )

parser.add_option_group(group)

#group = OptionGroup(parser, "Output options")
#group.add_option('-m', '--memcached', dest='memcached_ouput', action='store_true', default=False, help="Declare to enable uploading information to memcached.")
#group.add_option('-o', '--outputfile', dest='out_file', action='store', default=None, help="Change where to ouput the data. Default: None")
#parser.add_option_group(group)

options, args = parser.parse_args()


#############
## Classes ##
#############

class StorageStats(object):
    """
    Class that will define how data from UGR's configruation file will be stored
    for earch storage endpoint. As well as how to obtain stats and output it.
    """
    def __init__(self, _ep):
        self.stats = {"bytesused": 0, 'quota': '-1'}
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
        storagestats = '%%'.join([self.id, self.stats['quota'], self.stats['bytesused']])
        mc.set(index, storagestats)
        return 0

    def get_storagestats(self):
        """
        Method for obtaining contacting a storage endpoint and obtain storage
        stats. Will be re-defined for each sub-class as each storage endpoint
        type requires different API's.
        """
        pass

    def validate_options(self):
        """
        Check the endpoints options from UGR's configuration file against the
        set of default and valid options defined under the self.validators dict.
        """
        for option in self.validators:
            # First check if the option has been defined in the config file..
            # If it is missing, check if it is required, and exit if true
            # otherwise set it to the default value.
            try:
                self.options[option]

            except KeyError:
                if self.validators[option]['required']:
                    print('Option "%s" is required, please check configuration\n'
                          % (option)
                         )
                    sys.exit(1)

                else:
                    print('No "%s" specified. Setting it to default value "%s"\n'
                          % (option, self.validators[option]['default'])
                         )
                    self.options.update({option: self.validators[option]['default']})

            # If the option has been defined, check against a list of valid
            # options (if defined, otherwise contiune). Also transform to boolean
            # form those that have the "boolean" key set as true.
            else:
                try:
                    self.validators[option]['valid']

                except KeyError:
                    pass

                else:
                    if self.options[option] not in self.validators[option]['valid']:
                        print('Incorrect value in option "%s". \
                              \nPlease check configuration. Valid options:\n %s'
                              % (option, self.validators[option]['valid'])
                             )
                        sys.exit(1)

                    else:
                        try:
                            self.validators[option]['boolean']

                        except KeyError:
                            pass

                        else:
                            if self.options['ssl_check'].lower() == 'false'\
                            or self.options['ssl_check'].lower() == 'no':
                                self.options.update({'ssl_check': False})

                            else:
                                self.options.update({'ssl_check': True})



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
        })

    def get_storagestats(self):
        """
        Connect to the storage endpoint with the defined or generic API's
        to obtain the storage status.
        """
        # Getting the storage Stats CephS3's Admin API
        if self.options['s3.api'].lower() == 'ceph-admin':
            u = urlsplit(self.url)
            if self.options['s3.alternate'].lower() == 'true'\
            or self.options['s3.alternate'].lower() == 'yes':
                endpoint_url = '{uri.scheme}://{uri.netloc}/admin/bucket?format=json'.format(uri=u)
                bucket = u.path.rpartition("/")[-1]
                payload = {'bucket': bucket, 'stats': 'True'}

            else:
                bucket, domain = u.netloc.partition('.')[::2]
                endpoint_url = '{uri.scheme}://{uri_domain}/admin/{uri_bucket}?format=json'.format(uri=u, uri_bucket=bucket, uri_domain=domain)
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
            stats = json.loads(r.content)
            self.stats['quota'] = str(stats['bucket_quota']['max_size'])
            self.stats['bytesused'] = str(stats['usage']['rgw.main']['size_utilized'])

        # Getting the storage Stats AWS S3 API
        #elif self.options['s3.api'].lower() == 'aws-cloudwatch':

        # Generic list all objects and add sizes using list-objectsv2 AWS-Boto3
        # API, should work for any compatible S3 endpoint.
        elif self.options['s3.api'].lower() == 'generic':
            u = urlsplit(self.url)
            if self.options['s3.alternate'].lower() == 'true'\
            or self.options['s3.alternate'].lower() == 'yes':
                endpoint_url = '{uri.scheme}://{uri.netloc}'.format(uri=u)
                bucket = u.path.rpartition("/")[-1]

            else:
                bucket, domain = u.netloc.partition('.')[::2]
                endpoint_url = '{uri.scheme}://{uri_domain}'.format(uri=u, uri_domain=domain)

            connection = boto3.client('s3',
                                      region_name=self.options['s3.region'],
                                      endpoint_url=endpoint_url,
                                      aws_access_key_id=self.options['s3.pub_key'],
                                      aws_secret_access_key=self.options['s3.priv_key'],
                                      use_ssl=True,
                                      verify=self.options['ssl_check'],
                                     )
            response = connection.list_objects_v2(Bucket=bucket,)
            total_bytes = 0
            total_files = 0
            for content in response['Contents']:
                total_bytes += content['Size']
                total_files += 1
            self.stats['bytesused'] = str(total_bytes)


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
            'cli_certificate': {
                'required': True,
            },
            'cli_private_key': {
                'required': True,
            },
        })

    def get_storagestats(self):
        headers = {'Depth': '0',}
        data = create_free_space_request_content()
        response = requests.request(
            method="PROPFIND",
            url=self.url,
            cert=(self.options['cli_certificate'], self.options['cli_private_key']),
            headers=headers,
            verify=self.options['ca_path'],
            data=data
        )

        tree = etree.fromstring(response.content)
        #self. = tree.find('.//{DAV:}quota-available-bytes').text
        self.stats['bytesused'] = tree.find('.//{DAV:}quota-used-bytes').text

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
                        _id, _option = key.split(".", 2)[1:]
                        endpoints.setdefault(_id, {})
                        endpoints[_id].setdefault('options', {})
                        endpoints[_id]['options'].update({_option:_val.strip()})

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
        #'libugrlocplugin_http.so': DAVStorageStats,
        'libugrlocplugin_s3.so': S3StorageStats,
        #'libugrlocplugin_azure.so': AzureStorageStats,
        #'libugrlocplugin_davrucio.so': RucioStorageStats,
        #'libugrlocplugin_dmliteclient.so': DMLiteStorageStats,
    }
    return switcher.get(plugin_type, "nothing")


def object_creator(config_dir="/etc/ugr/conf.d/"):
    """
    Returns list of storage endpoint objects whose class represents each storage
    endpoint configured in UGR's configuration files.
    """
    storage_objects = []
    endpoints = get_config(config_dir)
    for endpoint in endpoints:
        try:
            ep = factory(endpoints[endpoint]['plugin'])(endpoints[endpoint])
        except TypeError:
            print('Storage Endpoint Type "%s" not implemented yet. Skipping %s'
                  % (endpoints[endpoint]['plugin'], endpoint)
                 )
        else:
            ep.validate_options()
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

def parse_free_space_response(content, hostname):
    """Parses of response content XML from WebDAV server and extract an amount of free space.

    :param content: the XML content of HTTP response from WebDAV server for getting free space.
    :param hostname: the server hostname.
    :return: an amount of free space in bytes.
    """
    try:
        tree = etree.fromstring(content)
        node = tree.find('.//{DAV:}quota-available-bytes')
        if node is not None:
            return int(node.text)
        else:
            raise MethodNotSupported(name='free', server=hostname)
    except TypeError:
        raise MethodNotSupported(name='free', server=hostname)
    except etree.XMLSyntaxError:
        return str()


#############
# Self-Test #
#############

if __name__ == '__main__':
    endpoints = object_creator(options.directory)
    memcached_srv = '127.0.0.1:11211'
    mc = memcache.Client([memcached_srv])

    endpoint = 'https://srm-test.gridpp.ecdf.ed.ac.uk/dpm/ecdf.ed.ac.uk/home/atlas/'


    for endpoint in endpoints:
#        print(endpoint.stats)
#        print(endpoint.validators)
        #print('\n', type(endpoint), '\n')
#        print('\n', endpoint.options, '\n')
        endpoint.get_storagestats()
#        endpoint.upload_to_memcached()
        #print('\n', ep.options, '\n')
        print('\nSE:', endpoint.id, '\nURL:', endpoint.url, '\nQuota:', endpoint.stats['quota'], '\nBytes Used:', endpoint.stats['bytesused'], '\n')
#        index = "Ugrstoragestats_" + endpoint.id
#        print('Probing memcached index:', index)
#        print(mc.get(index), '\n')
