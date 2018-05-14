#!/usr/bin/python
"""
Set of libraries to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints.

v0.0.1 Works with cephS3 AdminAPI.
v0.0.2 Added AWS list-type2 API to list all objects in bucket and add object size.
       Added memcached format and upload each endpoint obtained.
v0.0.3 Added support for python3
v0.0.4 Changed from single configration file to all *.conf files in given directory.
"""
from __future__ import print_function

__version__ = "0.0.4"
__author__ = "Fernando Fernandez Galindo"

import sys
import os
import glob
import json

IS_PYTHON2 = sys.version_info[0] == 2

if IS_PYTHON2:
    from urlparse import urlsplit
else:
    from urllib.parse import urlsplit

from lxml import etree
import memcache
import requests
from requests_aws4auth import AWS4Auth


#############
## Classes ##
#############

class StorageStats(object):
    """
    Class that will define how data from UGR's configruation file will be stored
    for earch storage endpoint. As well as how to obtain stats and output it.
    """
    def __init__(self, _ep):
        self.bytesused = 0
        self.id = _ep['id']
        self.options = _ep['options']
        self.quota = '-1'
        self.plugin = _ep['plugin']
        self.url = _ep['url']

    def upload_to_memcached(self, memcached_ip='127.0.0.1', memcached_port='11211'):
        memcached_srv = memcached_ip + ':' + memcached_port
        mc = memcache.Client([memcached_srv])
        index = "Ugrstoragestats_" + self.id
        storagestats = '%%'.join([self.id, self.quota, self.bytesused])
        mc.set(index, storagestats)
        return 0

    def get_storagestats(self):
        pass

class S3StorageStats(StorageStats):
    """
    Subclass that defines methods for obtaining storage stats of S3 endpoints.
    """

    def get_storagestats(self):
        try:
            self.options['s3.region']
        except KeyError:
            print('No s3.region option specified inf config. Trying "us-east-1"')
            self.options.update({'s3.region': 'us-east-1'})

        # Check if user specified a specific type of API to use. If not, we will
        # extract the BytesUsed by listing objects and summing their size.
        try:
            self.options['s3.api']
        except KeyError:
            print('\nNo s3.api option specified. Setting to "generic"')
            self.options.update({'s3.api': 'generic'})

        try:
            self.options['s3.alternate']
        except KeyError:
            print('\nNo s3.alternate option specified. Setting s3.alternate to "false"')
            self.options.update({'s3.alternate': 'false'})

        # Getting the storage Stats CephS3's Admin API
        if self.options['s3.api'].lower() == 'ceph-admin':
            u = urlsplit(self.url)
            if self.options['s3.alternate'].lower() == 'true' or self.options['s3.alternate'].lower() == 'yes':
                api_url = '{uri.scheme}://{uri.hostname}/admin/bucket?format=json'.format(uri=u)
                payload = {'bucket': u.path.strip("/"), 'stats': 'True'}
            else:
                u_bucket, u_domain = u.hostname.partition('.')[::2]
                api_url = '{uri.scheme}://{uri_domain}/admin/{uri_bucket}?format=json'.format(uri=u, uri_bucket=u_bucket, uri_domain=u_domain)
                payload = {'bucket': u_bucket, 'stats': 'True'}
            auth = AWS4Auth(self.options['s3.pub_key'], self.options['s3.priv_key'], self.options['s3.region'], 's3', verify=False)
            r = requests.get(api_url, params=payload, auth=auth, verify=False)
            stats = json.loads(r.content)
            self.quota = str(stats['bucket_quota']['max_size'])
            self.bytesused = str(stats['usage']['rgw.main']['size_utilized'])

        # Getting the storage Stats AWS S3 API
        elif self.options['s3.api'].lower()  == 'aws-list':
            #This part hasn't been dealt with.
            if self.options['s3.alternate'].lower() == 'true' or self.options['s3.alternate'].lower() == 'yes':
                u = urlsplit(self.url)
                api_url = '{uri.scheme}://{uri.hostname}/admin/bucket?format=json'.format(uri=u)
                payload = {'bucket': u.path.strip("/"), 'stats': 'True'}
            else:
                api_url = self.url
                payload = {'list-type': 2}
            auth = AWS4Auth(self.options['s3.pub_key'], self.options['s3.priv_key'], self.options['s3.region'], 's3', verify=False)
            r = requests.get(api_url, params=payload, auth=auth, verify=False)
            xml_tree = etree.fromstring(r.content)
            contents = xml_tree.findall('Contents', namespaces=xml_tree.nsmap)
            total_bytes = 0
            total_files = 0
            for content in contents:
                count = content.find('Size', namespaces=xml_tree.nsmap).text
                count = int(count)
                total_bytes += count
                total_files += 1

            self.bytesused = str(total_bytes)
#            stats = json.loads(r.content)
#            self.quota = str( stats['bucket_quota']['max_size'] )
#            self.bytesused = str( stats['usage']['rgw.main']['size_utilized'] )



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

#############
# Self-Test #
#############

if __name__ == '__main__':
    import ugr
    endpoints = ugr.get_config('./')
    memcached_srv = '127.0.0.1:11211'
    mc = memcache.Client([memcached_srv])

    for endpoint in endpoints:
        ep = ugr.S3StorageStats(endpoints[endpoint])
        print('\n', ep.url, '\n')
        #print('\n', type(ep), '\n')
        #print('\n', ep.options, '\n')
        ep.get_storagestats()
        ep.upload_to_memcached()
        #print('\n', ep.options, '\n')
        print('\nSE:', ep.id, '\nQuota:', ep.quota, '\nBytes Used:', ep.bytesused, '\n')
        index = "Ugrstoragestats_" + ep.id
        print('Probing memcached index:', index)
        print(mc.get(index), '\n')
