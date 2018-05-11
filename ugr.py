#!/usr/bin/python
"""
Set of libraries to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints
"""
from __future__ import print_function

__version__ = "0.0.1"
__author__ = "Fernando Fernandez Galindo"

import memcache
import requests
import json
import os
from requests_aws4auth import AWS4Auth
from urlparse import urlsplit

#############
## Classes ##
#############

class storageStats(object):
    def __init__(self, ep={}):
        self.bytesused = 0
        self.id = ep['id']
        self.options = ep['options']
        self.quota = -1
        self.plugin = ep['plugin']
        self.url = ep['url']

    def upload_to_memcached ( self, memcached_ip='127.0.0.1', memcached_port='11211' ):
        memcached_srv = memcached_ip + ':' + memcached_port
        mc = memcache.Client([memcached_srv])
        index = "Ugrstoragestats_" + self.name
        mc.set(index, x)
        return 0

    def get_storagestats( self, ep={} ): pass

class s3StorageStats( storageStats ):

    def get_storagestats( self ):
        for key in ep:
            quota = 'N/A'
            free = 'N/A'
            for option in ep[key]:
                if option == 's3.ceph_api':
                    u = urlsplit( ep[key]['url'] )
                    if ep[key]['s3.alternate']:
                        api_url = '{uri.scheme}://{uri.hostname}/admin/bucket?format=json'.format(uri = u)
                        payload = { 'bucket': u.path.strip("/"), 'stats': 'True' }
                    else:
                        u_bucket, u_domain = u.hostname.partition('.')[::2]
                        api_url = '{uri.scheme}://{uri_domain}/admin/{uri_bucket}?format=json'.format(uri = u, uri_bucket = u_bucket, uri_domain = u_domain)
                        payload = { 'bucket': u_bucket, 'stats': 'True' }
                    auth = AWS4Auth(ep[key]['s3.pub_key'], ep[key]['s3.priv_key'], 'us-east-1', 's3', verify=False )
                    r = requests.get(api_url, params=payload, auth=auth, verify=False)
                    stats = json.loads(r.content)
                    quota = str( stats['bucket_quota']['max_size'] )
                    free = str( stats['bucket_quota']['max_size'] - stats['usage']['rgw.main']['size_utilized'] )



###############
## Functions ##
###############

def get_conf( configs ="/etc/ugr/conf.d/endpoints.conf" ):
#def getugrconf( configs ="./endpoints.conf" ):
    endpoints = {}

    # Read the configuration file and extract the plugin URL and it's options.
    # The temp_vals dict will be merged later using the endpoint name as key
    # as a nested dict, so we can have each enpoints options together.

    with open( configs, "r") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("#"):

                if "glb.locplugin[]" in line:
                    _plugin, _id, _concurrency, _url = line.split(" ")[1::]
                    endpoints.setdefault(_id,{})
                    endpoints[_id].update({'id':_id.strip()})
                    endpoints[_id].update({'url':_url.strip()})
                    endpoints[_id].update({'url':_url.strip()})
                    endpoints[_id].update({'plugin':_plugin.split("/")[-1]})

                elif "locplugin" in line:
                    key, _val = line.partition(":")[::2]
                    _id, _option = key.split(".",2)[1:]
                    endpoints.setdefault(_id,{})
                    endpoints[_id].setdefault('options',{})
                    endpoints[_id]['options'].update({_option:_val.strip()})

                else:
                    #print( "I don't know what to do with %s", line)
                    pass

    return endpoints
