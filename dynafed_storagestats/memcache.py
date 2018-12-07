"""
Functions to deal with obtaining and placing data into an memcache instance.
"""

import memcache

from dynafed_storagestats import exceptions

#############
# Functions #
#############

def get(index, memcached_ip='127.0.0.1', memcached_port='11211'):
    """
    Get the contents of the given index from a memcached instance.
    """

    # Setup connection to a memcache instance
    _memcached_server = memcached_ip + ':' + memcached_port
    _memcached_client = memcache.Client([_memcached_server])
    _memcached_content = _memcached_client.get(index)

    if _memcached_content is None:
        raise exceptions.DSSMemcachedIndexError()

    else:
        return _memcached_content

def set(index, data, memcached_ip='127.0.0.1', memcached_port='11211'):
    """
    Upload the data given to an index of a memcached instance.
    """

    # Setup connection to a memcache instance
    _memcached_server = memcached_ip + ':' + memcached_port
    _memcached_client = memcache.Client([_memcached_server])
    _memcached_result = _memcached_client.set(index, data)

    if _memcached_result == 0:
        raise exceptions.DSSMemcachedConnectionError()
