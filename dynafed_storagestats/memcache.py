"""Functions to deal with obtaining and placing data into memcache."""

import memcache

import dynafed_storagestats.exceptions


#############
# Functions #
#############

def get(index, memcached_ip='127.0.0.1', memcached_port='11211'):
    """Get the contents of the given index from a memcached instance.

    Arguments:
    index -- String defining index to read from in memcache.
    memcached_ip   -- memcached instance IP.
    memcahced_port -- memcached instance Port.

    Returns:
    String containing contents from memcached index, if it exists.

    """
    # Setup connection to a memcache instance
    _memcached_server = memcached_ip + ':' + memcached_port
    _memcached_client = memcache.Client([_memcached_server])
    _memcached_content = _memcached_client.get(index)

    if _memcached_content is None:
        raise dynafed_storagestats.exceptions.MemcachedIndexError()

    else:
        return _memcached_content


def set(index, data, memcached_ip='127.0.0.1', memcached_port='11211', ttl=3600):
    """Upload the data given to an index of a memcached instance.

    Arguments:
    index -- String defining index to write to in memcache.
    data  -- String to set into specified index.
    memcached_ip   -- memcached instance IP.
    memcahced_port -- memcached instance Port.
    ttl -- Time to live expiry of indexed data. Default of 1 hour.

    """
    # Setup connection to a memcache instance
    _memcached_server = memcached_ip + ':' + memcached_port
    _memcached_client = memcache.Client([_memcached_server])
    _memcached_result = _memcached_client.set(index, data, time=ttl)

    if _memcached_result == 0:
        raise dynafed_storagestats.exceptions.MemcachedConnectionError()
