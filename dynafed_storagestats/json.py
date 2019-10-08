"""Functions to deal with the formatting and handling  of JSON data."""

import datetime
import json


#############
# Functions #
#############

def format_wlcg(storage_endpoints, hostname="localhost"):
    """Create JSON file representing Dynafed site storage stats in WLCG format.

    Creates a JSON file containing the storage stats information for each
    StorageEndpoint and StorageShare. Tries to adhere to the format given by
    the WLCG Accounting Task Force used for Storage Space Accounting:
    https://twiki.cern.ch/twiki/bin/view/LCG/StorageSpaceAccounting

    Arguments:
    storage_endpoints -- List of dynafed_storagestats StorageEndpoint objects.
    hostname -- string defining Dynafed host.

    Returns:
    String in JSON format.

    """
    # Create the json structure in python terms
    _dynafed_usedsize = 0
    _dynafed_totalsize = 0
    _skeleton = {}
    _storageservice = {}
    _storageendpoints = []
    _storageshares = []
    _shares = {}

    for _storage_endpoint in storage_endpoints:
        for _storage_share in _storage_endpoint.storage_shares:
            _dynafed_usedsize += _storage_share.stats['bytesused']
            _dynafed_totalsize += _storage_share.stats['quota']
            _path = _storage_share.plugin_settings['xlatepfx'].split()[0]
            _storageendpoint = {
                "name": _storage_share.id,
                # "id": 'tbd',
                "endpointurl": _storage_share.uri['url'],
                "interfacetype": _storage_share.storageprotocol,
                "timestamp": _storage_share.stats['starttime'],
                "storage_sharecapacity": {
                    "totalsize": _storage_share.stats['quota'],
                    "usedsize": _storage_share.stats['bytesused'],
                    "numberoffiles": _storage_share.stats['filecount'],
                },
                "assignedshares": [_path],
            }
            _storageendpoints.append(_storageendpoint)

            if _path in _shares:
                _shares[_path]['totalsize'] += _storage_share.stats['quota']
                _shares[_path]['usedsize'] += _storage_share.stats['bytesused']
                _shares[_path]['assignedendpoints'].append(_storage_share.id)

            else:
                _shares.setdefault(_path, {})
                _shares[_path] = {
                    "totalsize": _storage_share.stats['quota'],
                    "usedsize": _storage_share.stats['bytesused'],
                    "path": _path,
                    "assignedendpoints": [_storage_share.id]
                }

    for _share in _shares:
        _storageshares.append(_shares[_share])

    _storageservice = {
        "name": hostname,
        # 'id': "tbd",
        # 'servicetype': "tbd",
        "implementation": "dynafed",
        # 'implementationversion': "tbd",
        "latestupdate": int(datetime.datetime.now().timestamp()),
        "storageservicecapacity": {
            "totalsize": _dynafed_totalsize,
            "usedsize": _dynafed_usedsize,
        },
        "storageendpoints": _storageendpoints,
        "storageshares": _storageshares,
    }
    _skeleton = {"storageservice": _storageservice}

    return json.dumps(_skeleton, indent=4)
