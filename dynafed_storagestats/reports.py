#!/usr/bin/env python3

"""Functions to generate storage reports."""

import json
import time

from dynafed_storagestats import helpers

#############
# Functions #
#############

def create_wlcg_storage_report(dynafed_endpoints, schema, output='/tmp'):
    """Creates json file according to the WLCG storage report format.

    """

    _output_file = output + '/space-usage.json'

    # Getting current timestamp
    NOW = int(time.time())

    # Getting Dynafed version.
    if 'implementationversion' not in schema['storageservice']:

        _dyanfed_version = helpers.get_dynafed_version()

        schema['storageservice'].update(
            {
                "implementationversion": _dyanfed_version
            }
        )

    # Add info to the storageservice block:
    schema['storageservice'].update(
        {
            "implementation": "dynafed",
            "latestupdate": NOW
        }
    )
    # Calculate the totals space and used space for each of the dynafed endpoints
    # under each storage share.
    for _schema_storage_share in schema['storageservice']['storageshares']:
        bytes_used = 0
        total_size = 0
        for _dynafed_endpoint in _schema_storage_share['dynafedendpoints']:

            _id = _dynafed_endpoint

            for _endpoint in dynafed_endpoints:
                if _endpoint.id == _id:
                    bytes_used += int(_endpoint.stats['bytesused'])
                    total_size += int(_endpoint.stats['quota'])

        _schema_storage_share.update(
            {
                "totalsize": total_size,
                "timestamp": NOW,
                "usedsize": bytes_used,
            }
        )
        del _schema_storage_share['dynafedendpoints']

    # Ouptut json to file.
    with open(_output_file, 'w') as json_file:
        json.dump(schema, json_file, indent=4, sort_keys=True)
