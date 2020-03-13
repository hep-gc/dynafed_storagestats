#!/usr/bin/env python3

"""Functions to generate storage reports."""

import json
import subprocess
import time


#############
# Functions #
#############

def create_wlcg_storage_report(dynafed_endpoints, schema, output='/tmp'):
    """Creates json file according to the WLCG storage report format.

    """

    _output_file = output + '/space-usage.json'

    # Getting current timestamp
    NOW = int(time.time())

    # Getting Dynafed version if not found in the schema:
    if not schema['storageservice']['implementationversion']:
        _process = subprocess.Popen(
            ['rpm', '--queryformat', '%{VERSION}', '-q', 'dynafed'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        _stdout, _stderr = _process.communicate()
        if _stdout is not None:
            _stdout = _stdout.decode("utf-8")

        if _stderr is not None:
            _stderr = _stderr.decode("utf-8")

        _dyanfed_version = _stdout

        schema['storageservice'].update(
            {
                "implementationversion": _dyanfed_version
            }

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
                    bytes_used += _endpoint.stats['bytesused']
                    total_size += _endpoint.stats['quota']

        _schema_storage_share.update(
            {
                "totalsize": total_size,
                "timestamp": NOW,
                "usedsize": bytes_used,
            }
        )
        del _schema_storage_share['dynafedendpoints']

    # Create the json structure
    # skeleton = {
    #     'storage_service': {
    #         'latestupdate': NOW,
    #         'name': dynafed_hostname,
    #         'storage_shares': storage_shares
    #     }
    # }

    # Ouptut json to file.
    with open(_output_file, 'w') as json_file:
        json.dump(schema, json_file, indent=4, sort_keys=True)
