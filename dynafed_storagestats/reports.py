#!/usr/bin/env python3

"""Functions to generate storage reports."""

import json
import logging
import time
import sys

#############
# Functions #
#############

def create_wlcg_storage_report(dynafed_endpoints, schema, output='/tmp'):
    """Creates json file according to the WLCG storage report format.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    _output_file = output + '/space-usage.json'

    # Getting current timestamp
    NOW = int(time.time())

    # Calculate the totals space and used space for each of the dynafed endpoints
    # under each storage share.
    for _schema_storage_service in schema['storageservice']:
        for _schema_storage_share in _schema_storage_service['storageshares']:
            bytes_used = 0
            total_size = 0
            for _dynafed_endpoint in _schema_storage_share['dynafedendpoints']:

                _id = _dynafed_endpoint

                for _endpoint in dynafed_endpoints:
                    if _endpoint.id == _id:
                        bytes_used +=_endpoint.stats['bytesused']
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
