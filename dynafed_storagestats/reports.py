#!/usr/bin/env python3

"""Functions to generate storage reports."""

import logging
import yaml
import sys

def create_wlcg_storage_report(args):
    """Creates json file according to the WLCG storage report format.

    """
    ############# Creating loggers ################
    _logger = logging.getLogger(__name__)
    ###############################################

    try:
        _logger.info(
            "Trying to open schema file: %s",
            args.schema
        )

        with open(args.schema, 'r') as _stream:
            try:
                _schema = yaml.safe_load(_stream)
            except yaml.YAMLError as ERROR:
                _logger.critical(
                    "Failed to read YAML stream from file: %s. %s",
                    args.schema,
                    ERROR
                )
                print(
                    "[CRITICAL]Failed to read YAML stream from file: %s. %s" % (
                    args.schema,
                    ERROR
                    )
                )
                sys.exit(1)

    except IOError as ERROR:
        _logger.critical(
            "%s",
            ERROR
        )
        print("[CRITICAL]%s" % (ERROR))
        sys.exit(1)

    else:
