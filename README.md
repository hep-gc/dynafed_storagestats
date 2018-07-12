# ugrstoragestats.py

Module to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints and upload the
information to memcahe.

While it works under Python2 it is recommended to be run with Python3.

So far it supports has been tested with:
AWS S3
Ceph S3
Minio S3
DPM via WebDAV
dCache via WebDAV

## Prerequisites:
Python Modules:
- boto3
- lxml
- memcache
- requests
- requests_aws4auth

## Usage

This module is intended to be run periodically as a cron job, so place it in
an appropriate location for this.

First run with the following flags:

```
./ugrstoragestats.py -d /etc/ugr/conf.d --stdout -m --debug
```

This will give is the best way to test if there are any options missing in the
configuration or errors contacting or obtaining the information from each endpoint.
It will also show the information uploaded to memcached and if there were issues.

When everything is in place and working as desired, the following command is
what would normally used with cron:

```
./ugrstoragestats.py -d /etc/ugr/conf.d -m
```

To get help:
```
ugrstoragestats -h

Usage: ugrstoragestats.py [options]

Options:
  -h, --help            show this help message and exit
  -d CONFIGS_DIRECTORY, --dir=CONFIGS_DIRECTORY
                        Path to UGR's endpoint .conf files.

  Memcached options:
    --memhost=MEMCACHED_IP
                        IP or hostname of memcached instance. Default:
                        127.0.0.1
    --memport=MEMCACHED_PORT
                        Port where memcached instances listens on. Default:
                        11211

  Output options:
    --debug             Declare to enable debug output on stdout.
    -m, --memcached     Declare to enable uploading information to memcached.
    --stdout            Set to output stats on stdout. If no other output
                        option is set, this is enabled by default.
```


## Endpoints Configuration

In order to use the correct methods for each storage type some options should
be added to the endpoints.conf configuration file:

### General

```
locplugin.<ID>.storagestats.quota: [api, 1b|mb|gb|tb|pb|mib|gib|tib|pib]
```

If this option is missing, the script will try to get the quota from the endpoint
using the relevant API. Failing this, a default quota of 1TB will used.

##### api
Will try to obtain the quota from the storage endpoint. If that fails a default
of 1TB will be used.

##### bytes
The quota can be specify in bytes, megabytes, mebibytes, etc. Lower or uppercase.

### S3

```
locplugin.<ID>.storagestats.api: [generic, ceph-admin]
```

##### generic

This option will list all objects behind the bucket and add the individual
sizes.

##### ceph-admin

Use this option if Ceph's Admin API is to be used. The credentials of the
configured user should have the "bucket read" caps enabled.

```
locplugin.<ID>.s3.signature_ver: [s3, s3v4]
```
Most S3 endpoints should use the s3v4 signature auth version, and is used as
default, but use s3 in case is needed.

## How it works

When run the main function will read every configuration file in the directory
given by the user (which defaults to /etc/ugr/conf.d), and will identify all the
different endpoints with their respective options and authorization credentials.

A python object belonging to a subclass of StorageStats, depending on the protocol
to be used, is created for each endpoint containing all the information and
methods necessary to request and process storage stats and quota information.

The gathered information can then be output either to a memcache instance or
the STDOUT.
