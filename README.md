# dynafed_storagestats.py

Module to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints and upload the
information to memcahe.

Tested with Python >= 3.4.8

Might work with >= 2.7.5 but is not actively tested.

So far it supports has been tested with:
Azure Storage Blob
AWS S3
Ceph S3
Minio S3
DPM via WebDAV
dCache via WebDAV

## Prerequisites (older versions might work, but these are the oldest one that have been tested):
Python Modules:
- azure-storage >= 0.36.0 (pip3 install azure-storage)
- boto3 >= 1.6.1 (CentOS 7.5 does not have python3 repo modules, use pip3 install boto3)
- lxml >= 4.2.1   (CentOS 7.5 does not have python3 repo modules, use pip3 install lxml)
- python-memcache >= 1.59
- requests >= 2.12.5
- requests_aws4auth >= 0.9 (pip3 install requests-aws4auth)

## Usage

This module is intended to be run periodically as a cron job, so place it in
an appropriate location for this.

First run with the following flags:

```
./dynafed_storagestats.py -d /etc/ugr/conf.d --stdout -m --debug
```

This will give is the best way to test if there are any settings missing in the
configuration or errors contacting or obtaining the information from each endpoint.
It will also show the information uploaded to memcached and if there were issues.

When everything is in place and working as desired, the following command is
what would normally used with cron, which will create a log file at
/tmp/dynafed_storagestats.log with WARNING level:

```
./dynafed_storagestats.py -d /etc/ugr/conf.d -m
```

To get help:
```
dynafed_storagestats -h

Usage: dynafed_storagestats.py [settings]

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
    --stdout            Set to output stats on stdout.
    --xml               Set to output xml file with StAR format.
    --logfile=LOGFILE   Change where to ouput logs. Default:
                        /tmp/dynafed_storagestats.log
    --loglevel=LOGLEVEL
                        Set file log level. Default: WARNING. Valid: DEBUG,
                        INFO, WARNING, ERROR
```
**Important Note: DEBUG level might an enormous amount of data as it will log the contents obtained from requests. In the case of the generic methods this will print all the stats for each file being parsed. It is recommended to use this level with a file with only the endpoint one wants to troulbeshoot.**

## Endpoints Configuration

In order to use the correct methods for each storage type some settings should
be added to the endpoints.conf configuration file.

### Known issues



### General

```
locplugin.<ID>.storagestats.quota: [api, 1b|mb|gb|tb|pb|mib|gib|tib|pib]
```

If this setting is missing, the script will try to get the quota from the endpoint
using the relevant API. Failing this, a default quota of 1TB will used.

##### api
Will try to obtain the quota from the storage endpoint. If that fails a default
of 1TB will be used.

##### bytes
The quota can be specify in bytes, megabytes, mebibytes, etc. Lower or uppercase.

### Azure

```
locplugin.<ID>.storagestats.api: [generic]
```

##### generic

This setting will list all objects in a blob container and add the individual
sizes.

### DAV/HTTP

```
locplugin.<ID>.storagestats.api: [generic, rfc4331]
```

##### generic

This setting will list all objects behind the endpoint and add the individual
sizes. For this method to recrusivley get all objects, the DAV server needs
to header "Depth" with attribute "infinity". This is not recommended as
it is an expensive method, can use a lot of memory and is suceptible to
denial of service. Therefore this setting should be avoided if possible in
favor of rfc4331

##### rfc4331

This setting will query the DAV server according to [RFC4331](https://tools.ietf.org/html/rfc4331).

### S3

```
locplugin.<ID>.storagestats.api: [generic, ceph-admin]
```

##### generic

This setting will list all objects behind the bucket and add the individual
sizes.

##### ceph-admin

Use this setting if Ceph's Admin API is to be used. The credentials of the
configured user should have the "bucket read" caps enabled.

```
locplugin.<ID>.s3.signature_ver: [s3, s3v4]
```
Most S3 endpoints should use the s3v4 signature auth version, and is used as
default, but use s3 in case is needed.

## How it works

When run the main function will read every configuration file in the directory
given by the user (which defaults to /etc/ugr/conf.d), and will identify all the
different endpoints with their respective settings and authorization credentials.

A python object belonging to a subclass of StorageStats, depending on the protocol
to be used, is created for each endpoint containing all the information and
methods necessary to request and process storage stats and quota information.

The gathered information can then be output either to a memcache instance or
the STDOUT.
