# ugrstoragestats.py

Module to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints.

## Prerequisites:
Python Modules:
- boto3
- lxml
- memcache
- requests
- requests_aws4auth

## Usage
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

**Examples**
For testing that it all works, including memcached, this will print out warnings
for missing options and the stats gathered, as well as memcached idices:
```
./ugrstoragestats.py -d /etc/ugr/conf.d --stdout -m --debug
```

For testing only stats with warnings:
```
./ugrstoragestats.py -d /etc/ugr/conf.d --stdout
```

To output stats only to memcached:
```
./ugrstoragestats.py -d /etc/ugr/conf.d -m
```


## Endpoints Configuration

In order to use the correct methods for each storage type some options should
be added to the endpoints.conf configuration file:

### General

```
locplugin.<ID>.api: [api, 1b|mb|gb|tb|pb|mib|gib|tib|pib]
```

**api**
Will try to obtain the quota from the storage endpoint. If that fails a default
of 1TB will be used.

**bytes**
The quota can be specify in bytes, megabytes, mebibytes, etc. Lower or uppercase.

### S3
```
locplugin.<ID>.s3.api: [generic, ceph-admin]
```

**generic**
This option will list all objects behind the bucket and add the individual
sizes.

**ceph-admin**

Use this option if Ceph's Admin API is to be used. The credentials of the
configured user should have the "bucket read" caps enabled.

```
locplugin.<ID>.s3.signature_ver: [s3, s3v4]
```
Most S3 endpoints should use the s3v4 signature auth version, and is used as
default, but use s3 in case is needed.
