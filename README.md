# dynafed_storagestats.py

Module to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints and upload the
information to memcache. It leverage's UGR's connection check that it uploads
to memcache to skip any endpoints it has detected as being "Offline". (If
this information is not found, it is ignored and all endpoints are contacted)

So far it supports has been tested with:
- Azure Storage Blob
- AWS S3
- Ceph S3
- Minio S3
- DPM via WebDAV
- dCache via WebDAV

## Prerequisites (older versions might work, but these are the oldest one that have been tested):

Python >= 3.4.8

Python Modules:
- azure-storage >= 0.36.0 (pip3 install azure-storage)
- boto3 >= 1.6.1 (CentOS 7.5 does not have python3 repo modules, use pip3 install boto3)
- lxml >= 4.2.1   (CentOS 7.5 does not have python3 repo modules, use pip3 install lxml)
- python-memcached >= 1.59
- requests >= 2.12.5
- requests_aws4auth >= 0.9 (pip3 install requests-aws4auth)

### CentOS / SL 6

Python 3.4 is available from EPEL repository.

In order to install the above modules in python 3, pip3 needs to be setup. Since
it is not in the repos, run the following command:

```
sudo  python3 /usr/lib/python3.4/site-packages/easy_install.py pip
```

## Usage

This module is intended to be run periodically as a cron job, so place it in
an appropriate location and make sure the user that runs it is able to read
UGR's configuration files.

First run with the following flags:

```
./dynafed_storagestats.py -d /etc/ugr/conf.d --stdout -m -v
```

This will printout any warnings and errors as they are encountered as well as
the information obtained from each of the endpoints. If you need more information
about the errors consider adding the "--debug" flag which will print more
information at the end of each endpoint's stats. If you still need more,
change the --loglevel to INFO or DEBUG, just be warned DEBUG might print a lot
of information.

It is recommended to create a directory for the logfile, such as
"/var/log/dynafed_storagestats/dynafed_storagestats.log",as the default is
"/tmp/dynafed_storagestats.log".

When everything looks to be setup as desired, setup cron to run the following
(add any other options that make sense to your site).
```
dynafed_storagestats.py -d /etc/ugr/conf.d -m --loglevel=WARNING --logfile='/var/log/dynafed_storagestats/dynafed_storagestats.log'
```

To get help:
```
dynafed_storagestats -h

usage: dynafed_storagestats.py [-h] [-c CONFIGS_DIRECTORY] [-e ENDPOINT]
                               [--logfile LOGFILE]
                               [--loglevel {DEBUG,INFO,WARNING,ERROR}]
                               [--memhost MEMCACHED_IP]
                               [--memport MEMCACHED_PORT] [--debug] [-m]
                               [--json] [-o OUTPUT_PATH] [--plain] [--stdout]
                               [-v] [--xml]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIGS_DIRECTORY, --config CONFIGS_DIRECTORY, -d CONFIGS_DIRECTORY, --dir CONFIGS_DIRECTORY
                        Path to UGR's endpoint .conf files or direcotry. Can
                        be used multiple times. Default: '/etc/ugr/conf.d'.
  -e ENDPOINT, --endpoint ENDPOINT
                        Choose endpoint to check. If not present, all
                        endpoints will be checked.

Logging options:
  --logfile LOGFILE     Change where to ouput logs. Default:
                        /tmp/dynafed_storagestats.log
  --loglevel {DEBUG,INFO,WARNING,ERROR}
                        Set file log level. Default: WARNING.

Memcached Options:
  --memhost MEMCACHED_IP
                        IP or hostname of memcached instance. Default:
                        127.0.0.1
  --memport MEMCACHED_PORT
                        Port where memcached instances listens on. Default:
                        11211

Output options:
  --debug               Declare to enable debug output on stdout.
  -m, --memcached       Declare to enable uploading information to memcached.
  --json                Set to output json file with storage stats. !!In
                        development!!
  -o OUTPUT_PATH, --output OUTPUT_PATH
                        Directory or file to output storage stat files.
                        Defautl: /tmp/dynafed_storagestats.json
  --plain               Set to output stats to plain txt file.
  --stdout              Set to output stats on stdout.
  -v, --verbose         Show on stderr events according to loglevel.
  --xml                 Set to output xml file with StAR format. !!In
                        development!!
```
**Important Note: DEBUG level might an enormous amount of data as it will log the contents obtained from requests. In the case of the generic methods this will print all the stats for each file being parsed. It is recommended to use this level with only the endpoint one wants to troulbeshoot.**

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
Each GET request obtains 5,000 objects. Therefore 10,005 objects cost 3 GET's.

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
Each GET request obtains 1,000 objects. Therefore 2,005 objects cost 3 GET's

##### ceph-admin

Use this setting if Ceph's Admin API is to be used. The credentials of the
configured user should have the "bucket read" caps enabled.

```
locplugin.<ID>.s3.signature_ver: [s3, s3v4]
```
Most S3 endpoints should use the s3v4 signature auth version, and is used as
default, but use s3 in case is needed.

## How it works

[Simple Flowchart](doc/diagrams/dynafed_storagestats_flowchart.pdf)

When run the main function will read every configuration file in the directory
given by the user (which defaults to /etc/ugr/conf.d), and will identify all the
different endpoints with their respective settings and authorization credentials.

A python object belonging to a subclass of StorageStats, depending on the protocol
to be used, is created for each endpoint containing all the settings and
methods necessary to request and process storage stats and quota information.

Memcache is contacted to look for UGR's endpoint connection stats. Any endpoints
flagged as "Offline" here will be skipped and flagged this will be infomred in
the output. For those that are "Online", or if they could not be found to have
information will be contacted to obtain the storage stats. The information is
then stored a dictionary attribute "stats" in each object.

The stats can then be output either to a memcache instance or the STDOUT,
depending on the options chosen when invoking this script.

## Warning/Error Codes

Keyword/Setting | Status Code
--------------- | -----------
Unknown Gneral Warning/Error | 000
Unknown/Invalid setting error | 001
Setting ID Mismatch | 002
**General Settings/DAV Plugin** |
cli_certificate | 003
cli_private_key | 004
conn_timeout | 005
ssl_check | 006
Invalid URL Schema | 008
Unsupported Plugin | 009
**Azure Plugin** |
azure.key | 010
**S3 Plugin** |
s3.alternate | 020
s3.priv_key | 021
s3.pub_key | 022
s3.region | 023
s3.signature_ver | 024
**Storage Stats Scripts Settings** |
storagestats.api | 070
storagestats.quota | 071
**Memcached Warning/Errors** |
Unknown | 080
Memcached Connection | 081
Memcached Index | 082
**StorageStats Connection Warning/Errors** |
Unknown | 090
Client Certificate Path | 091
Server SSL Validation | 092
Boto Param Validatoin Error | 095
RFC4331 DAV Quota Method Not Supported | 096
No Quota Given by Endpoint | 098
Ceph S3 Bucket Quota Disabled | 099
Connection Error | 400
Element/Bucket/Blob not found | 404
Endpoint Offline | 503
