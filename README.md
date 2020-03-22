# dynafed_storagestats

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

## Installation

Linux:

```sh
pip3 install dynafed-storagestats
```
This will install all necessary dependencies and create the executable
'/usr/bin/dynafed-storage'

### CentOS / SL 6

Python 3.4 is available from EPEL repository.

In order to install the above modules in python 3, pip3 needs to be setup. Since
it is not in the repos, run the following command:

```bash
sudo  python3 /usr/lib/python3.4/site-packages/easy_install.py pip
```
## Known issues
-

## Usage
Make sure the user that runs it is able to read UGR's configuration files.

```bash
dynafed-storage -h
usage: dynafed-storage [-h] {reports,stats} ...

positional arguments:
  {reports,stats}
    reports        In development
    stats          Obtain and output storage stats.

optional arguments:
  -h, --help       show this help message and exit

```
#### Sub-commands
##### Checksums
This sub-command is intended to be called by Dynafed's external scripts as
specified in the option 'checksumcalc' to obtain and add checksum information
files or objects checksum information, specially for cloud based storage
endpoints that do not support the usual grid tool requests.

The checksums sub-command has two sub-commands itself:

###### *get*
**Currently only S3 endpoints are supported.**

A client gives the URL of the object (not the Dynafed URL, but the actual SE
URL) and the type of checksum hash to obtain. If this information is
found, it will be printed out to stdout, if not, 'None' will be printed out.

In its simplest form it is called with all three required arguments:

```bash
dynafed-storage checksums get -e [ENDPOINT_ID] -u [URL] -t [HASH_TYPE]
```

A more complex example specifying configuration file path, logging file and level,
and verbosity:

```bash
dynafed-storage checksums get -v -c /etc/ugr/conf.d --loglevel=WARNING --logfile='/var/log/dynafed_storagestats/dynafed_storagestats.log' -e [Endpoint ID] -u [URL] -t [HASH_TYPE]
```

Help:

```bash
dynafed-storage checksums get -h
usage: dynafed-storage checksums get [-h] [-c [CONFIG_PATH [CONFIG_PATH ...]]]
                                     [-f] [-v] [-e ENDPOINT] [-t HASH_TYPE]
                                     [-u URL] [--logfile LOGFILE]
                                     [--logid LOGID]
                                     [--loglevel {DEBUG,INFO,WARNING,ERROR}]
                                     [--stdout]

optional arguments:
  -h, --help            show this help message and exit
  -c [CONFIG_PATH [CONFIG_PATH ...]], --config [CONFIG_PATH [CONFIG_PATH ...]]
                        Path to UGRs endpoint .conf files or directories.
                        Accepts any number of arguments. Default:
                        '/etc/ugr/conf.d'.
  -f, --force           Force command execution.
  -v, --verbose         Show on stderr events according to loglevel.

Checksum options. Required!:
  -e ENDPOINT, --endpoint ENDPOINT
                        Choose endpoint containing desired object. Required.
  -t HASH_TYPE, --hash_type HASH_TYPE
                        Type of checksum hash. ['adler32', md5] Required.
  -u URL, --url URL     URL of object/file to request checksum of. Required.

Logging options:
  --logfile LOGFILE     Set logfiles path. Default:
                        /tmp/dynafed_storagestats.log
  --logid LOGID         Add this log id to every log line.
  --loglevel {DEBUG,INFO,WARNING,ERROR}
                        Set log output level. Default: WARNING.

Output options:
  --stdout              Set to output stats on stdout.
```

###### *put*
**Currently only S3 endpoints are supported.**

A client gives the URL of the object (not the Dynafed URL, but the actual SE
URL), the checksum, and the type of checksum hash to add this information
to the object. Nothing is returned unless the process encounters errors.

In its simplest form it is called with all four required arguments:

```bash
dynafed-storage checksums put -e [ENDPOINT_ID] -u [URL] -t [HASH_TYPE] --checksum [CHECKSUM]
```

A more complex example specifying configuration file path, logging file and level,
and verbosity:

```bash
dynafed-storage checksums put -v -c /etc/ugr/conf.d --loglevel=WARNING --logfile='/var/log/dynafed_storagestats/dynafed_storagestats.log' -e [ENDPOINT_ID] -u [URL] -t [HASH_TYPE] --checksum [CHECKSUM]
```

Help:

```bash
dynafed-storage checksums put -h
usage: dynafed-storage checksums put [-h] [-c [CONFIG_PATH [CONFIG_PATH ...]]]
                                     [-f] [-v] [--checksum CHECKSUM]
                                     [-e ENDPOINT] [-t HASH_TYPE] [-u URL]
                                     [--logfile LOGFILE] [--logid LOGID]
                                     [--loglevel {DEBUG,INFO,WARNING,ERROR}]
                                     [--stdout]

optional arguments:
  -h, --help            show this help message and exit
  -c [CONFIG_PATH [CONFIG_PATH ...]], --config [CONFIG_PATH [CONFIG_PATH ...]]
                        Path to UGRs endpoint .conf files or directories.
                        Accepts any number of arguments. Default:
                        '/etc/ugr/conf.d'.
  -f, --force           Force command execution.
  -v, --verbose         Show on stderr events according to loglevel.

Checksum options. Required!:
  --checksum CHECKSUM   String with checksum to set. ['adler32', md5] Required
  -e ENDPOINT, --endpoint ENDPOINT
                        Choose endpoint containing desired object. Required.
  -t HASH_TYPE, --hash_type HASH_TYPE
                        Type of checksum hash. ['adler32', md5] Required.
  -u URL, --url URL     URL of object/file to request checksum of. Required.

Logging options:
  --logfile LOGFILE     Set logfiles path. Default:
                        /tmp/dynafed_storagestats.log
  --logid LOGID         Add this log id to every log line.
  --loglevel {DEBUG,INFO,WARNING,ERROR}
                        Set log output level. Default: WARNING.

Output options:
  --stdout              Set to output stats on stdout.                           
```

---

##### Reports

The reports sub-command has two sub-commands itself:

###### *filelist*
**Note: Only works with Azure and S3 endpoints**

This sub-command is intended to be used to obtain file reports from the storage
endpoints. At this time, what is being developed is to be able to create
file dumps with the intention of using the for Rucio's integrity checks for
cloud based storage such as S3 and Azure since other grid storage solutions like
dCache and DPM have their own tools.

For more information on this file dumps: [DDMDarkDataAndLostFiles](https://twiki.cern.ch/twiki/bin/view/AtlasComputing/DDMDarkDataAndLostFiles)

Usage example:
This will create a file at /tmp/ containing a list of files under the 'rucio'
prefix that are a day older from endpoints 'entpoint1' and 'endpoint2'. The
filename is the endpoint's ID name in the endpoints.conf file.

```bash
dynafed-storage reports filelist -c /etc/ugr/conf.d -o /tmp --rucio -e endpoint1 endpoint2
```

**reports filelist help:**
```bash
dynafed-storage reports filelist -h
usage: dynafed-storage reports filelist [-h]
                                        [-c [CONFIG_PATH [CONFIG_PATH ...]]]
                                        [-f] [-v]
                                        [-e [ENDPOINT [ENDPOINT ...]]]
                                        [--logfile LOGFILE] [--logid LOGID]
                                        [--loglevel {DEBUG,INFO,WARNING,ERROR}]
                                        [--delta DELTA] [--rucio]
                                        [-o OUTPUT_PATH] [-p PREFIX]

optional arguments:
  -h, --help            show this help message and exit
  -c [CONFIG_PATH [CONFIG_PATH ...]], --config [CONFIG_PATH [CONFIG_PATH ...]]
                        Path to UGRs endpoint .conf files or directories.
                        Accepts any number of arguments. Default:
                        '/etc/ugr/conf.d'.
  -f, --force           Force command execution.
  -v, --verbose         Show on stderr events according to loglevel.
  -e [ENDPOINT [ENDPOINT ...]], --endpoint [ENDPOINT [ENDPOINT ...]]
                        Choose endpoint(s) to check. Accepts any number of
                        arguments. If not present, all endpoints will be
                        checked.

Logging options:
  --logfile LOGFILE     Set logfiles path. Default:
                        /tmp/dynafed_storagestats.log
  --logid LOGID         Add this log id to every log line.
  --loglevel {DEBUG,INFO,WARNING,ERROR}
                        Set log output level. Default: WARNING.

Reports options:
  --delta DELTA         Mask for Last Modified Date of files. Integer in days.
                        Default: 1
  --rucio               Use to create rucio file dumps for consitency checks.
                        Same as: --delta 1 --prefix rucio

Output options:
  -o OUTPUT_PATH, --output-dir OUTPUT_PATH
                        Set output directory. Default: '.'
  -p PREFIX, --path PREFIX, --prefix PREFIX
                        Set the prefix/path from where to start the recursive
                        list. The prefix is excluded from the resulting paths.
                        Default: ''
```
**Important Note: DEBUG level might print an enormous amount of data as it will
log the contents obtained from requests. In the case of the generic methods this
will print all the stats for each file being parsed. It is recommended to use
this level with only the endpoint one wants to troubleshoot.**

###### *storage*

The purpose of this sub-command is to create storage accounting reports according
to different formats that experiments might require.

At the moment only WLCG's JSON storage accounting file is available.

In order to create it, the user will have to create a 'site-schema' YAML file
containing the site information. Please check in the 'samples' folder for
examples on how to create these file. The '-s/--schema' flag is required and
should point to this site-schema file.

Usage example:
This will create file '/tmp/space-usage.json'

```bash
dynafed-storage reports storage --wlcg -c /etc/ugr/conf.d -s wlcg-schema.yml -o /tmp
```

**reports storage help:**
```bash
usage: dynafed-storage reports storage [-h]
                                       [-c [CONFIG_PATH [CONFIG_PATH ...]]]
                                       [-f] [-v]
                                       [-e [ENDPOINT [ENDPOINT ...]]]
                                       [--logfile LOGFILE] [--logid LOGID]
                                       [--loglevel {DEBUG,INFO,WARNING,ERROR}]
                                       [--memhost MEMCACHED_IP]
                                       [--memport MEMCACHED_PORT] [-s SCHEMA]
                                       [--wlcg] [-o OUTPUT_PATH]

optional arguments:
  -h, --help            show this help message and exit
  -c [CONFIG_PATH [CONFIG_PATH ...]], --config [CONFIG_PATH [CONFIG_PATH ...]]
                        Path to UGRs endpoint .conf files or directories.
                        Accepts any number of arguments. Default:
                        '/etc/ugr/conf.d'.
  -f, --force           Force command execution.
  -v, --verbose         Show on stderr events according to loglevel.
  -e [ENDPOINT [ENDPOINT ...]], --endpoint [ENDPOINT [ENDPOINT ...]]
                        Choose endpoint(s) to check. Accepts any number of
                        arguments. If not present, all endpoints will be
                        checked.

Logging options:
  --logfile LOGFILE     Set logfiles path. Default:
                        /tmp/dynafed_storagestats.log
  --logid LOGID         Add this log id to every log line.
  --loglevel {DEBUG,INFO,WARNING,ERROR}
                        Set log output level. Default: WARNING.

Memcached Options:
  --memhost MEMCACHED_IP
                        IP or hostname of memcached instance.Default:
                        127.0.0.1
  --memport MEMCACHED_PORT
                        Port of memcached instance. Default: 11211

Reports options:
  -s SCHEMA, --schema SCHEMA
                        YAML file containing site schema. Required.
  --wlcg                Produces WLCG JSON output file. Requires setup file.

Output options:
  -o OUTPUT_PATH, --output-dir OUTPUT_PATH
                        Set output directory. Default: '.'
```

---

##### stats

This sub-command is intended to be run periodically as a cron job in order to
upload the stats into memcached so that Dynafed can use it to be aware of the
storage capacity of the storage endpoints.

It contacts each storage endpoint, obtains available stats and outputs them
according settings.

First run with the following flags:

```bash
dynafed-storage stats -v -c /etc/ugr/conf.d --stdout -m
```

This will printout any warnings and errors as they are encountered as well as
the information obtained from each of the endpoints. If you need more information
about the errors consider adding the "--debug" flag which will print more
information at the end of each endpoint's stats. If you still need more,
change the --loglevel to INFO or DEBUG, just be warned DEBUG might print a lot
of information.

It is recommended to create a directory for the logfile, such as
"/var/log/dynafed_storagestats/dynafed_storagestats.log", as the default is
"/tmp/dynafed_storagestats.log".

When everything looks to be setup as desired, setup cron to run the following
(add any other options that make sense to your site).

```bash
dynafed-storage stats -c /etc/ugr/conf.d -m --loglevel=WARNING --logfile='/var/log/dynafed_storagestats/dynafed_storagestats.log'
```

If instead of checking all configured endpoints, specific endpoint ID's can be
specified with the "-e" flag:

```bash
dynafed-storage stats -c /etc/ugr/conf.d -m -e endpoint1 endpoint2
```

**stats help:**
```bash
usage: dynafed-storage stats [-h] [-c [CONFIG_PATH [CONFIG_PATH ...]]] [-f]
                             [-v] [-e [ENDPOINT [ENDPOINT ...]]]
                             [--logfile LOGFILE] [--logid LOGID]
                             [--loglevel {DEBUG,INFO,WARNING,ERROR}]
                             [--memhost MEMCACHED_IP]
                             [--memport MEMCACHED_PORT] [--debug] [-m]
                             [-o OUTPUT_PATH] [-p [TO_PLAINTEXT]] [--stdout]

optional arguments:
  -h, --help            show this help message and exit
  -c [CONFIG_PATH [CONFIG_PATH ...]], --config [CONFIG_PATH [CONFIG_PATH ...]]
                        Path to UGRs endpoint .conf files or directories.
                        Accepts any number of arguments. Default:
                        '/etc/ugr/conf.d'.
  -f, --force           Force command execution.
  -v, --verbose         Show on stderr events according to loglevel.
  -e [ENDPOINT [ENDPOINT ...]], --endpoint [ENDPOINT [ENDPOINT ...]]
                        Choose endpoint(s) to check. Accepts any number of
                        arguments. If not present, all endpoints will be
                        checked.

Logging options:
  --logfile LOGFILE     Set logfiles path. Default:
                        /tmp/dynafed_storagestats.log
  --logid LOGID         Add this log id to every log line.
  --loglevel {DEBUG,INFO,WARNING,ERROR}
                        Set log output level. Default: WARNING.

Memcached Options:
  --memhost MEMCACHED_IP
                        IP or hostname of memcached instance.Default:
                        127.0.0.1
  --memport MEMCACHED_PORT
                        Port of memcached instance. Default: 11211

Output options:
  --debug               Declare to enable debug output on stdout.
  -m, --memcached       Declare to enable uploading storage stats to
                        memcached.
  -o OUTPUT_PATH, --output-dir OUTPUT_PATH
                        Set output directory for flags -j, -x and -p. Default:
                        '.'
  -p [TO_PLAINTEXT], --plain [TO_PLAINTEXT]
                        Set to output stats to plain txt file. Add argument to
                        set filename.Default: dynafed_storagestats.txt
  --stdout              Set to output stats on stdout.
```

## Endpoints Configuration

In order to use the correct methods for each storage type some settings should
be added to the endpoints.conf configuration file.

### General

```
locplugin.<ID>.storagestats.quota: [api, 1b|mb|gb|tb|pb|mib|gib|tib|pib]
```

If this setting is missing, the script will try to get the quota from the endpoint
using the relevant API. Failing this, a default quota of 1TB will used.

##### api
Will try to obtain the quota from the storage endpoint. If that fails a default
of 1TB will be used.

##### (bytes)
The quota can be specify in bytes, megabytes, mebibytes, etc. Lower or uppercase.

```
locplugin.<ID>.storagestats.frequency: [ 600 ]
```

This setting tells the script the number of seconds to wait before checking the endpoint again according to the timestamp of the last check stored in memcache. The default is 10 minutes (600 seconds).

### Azure

```
locplugin.<ID>.storagestats.api: [list-blobs]
```

##### list-blobs (deprecated: generic)

This setting will list all objects in a blob container and add the individual
sizes.
Each GET request obtains 5,000 objects. Therefore 10,005 objects cost 3 GET's.

### DAV/HTTP

```
locplugin.<ID>.storagestats.api: [list-files, rfc4331]
```

##### list-files (deprecated: generic)

This setting will list all objects behind the endpoint and add the individual
sizes. For this method to recursively get all objects, the DAV server needs
to header "Depth" with attribute "infinity". This is not recommended as
it is an expensive method, can use a lot of memory and is susceptible to
denial of service. Therefore this setting should be avoided if possible in
favour of rfc4331

##### rfc4331

This setting will query the DAV server according to [RFC4331](https://tools.ietf.org/html/rfc4331).

### S3

```
locplugin.<ID>.storagestats.api: [list-objects, ceph-admin, cloudwatch]
```

##### list-objects (deprecated: generic)

This setting will list all objects behind the bucket and add the individual
sizes.
Each GET request obtains 1,000 objects. Therefore 2,005 objects cost 3 GET's.

##### ceph-admin

Use this setting if Ceph's Admin API is to be used. The credentials of the
configured user should have the "bucket read" caps enabled.

##### cloudwatch

For AWS. Configure the cloudwatch metrics BucketSizeBytes and  NumberOfObjects.
This setting will poll these two. These metrics are updated daily at 00:00 UTC.
Read AWS's documentation for more information about Cloudwatch.

```
locplugin.<ID>.s3.signature_ver: [s3, s3v4]
```
Most S3 endpoints should use the s3v4 signature auth version, and is used as
default, but use s3 in case is needed.

##### minio_prometheus

Minio endpoints can expose metrics with a Prometheus client. The URL's path
is assumed to be "/minio/prometheus/metrics". This replaces any existing path
in the endpoint's configuration URL.

These are the metrics extracted:
  - minio_disk_storage_available_bytes
  - minio_disk_storage_total_bytes
  - minio_disk_storage_used_bytes

The quota/total_bytes can be overridden in the configuration file.

## How it works

[Simple Flowchart](doc/diagrams/dynafed_storagestats_flowchart.pdf)

When run the main function will read every configuration file in the directory
given by the user (which defaults to /etc/ugr/conf.d), and will identify all the
different endpoints with their respective settings and authorization credentials.

A python object belonging to a subclass of StorageStats, depending on the protocol
to be used, is created for each endpoint containing all the settings and
methods necessary to request and process storage stats and quota information.

Memcache is contacted to look for UGR's endpoint connection stats. Any endpoints
flagged as "Offline" here will be skipped and flagged this will be informed in
the output. For those that are "Online", or if they could not be found to have
information will be contacted to obtain the storage stats. The information is
then stored a dictionary attribute "stats" in each object.

The stats can then be output either to a memcache instance or the STDOUT,
depending on the options chosen when invoking this script.

## Warning/Error Codes

Keyword/Setting | Status Code
--------------- | -----------
Unknown General Warning/Error | 000
Unknown/Invalid setting error | 001
No Config Files / ID Mismatch | 002
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
storagestats.frequency | 072
**Memcached Warning/Errors** |
Unknown | 080
Memcached Connection | 081
Memcached Index | 082
**StorageStats Connection Warning/Errors** |
Unknown | 090
Client Certificate Path | 091
Server SSL Validation | 092
Boto Param Validation Error | 095
RFC4331 DAV Quota Method Not Supported | 096
No Quota Given by Endpoint | 098
Ceph S3 Bucket Quota Disabled | 099
Connection Error | 400
Element/Bucket/Blob not found | 404
Endpoint Offline | 503

## Development setup

To install in "edit" mode, add the "-e" flag to pip3. This installs the package
as a symlink to the source code, so any changes made are reflected automatically
when running the executable.

```bash
pip3 install -e .
```

## Meta

Fernando Fernandez – ffernandezgalindo@triumf.ca

Distributed under the Apache license. See [``LICENSE``](LICENSE) for more information.

[https://github.com/hep-gc/dynafed_storagestats](https://github.com/hep-gc/dynafed_storagestats)

## Contributing

<!-- Markdown link & img dfn's -->
[wiki]: https://github.com/hep-gc/dynafed_storagestats/wiki
