# ugrstoragestats.py

Module to interact with UGR's configuration files in order to obtain
storage status information from various types of endpoints.

## Prerequisites:
Python Modules:
- lxml
- memcache
- requests
- requests_aws4auth

## Endpoints Configuration

In order to use the correct methods for each storage type some options should
be added to the endpoints.conf configuration file:

### S3
```
locplugin.<ID>.s3.api: [generic, ceph-admin]
```

<<<<<<< HEAD
**generic**
This option will list all objects behind the bucket and add the individual
sizes.

**ceph-admin**
=======
#### generic
This option will list all objects behind the bucket and add the individual
sizes.

#### ceph-admin
>>>>>>> Added documentation.
Use this option if Ceph's Admin API is to be used. The credentials of the
configured user should have the "bucket read" caps enabled.

```
<<<<<<< HEAD
locplugin.<ID>.s3.signature_ver: [s3, s3v4]
=======
locplugin.cceastw.<ID>.signature_ver: [s3, s3v4]
>>>>>>> Added documentation.
```
Most S3 endpoints should use the s3v4 signature auth version, and is used as
default, but use s3 in case is needed.
