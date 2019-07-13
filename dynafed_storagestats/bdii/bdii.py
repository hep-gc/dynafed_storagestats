"""Functions to deal with the formatting and handling of BDII info format."""

import datetime

#############
## Classes ##
#############

# class ():
#     """Base class representing unique URL storage endpoint.
#
#     Will be completed with one or several StorageShare objects that share the
#     same URL, as a list in self.storage_shares.
#
#     """
#
#     def __init__(self, url):
#         """Create storage_shares and url attributes.
#
#         Arguments:
#         url -- string.
#
#         """
#         self.storage_shares = []
#         self.url = url


#############
# Functions #
#############

def format_bdii(storage_endpoints, hostname="localhost"):
    """Create object(s) representing Dynafed site storage stats for BDII publishing.

    Creates a BDII compatible format containing the storage stats information
    for the Dynafed Site and its "shares" as required and specified by:
    https://wiki.egi.eu/wiki/MAN01_How_to_publish_Site_Information

    Arguments:
    storage_endpoints -- List of dynafed_storagestats StorageEndpoint objects.
    hostname -- string defining Dynafed host.

    Returns:


    """
    #!!! Validate Types and if Mandatory!!!
    NOW = int(datetime.datetime.now().timestamp())

    _dynafed_mountpoint = 'dynafed'
    _entity_name = 'CA-TRIUMF-DYNAFED'
    _interface = 'https'
    _port = '443'
    _policy_scheme = 'basic'

    _glue_organization = {
        'objectClass': 'organization',
        'o': 'glue',
    }
    _glue_organization_dn = 'o=' + _glue_organization['o']

    _glue_group = {
        'objectClass': 'GLUE2Group',
        'GLUE2GroupID': 'grid',
    }
    _glue_group_dn = 'GLUE2GroupID=' + _glue_group['GLUE2GroupID'] + ',' + _glue_organization_dn

    _glue_resource = {
        'objectClass': 'GLUE2Group',
        'GLUE2GroupID': 'resource',
    }
    _glue_resource_dn = 'GLUE2GroupID=' + _glue_resource['GLUE2GroupID'] + ',' + _glue_organization_dn

    _glue_storage_service = {
        'GLUE2ServiceID': 'glue:' + hostname,
        'objectClass': 'GLUE2StorageService',
        'objectClass': 'GLUE2Service',
        'GLUE2EntityName': _entity_name,
        'GLUE2ServiceQualityLevel': 'production',
        'GLUE2ServiceCapability': 'data.access.flatfiles',
        'GLUE2ServiceType': 'org.dynafed.storage',
        'GLUE2ServiceAdminDomainForeignKey': _entity_name,
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_service_dn = 'GLUE2ServiceID=' + _glue_storage_service['GLUE2ServiceID'] + ',' + _glue_resource_dn

    _glue_storage_endpoint = {
        'GLUE2EndpointQualityLevel': production,
        'GLUE2EndpointInterfaceName': _interface,
        'GLUE2EndpointCapability': 'data.transfer',
        'objectClass': 'GLUE2StorageEndpoint',
        'objectClass': 'GLUE2Endpoint',
        'GLUE2EndpointURL': _interface + '://' + hostname + ':' + _port,
        'GLUE2EndpointImplementationName': 'dynafed',
        'GLUE2EndpointHealthState': 'ok',
        'GLUE2EndpointServingState': 'production',
        'GLUE2EndpointServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
        'GLUE2EndpointImplementationVersion': '1.5.0',
        'GLUE2EndpointID': _glue_storage_service['GLUE2ServiceID'] + '/' _dynafed_mountpoint,
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_endpoint_dn = 'GLUE2EndpointID=' + _glue_storage_endpoint['GLUE2EndpointID'] + ',' + _glue_storage_service_dn

    _glue_access_policy = {
        'objectClass': 'GLUE2AccessPolicy',
        'objectClass': 'GLUE2Policy',
        'GLUE2PolicyRule': 'vo:atlas',
        'GLUE2PolicyRule': 'vo:ops',
        'GLUE2PolicyRule': 'vo:dteam',
        'GLUE2PolicyScheme': _policy_scheme,
        'GLUE2AccessPolicyEndpointForeignKey': _glue_storage_endpoint['GLUE2EndpointID'],
        'GLUE2PolicyID': _glue_storage_endpoint + '/' + _policy_scheme,
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_access_policy_dn = 'GLUE2PolicyID=' + _glue_access_policy['GLUE2PolicyID'] + ',' + _glue_storage_endpoint_dn

    _glue_storage_share = {
        'objectClass': 'GLUE2StorageShare',
        'objectClass': 'GLUE2Share',
        'GLUE2StorageShareStorageServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
        'GLUE2StorageShareSharingID': 'dedicated',
        'GLUE2ShareID': _glue_storage_endpoint['GLUE2EndpointID'] + '/' + 'atlas/atlasscratchdisk',
        'GLUE2StorageShareServingState': 'production',
        'GLUE2StorageShareAccessLatency': 'online',
        GLUE2EntityCreationTime: NOW,
    }
    _glue_storage_share_dn = 'GLUE2ShareID=' + _glue_storage_share['GLUE2ShareID'] + ',' + _glue_storage_service_dn

    _glue_storage_share_capacity = {
        'GLUE2StorageShareCapacityStorageShareForeignKey': _glue_storage_share['GLUE2ShareID'],
        'GLUE2StorageShareCapacityType': 'online',
        'objectClass': 'GLUE2StorageShareCapacity',
        'GLUE2StorageShareCapacityID': _glue_storage_share['GLUE2ShareID'] + '/disk',
        'GLUE2StorageShareCapacityTotalSize': _glue_storage_share.stats['quota'],
        'GLUE2StorageShareCapacityFreeSize': _glue_storage_share.stats['bytesfree'],
        'GLUE2StorageShareCapacityUsedSize': _glue_storage_share.stats['bytesused'],
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_share_capacity_dn = 'GLUE2StorageShareCapacityID=' + _glue_storage_share_capacity['GLUE2StorageShareCapacityID'] + ',' + _glue_storage_share_dn
