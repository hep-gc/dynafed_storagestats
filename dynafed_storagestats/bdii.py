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
        'GLUE2GroupID': 'grid',
        'objectClass': 'GLUE2Group',
    }
    _glue_group_dn = 'GLUE2GroupID=' + _glue_group['GLUE2GroupID'] + ',' + _glue_organization_dn

    _glue_resource = {
        'GLUE2GroupID': 'resource',
        'objectClass': 'GLUE2Group',
    }
    _glue_resource_dn = 'GLUE2GroupID=' + _glue_resource['GLUE2GroupID'] + ',' + _glue_organization_dn

    _glue_storage_service = {
        'GLUE2ServiceID': 'glue:' + hostname + '/' + _dynafed_mountpoint,
        'objectClass': ['GLUE2Service', 'GLUE2StorageService'],
        'GLUE2EntityName': _entity_name,
        'GLUE2ServiceAdminDomainForeignKey': _entity_name,
        'GLUE2ServiceCapability': 'data.access.flatfiles',
        'GLUE2ServiceQualityLevel': 'testing',
        'GLUE2ServiceType': 'org.dynafed.storage',
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_service_dn = 'GLUE2ServiceID=' + _glue_storage_service['GLUE2ServiceID'] + ',' + _glue_resource_dn

    _glue_storage_service_capacity = {
        'GLUE2StorageServiceCapacityID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/disk',
        'objectClass': 'GLUE2StorageServiceCapacity',
        'GLUE2StorageServiceCapacityType': 'online',
        'GLUE2StorageServiceCapacityTotalSize': 0,
        'GLUE2StorageServiceCapacityUsedSize': 0,
        'GLUE2StorageServiceCapacityFreeSize': 35,
        'GLUE2StorageServiceCapacityStorageServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_service_capacity_dn = 'GLUE2StorageServiceCapacityID=' + _glue_storage_service_capacity['GLUE2StorageServiceCapacityID'] + ',' + _glue_storage_service_dn

    _glue_storage_endpoint = {
        'GLUE2EndpointID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/' + _interface,
        'objectClass': ['GLUE2Endpoint', 'GLUE2StorageEndpoint'],
        'GLUE2EndpointCapability': 'data.access.flatfiles',
        'GLUE2EndpointHealthState': 'ok',
        'GLUE2EndpointImplementationName': 'dynafed',
        'GLUE2EndpointImplementationVersion': '1.5.0',
        'GLUE2EndpointInterfaceName': _interface,
        'GLUE2EndpointInterfaceVersion': 1.1,
        'GLUE2EndpointQualityLevel': 'testing',
        'GLUE2EndpointServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
        'GLUE2EndpointServingState': 'production',
        'GLUE2EndpointURL': _interface + '://' + hostname + ':' + _port + '/' + _dynafed_mountpoint,
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_endpoint_dn = 'GLUE2EndpointID=' + _glue_storage_endpoint['GLUE2EndpointID'] + ',' + _glue_storage_service_dn

    _glue_access_policy = {
        'GLUE2PolicyID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/' + _interface + '/' + _policy_scheme,
        'objectClass': ['GLUE2Policy', 'GLUE2AccessPolicy'],
        'GLUE2AccessPolicyEndpointForeignKey': _glue_storage_endpoint['GLUE2EndpointID'],
        'GLUE2PolicyRule': ['vo:atlas', 'vo:ops', 'vo:dteam'],
        'GLUE2PolicyScheme': _policy_scheme,
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_access_policy_dn = 'GLUE2PolicyID=' + _glue_access_policy['GLUE2PolicyID'] + ',' + _glue_storage_endpoint_dn

    _glue_storage_share = {
        'GLUE2ShareID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/' + _interface + '/' + 'atlas/atlasscratchdisk',
        'objectClass': ['GLUE2Share', 'GLUE2StorageShare'],
        'GLUE2StorageShareAccessLatency': 'online',
        'GLUE2StorageShareExpirationMode': 'neverexpire',
        'GLUE2StorageShareServingState': 'production',
        'GLUE2StorageShareSharingID': 'dedicated',
        'GLUE2StorageShareStorageServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_share_dn = 'GLUE2ShareID=' + _glue_storage_share['GLUE2ShareID'] + ',' + _glue_storage_service_dn

    _glue_storage_share_capacity = {
        'GLUE2StorageShareCapacityID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/' + _interface + '/' + 'atlas/atlasscratchdisk' + '/disk',
        'objectClass': 'GLUE2StorageShareCapacity',
        'GLUE2StorageShareCapacityType': 'online',
        'GLUE2StorageShareCapacityStorageShareForeignKey': _glue_storage_share['GLUE2ShareID'],
        'GLUE2StorageShareCapacityFreeSize': _storage_share.stats['bytesfree'],
        'GLUE2StorageShareCapacityTotalSize': _storage_share.stats['quota'],
        'GLUE2StorageShareCapacityUsedSize': _storage_share.stats['bytesused'],
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_share_capacity_dn = 'GLUE2StorageShareCapacityID=' + _glue_storage_share_capacity['GLUE2StorageShareCapacityID'] + ',' + _glue_storage_share_dn
