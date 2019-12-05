"""Functions to deal with the formatting and handling of BDII info format."""

import datetime

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
    NOW = '{0:%Y-%m-%dT%H:%M:%SZ}'.format(datetime.datetime.utcnow())

    _dynafed_mountpoint = 'dynafed'
    _entity_name = 'CA-TRIUMF-DYNAFED'
    _interface = 'https'
    _interface_version = '1.1'
    _port = '443'
    _policy_scheme = 'basic'
    _retention_policy = 'replica' #http://glue20.web.cern.ch/glue20/#b37

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
        'GLUE2ServiceType': 'dynafed',
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_service_dn = 'GLUE2ServiceID=' + _glue_storage_service['GLUE2ServiceID'] + ',' + _glue_resource_dn

    _glue_storage_service_capacity = {
        'GLUE2StorageServiceCapacityID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/ssc/disk',
        'objectClass': 'GLUE2StorageServiceCapacity',
        'GLUE2StorageServiceCapacityType': 'online',
        'GLUE2StorageServiceCapacityTotalSize': 0,
        'GLUE2StorageServiceCapacityUsedSize': 0,
        'GLUE2StorageServiceCapacityFreeSize': 35,
        'GLUE2StorageServiceCapacityStorageServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_service_capacity_dn = 'GLUE2StorageServiceCapacityID=' + _glue_storage_service_capacity['GLUE2StorageServiceCapacityID'] + ',' + _glue_storage_service_dn

    _glue_storage_access_protocol = {
        'GLUE2StorageAccessProtocolID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/ap/' + _interface + '/' + _interface_version,
        'objectClass': 'GLUE2StorageAccessProtocol',
        'GLUE2StorageAccessProtocolStorageServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
        'GLUE2StorageAccessProtocolType': _interface,
        'GLUE2StorageAccessProtocolVersion': _interface_version,
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_access_protocol_dn = 'GLUE2StorageAccessProtocolID=' + _glue_storage_access_protocol['GLUE2StorageAccessProtocolID'] + ',' + _glue_storage_service_dn

    _glue_storage_endpoint = {
        'GLUE2EndpointID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/ep/' + _interface + '/' + _interface_version,
        'objectClass': ['GLUE2Endpoint', 'GLUE2StorageEndpoint'],
        'GLUE2EndpointCapability': 'data.access.flatfiles',
        'GLUE2EndpointHealthState': 'ok',
        'GLUE2EndpointImplementationName': 'dynafed',
        'GLUE2EndpointImplementationVersion': '1.5.0',
        'GLUE2EndpointInterfaceName': _interface,
        'GLUE2EndpointInterfaceVersion': _interface_version,
        'GLUE2EndpointQualityLevel': 'testing',
        'GLUE2EndpointServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
        'GLUE2EndpointServingState': 'production',
        'GLUE2EndpointURL': _interface + '://' + hostname + ':' + _port + '/' + _dynafed_mountpoint,
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_storage_endpoint_dn = 'GLUE2EndpointID=' + _glue_storage_endpoint['GLUE2EndpointID'] + ',' + _glue_storage_service_dn

    _glue_access_policy = {
        'GLUE2PolicyID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/ep/ap/' + _policy_scheme,
        'objectClass': ['GLUE2Policy', 'GLUE2AccessPolicy'],
        'GLUE2AccessPolicyEndpointForeignKey': _glue_storage_endpoint['GLUE2EndpointID'],
        'GLUE2PolicyRule': ['vo:atlas', 'vo:ops', 'vo:dteam'],
        'GLUE2PolicyScheme': _policy_scheme,
        'GLUE2EntityCreationTime': NOW,
    }
    _glue_access_policy_dn = 'GLUE2PolicyID=' + _glue_access_policy['GLUE2PolicyID'] + ',' + _glue_storage_endpoint_dn

    # For each storage share
    _glue_storage_shares = []
    _glue_storage_share_capacities = []

    for _storage_endpoint in storage_endpoints:
        for _storage_share in _storage_endpoint.storage_shares:

            _share_mountpoint = _storage_share.plugin_settings['xlatepfx'].split()[0]
            # We need to convert the bytes into GB and also if a '-1' is present
            # indicating error obtaining the data, then it must be transformed
            # to the Glue2 reference placeholder of 999,999,999,999,999,999
            # http://glue20.web.cern.ch/glue20/#a8

            _stats = {
                      'bytesfree': 999999999999999999,
                      'quota': 999999999999999999,
                      'bytesused': 999999999999999999,
            }

            for _stat in ['bytesfree','quota','bytesused']:
                if _storage_share.stats[_stat] != -1:
                    _stats[_stat] = int(_storage_share.stats[_stat] * 1e-9)

            _glue_storage_share = {
                'GLUE2ShareID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/ss' + _share_mountpoint,
                'objectClass': ['GLUE2Share', 'GLUE2StorageShare'],
                'GLUE2StorageShareAccessLatency': 'online',
                'GLUE2StorageShareExpirationMode': 'neverexpire',
                'GLUE2StorageShareRetentionPolicy': _retention_policy,
                'GLUE2StorageShareServingState': 'production',
                'GLUE2StorageShareSharingID': 'dedicated',
                'GLUE2StorageShareStorageServiceForeignKey': _glue_storage_service['GLUE2ServiceID'],
                'GLUE2EntityCreationTime': NOW,
            }
            _glue_storage_share_dn = 'GLUE2ShareID=' + _glue_storage_share['GLUE2ShareID'] + ',' + _glue_storage_service_dn

            _glue_storage_share_capacity = {
                'GLUE2StorageShareCapacityID': 'glue:' + hostname + '/' + _dynafed_mountpoint + '/ss' + _share_mountpoint + '/disk',
                'objectClass': 'GLUE2StorageShareCapacity',
                'GLUE2StorageShareCapacityType': 'online',
                'GLUE2StorageShareCapacityStorageShareForeignKey': _glue_storage_share['GLUE2ShareID'],
                'GLUE2StorageShareCapacityFreeSize': _stats['bytesfree'],
                'GLUE2StorageShareCapacityTotalSize': _stats['quota'],
                'GLUE2StorageShareCapacityUsedSize': _stats['bytesused'],
                'GLUE2StorageShareRetentionPolicy': _retention_policy,
                'GLUE2EntityCreationTime': NOW,
            }
            _glue_storage_share_capacity_dn = 'GLUE2StorageShareCapacityID=' + _glue_storage_share_capacity['GLUE2StorageShareCapacityID'] + ',' + _glue_storage_share_dn
            # Add the capacities:
            _glue_storage_service_capacity['GLUE2StorageServiceCapacityTotalSize'] += _glue_storage_share_capacity['GLUE2StorageShareCapacityTotalSize']
            _glue_storage_service_capacity['GLUE2StorageServiceCapacityFreeSize'] += _glue_storage_share_capacity['GLUE2StorageShareCapacityFreeSize']
            _glue_storage_service_capacity['GLUE2StorageServiceCapacityUsedSize'] += _glue_storage_share_capacity['GLUE2StorageShareCapacityUsedSize']

            _glue_storage_shares.append([_glue_storage_share_dn, _glue_storage_share])
            _glue_storage_share_capacities.append([_glue_storage_share_capacity_dn, _glue_storage_share_capacity])

    # Let the printing begin
    print_bdii(_glue_organization_dn, _glue_organization)
    print("\n")
    print_bdii(_glue_group_dn, _glue_group)
    print("\n")
    print_bdii(_glue_resource_dn, _glue_resource)
    print("\n")
    print_bdii(_glue_storage_service_dn, _glue_storage_service)
    print("\n")
    print_bdii(_glue_storage_service_capacity_dn, _glue_storage_service_capacity)
    print("\n")
    print_bdii(_glue_storage_access_protocol_dn, _glue_storage_access_protocol)
    print("\n")
    print_bdii(_glue_storage_endpoint_dn, _glue_storage_endpoint)
    print("\n")
    print_bdii(_glue_access_policy_dn, _glue_access_policy)
    print("\n")

    for _share in _glue_storage_shares:
        print_bdii(_share[0], _share[1])
        print("\n")

    for _capacity in _glue_storage_share_capacities:
        print_bdii(_capacity[0], _capacity[1])
        print("\n")


def print_bdii(dn, body):
    print('dn: %s' % (dn))
    for key, val in body.items():
        if type(val) is list:
            for v in val:
                print('%s: %s' % (key, v))
        else:
            print('%s: %s' % (key, val))
