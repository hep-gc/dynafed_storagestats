"""Helper functions used to contact DAV based API's."""

import datetime
import logging

import requests

from dynafed_storagestats import xml
import dynafed_storagestats.exceptions


##############
# Functions ##
##############

def list_files(storage_share):
    """Contact DAV endpoint to list all files and sum their sizes.

    Contacts a DAV endpoints and uses the header "'Depth': 'infinity'" to
    recursively obtain all the objects in a container and sum their size to
    obtain total space usage. Endpoint must accept the infinity value.
    ***Not recomended due to high resource usage for hosts wit a lot of files***

    Attributes:
    storage_share -- dynafed_storagestats StorageShare object.

    """
    # Creating logger
    _logger = logging.getLogger(__name__)

    _api_url = '{scheme}://{netloc}{path}'.format(
        scheme=storage_share.uri['scheme'],
        netloc=storage_share.uri['netloc'],
        path=storage_share.uri['path']
    )

    _headers = {'Depth': 'infinity'}
    _data = ''

    _logger.debug(
        "[%s]Requesting storage stats with: URN: %s API Method: %s Headers: %s Data: %s",
        storage_share.id,
        _api_url,
        storage_share.plugin_settings['storagestats.api'].lower(),
        _headers,
        _data
    )

    # We need to initialize "response" to check if it was successful in the
    # finally statement.
    _response = False

    try:
        _response = send_dav_request(
            storage_share,
            _api_url,
            _headers,
            _data
        )

    except requests.exceptions.InvalidSchema as ERR:
        raise dynafed_storagestats.exceptions.ConnectionErrorInvalidSchema(
            error='InvalidSchema',
            schema=storage_share.uri['scheme'],
            debug=str(ERR),
        )

    except requests.exceptions.SSLError as ERR:
        # If ca_path is custom, try the default in case
        # a global setting is incorrectly giving the wrong
        # ca's to check again.
        try:
            _response = send_dav_request(
                storage_share,
                _api_url,
                _headers,
                _data
            )

        except requests.exceptions.SSLError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="092",
                debug=str(ERR),
            )

    except requests.ConnectionError as ERR:
        raise dynafed_storagestats.exceptions.ConnectionError(
            error=ERR.__class__.__name__,
            status_code="400",
            debug=str(ERR),
        )

    except IOError as ERR:
        # We do some regex magic to get the file path
        _certfile = str(ERR).split(":")[-1]
        _certfile = _certfile.replace(' ', '')
        raise dynafed_storagestats.exceptions.ConnectionErrorDAVCertPath(
            certfile=_certfile,
            debug=str(ERR),
        )

    finally:
        if _response:
            # Check that we did not get an error code:
            if _response.status_code < 400:
                storage_share.stats['bytesused'], storage_share.stats['filecount'] = xml.add_xml_getcontentlength(_response.content)
                storage_share.stats['quota'] = int(storage_share.plugin_settings['storagestats.quota'])
                print("quota: %s, type: %s" % (storage_share.stats['quota'], type(storage_share.stats['quota'])))
                print("bytesused: %s, type: %s" % (storage_share.stats['bytesused'], type(storage_share.stats['bytesused'])))
                storage_share.stats['bytesfree'] = storage_share.stats['quota'] - storage_share.stats['bytesused']

            else:
                raise dynafed_storagestats.exceptions.ConnectionError(
                    error='ConnectionError',
                    status_code=_response.status_code,
                    debug=_response.text,
                )


def rfc4331(storage_share):
    """Contact DAV endpoint using RFC4331 to obtain storage stats.

    Utilizes the RFC4331 specification to ask a DAV endpoint for the quota
    and used space. DAV endpoint must support this specification and be
    properly configured. For more information about RFC4331:
    https://tools.ietf.org/html/rfc4331

    Attributes:
    storage_share -- dynafed_storagestats StorageShare object.

    """
    # Creating logger
    _logger = logging.getLogger(__name__)

    _api_url = '{scheme}://{netloc}{path}'.format(
        scheme=storage_share.uri['scheme'],
        netloc=storage_share.uri['netloc'],
        path=storage_share.uri['path']
    )

    _headers = {'Depth': '0'}
    _data = xml.create_rfc4331_request()

    _logger.debug(
        "[%s]Requesting storage stats with: URN: %s API Method: %s Headers: %s Data: %s",
        storage_share.id,
        _api_url,
        storage_share.plugin_settings['storagestats.api'].lower(),
        _headers,
        _data
    )

    # We need to initialize "response" to check if it was successful in the
    # finally statement.
    _response = False

    try:
        _response = send_dav_request(
            storage_share,
            _api_url,
            _headers,
            _data
        )

    except requests.exceptions.InvalidSchema as ERR:
        raise dynafed_storagestats.exceptions.ConnectionErrorInvalidSchema(
            error='InvalidSchema',
            schema=storage_share.uri['scheme'],
            debug=str(ERR),
        )

    except requests.exceptions.SSLError as ERR:
        # If ca_path is custom, try the default in case
        # a global setting is incorrectly giving the wrong
        # ca's to check agains.
        try:
            _response = send_dav_request(
                storage_share,
                _api_url,
                _headers,
                _data
            )

        except requests.exceptions.SSLError as ERR:
            raise dynafed_storagestats.exceptions.ConnectionError(
                error=ERR.__class__.__name__,
                status_code="092",
                debug=str(ERR),
            )

    except requests.ConnectionError as ERR:
        raise dynafed_storagestats.exceptions.ConnectionError(
            error=ERR.__class__.__name__,
            status_code="400",
            debug=str(ERR),
        )

    except IOError as ERR:
        # We do some regex magic to get the filepath
        _certfile = str(ERR).split(":")[-1]
        _certfile = _certfile.replace(' ', '')
        raise dynafed_storagestats.exceptions.ConnectionErrorDAVCertPath(
            certfile=_certfile,
            debug=str(ERR),
        )

    finally:
        if _response:
            # Check that we did not get an error code:
            if _response.status_code < 400:
                xml.process_rfc4331_response(_response, storage_share)

            else:
                raise dynafed_storagestats.exceptions.ConnectionError(
                    error='ConnectionError',
                    status_code=_response.status_code,
                    debug=_response.text,
                )


def send_dav_request(storage_share, api_url, headers, data):
    """Contact DAV endpoint with given headers and data.

    Arguments:
    storage_share -- dynafed_storagestats StorageShare object.
    api_url -- string defining API's url.
    headers -- dict containing HTML headers used in the request.
    data -- string containing data to be sent in the request. RFC4331 method
            uses this to request the stats in XML format. Obtained from:
            dynafed_storagestats.xml.create_rfc4331_request()

    Returns:
    String containing endpoint's response.

    """
    # Creating logger
    _logger = logging.getLogger(__name__)

    _response = requests.request(
        method="PROPFIND",
        url=api_url,
        cert=(
            storage_share.plugin_settings['cli_certificate'],
            storage_share.plugin_settings['cli_private_key']
        ),
        headers=headers,
        verify=storage_share.plugin_settings['ssl_check'],
        data=data,
        timeout=int(storage_share.plugin_settings['conn_timeout'])
    )
    # Save time when data was obtained.
    storage_share.stats['endtime'] = int(datetime.datetime.now().timestamp())

    # Log contents of response
    _logger.debug(
        "[%s]Endpoint reply: %s",
        storage_share.id,
        _response.text
    )

    return _response
