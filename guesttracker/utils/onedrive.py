# import onedrivesdk
import onedrivesdk_fork as onedrivesdk
from onedrivesdk_fork.helpers import GetAuthCodeServer  # type: ignore
from onedrivesdk_fork.helpers.resource_discovery import \
    ResourceDiscoveryRequest  # type: ignore

from guesttracker import functions as f

redirect_uri = 'http://localhost:8080'
# redirect_uri = 'https://login.microsoftonline.com/common/oauth2/nativeclient/'

# sms
# client_id = '9d90e2ed-eca3-4a8f-8d7f-0419c1bae35e'
# client_secret = '6-eY.fHqBPJ81aOe6aF~2Qy-170q..N4KH'

# personal
client_id = 'cf55ea19-a511-42d6-a3cf-385daa289de8'
client_secret = 'N_B1CL5S1jze84~AEjOnYr._eyQu5oVDqp'
secret_other = 'Cf2tqm-H6MM-SV_.ViwAhTVktL-_l366T2'
# client_secret = secret_other


class OneDrive(object):
    def __init__(self, mw=None, minesite=None, **kw):
        self._client = None
        _base_path = 'Photo Upload'

        f.set_self(vars())

    @property
    def client(self):
        if self._client is None:
            self._client = self._get_client()

        return self._client

    @property
    def photo_path(self):
        return f'{self._base_path}/{self.minesite}'

    def create_event_folder(self, name, uid='12345'):
        """Create new event folder in minesite's upload folder"""
        f = onedrivesdk.Folder()
        i = onedrivesdk.Item()
        i.name = name

        i.description = uid
        # i.id = uid
        i.c_tag = uid
        # i.e_tag = uid
        i.folder = f

        return self.client.item(drive='me', path=self.photo_path).children.add(i)

    def get_folder(self, name):
        """Return event folder in photos dir"""
        p = f'{self.photo_path}/{name}'
        return self.client.item(drive='me', path=p).get()

    def upload_file(self, p, dst: str):
        """Upload local file to specificed folder"""
        return

    def move_all_pdrive(self):
        """Move all pics to P drive"""

        return

    def get_inactive_folders(self, days_inactive: int = 7):
        """Return list of all inactive folder"""
        # get ALL folders in 'Photo Upload'

        # check fldr.last_modified_date_time()

        return

    def _get_client(self):
        scopes = ['wl.signin', 'wl.offline_access', 'onedrive.readwrite']

        client = onedrivesdk.get_default_client(
            client_id=client_id, scopes=scopes)

        auth_url = client.auth_provider.get_auth_url(redirect_uri)

        # this will block until we have the code
        code = GetAuthCodeServer.get_auth_code(auth_url, redirect_uri)

        # dont need client secret for personal!
        client_secret = None

        client.auth_provider.authenticate(code, redirect_uri, client_secret)
        return client


def get_client2():
    # sms
    client_id = '9d90e2ed-eca3-4a8f-8d7f-0419c1bae35e'
    client_secret = '6-eY.fHqBPJ81aOe6aF~2Qy-170q..N4KH'

    auth_server_url = 'https://login.microsoftonline.com/common/oauth2/authorize'
    auth_token_url = 'https://login.microsoftonline.com/common/oauth2/token'

    discovery_uri = 'https://api.office.com/discovery/'
    # discovery_uri = 'https://login.microsoftonline.com/common/oauth2/nativeclient/'

    http = onedrivesdk.HttpProvider()
    auth = onedrivesdk.AuthProvider(http,
                                    client_id,
                                    auth_server_url=auth_server_url,
                                    auth_token_url=auth_token_url)

    auth_url = auth.get_auth_url(redirect_uri)
    code = GetAuthCodeServer.get_auth_code(auth_url, redirect_uri)

    auth.authenticate(code, redirect_uri, client_secret, resource=discovery_uri)
    # If you have access to more than one service, you'll need to decide
    # which ServiceInfo to use instead of just using the first one, as below.
    service_info = ResourceDiscoveryRequest().get_service_info(auth.access_token)[0]
    auth.redeem_refresh_token(service_info.service_resource_id)
    client = onedrivesdk.OneDriveClient(service_info.service_resource_id + '/_api/v2.0/', auth, http)

    return client
