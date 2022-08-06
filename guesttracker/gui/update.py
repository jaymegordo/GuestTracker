import re
import warnings
from typing import *

import requests
from pkg_resources import parse_version
from pkg_resources.extern.packaging.version import Version  # type: ignore
from pyupdater.client import Client

from guesttracker import VERSION
from guesttracker import config as cf
from guesttracker import errors as er
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.gui import _global as gbl
from guesttracker.utils import fileops as fl

warnings.simplefilter('ignore', DeprecationWarning)  # pyupdater turns this on, annoying
log = getlog(__name__)


class ClientConfig(object):
    PUBLIC_KEY = 'Rbk396oV6YSKhJtYTZHGdu/z7P/Gom11LdqI/w3AlyQ'
    APP_NAME = 'SMS Event Log'
    COMPANY_NAME = 'SMS Equipment Inc.'
    HTTP_TIMEOUT = 30
    MAX_DOWNLOAD_RETRIES = 3
    UPDATE_URLS = ['https://guesttracker.s3.amazonaws.com']


class Updater(object):
    """Wrap Pyupdater Client/AppUpdate classes to check, download, and install updates"""

    @er.errlog('Failed to initialize updater!')
    def __init__(self, mw=None, test_version=None, channel='stable', dev_channel=False):

        client_config = ClientConfig()
        self.client = Client(client_config, progress_hooks=[self.print_status_info])

        # set dev channel to alpha internally based on bool
        if dev_channel:
            channel = 'alpha'

        warnings.simplefilter('ignore', DeprecationWarning)  # pyupdater turns this on, annoying

        _version = VERSION if test_version is None else test_version
        update_available = False
        app_update = None
        status = 'initialized'
        self.url_changelog = f'{cf.URL_REPO}/CHANGELOG.md'

        f.set_self(vars())

    def get_changelog(self) -> Dict[str, str]:
        """Read updates from CHANGELOG.md on github

        Returns
        -------
        Dict[str, str]
            changelog parsed by version
            {'3.7.0': 'Added ... Fixed ...'}
        """
        html = requests.get(self.url_changelog).text

        expr = r'##\s\d\.\d\.\d.*'

        m = {}
        for s in re.finditer(expr, html):
            m[s.group(0).replace('## ', '')] = s.span()[0]

        # create index ranges between current and next version header
        idxs = list(m.values()) + [len(html)]

        for i, (k, v) in enumerate(m.items()):
            m[k] = html[v: idxs[i + 1]]

        return m

    def get_changelog_full(self) -> str:
        """Get full changelog as string for markdown message"""
        m = self.get_changelog()
        return '\n'.join([s for v, s in m.items()])

    def get_changelog_new(self, version: str = None) -> str:
        """Get changelog from current version to newest

        Parameters
        ----------
        version : str
            get changelog updates newer than this version, default self.version

        Returns
        -------
        str
            changelog text updates
        """
        try:
            version = version or self.version
            m = self.get_changelog()
            if m:
                return '\n'.join([s for v, s in m.items() if parse_version(v) > parse_version(str(version))])
            else:
                return 'No newer versions detected in changelog.'

        except Exception as e:
            # just in case anything goes wrong with getting changelog data
            er.log_error()
            return f'Failed to get update from changelog:\n\n{self.url_changelog}'

    def update_statusbar(self, msg: str, log_msg: bool = True, *args, **kw):

        if log_msg:
            self.set_status(msg=msg)

        if not self.mw is None:
            self.mw.update_statusbar(msg, *args, **kw)
        else:
            print(msg)

    def set_status(self, msg: str):
        self.status = msg
        log.info(msg)

    def get_app_update(self):
        client = self.client
        client.refresh()  # download version info

        app_update = client.update_check(client.app_name, self._version, channel=self.channel)
        if not app_update is None:
            self.update_available = True
            self.app_update = app_update

        return app_update

    def check_update(self, **kw) -> Union['Updater', None]:
        app_update = self.get_app_update()
        self.update_statusbar(msg='Checking for update.')

        if self.update_available:
            self.update_statusbar(msg='Update available, download started.')

            # download can fail to rename '...zip.part' to '...zip' if zombie locking process exists
            try:
                fl.kill_sms_proc_locking(filename='zip')
            except:
                er.log_error(msg='Failed to check/kill locking process', log=log)

            app_update.download()  # background=True # don't need, already in a worker thread
            if app_update.is_downloaded():
                self.update_statusbar(msg=f'Update successfully downloaded. New version: {self.ver_latest}')

                return self

        else:
            self.update_statusbar(
                msg=f'No update available. Current: {self.version}, Latest: {self.ver_latest}')

    def install_update(self, restart: bool = True) -> None:
        """Extract update and restart app
        - Show console window
        """
        app_update = self.app_update
        if not app_update is None and app_update.is_downloaded():
            if restart:
                self.set_status(msg='Extracting update and restarting.')
                app_update.extract_restart()
            else:
                self.set_status(msg='Extracting on close without restart.')
                app_update.extract_overwrite()

    def print_finished(self):
        self.update_statusbar(msg='Download finished.')

    def print_failed(self, *args, **kw):
        self.update_statusbar(msg=f'Update failed at: {self.status}', warn=True)

    def print_status_info(self, info):
        total = info.get('total')
        downloaded = info.get('downloaded')
        status = info.get('status')
        pct = downloaded / total
        self.update_statusbar(msg=f'Update {status} - {fl.size_readable(total)} - {pct:.0%}', log_msg=False)

    @staticmethod
    def check_ver(ver: Union[Version, str]) -> Version:
        """Ensure version strings have patch ".0"

        Parameters
        ----------
        ver : Union[Version, str]

        Returns
        -------
        Version
            Version eg "3.7.0" instead of "3.7"
        """

        # self.app_update.version can be "3.7.1 Alpha 1"
        m_replace = dict(alpha='a', beta='b')
        ver = str(ver).lower().replace(' ', '')

        for k, v in m_replace.items():
            ver = ver.replace(k, v)

        ver = parse_version(ver)

        if ver.micro == 0:
            parts = str(ver).split('.')

            # fix 3.7, but not 3.7.0a1
            if len(parts) == 2:
                ver = parse_version(str(ver) + '.0')

        return ver

    @property
    def version(self) -> Version:
        """Current app version
        - Version string shortened to remove .0.1 for display
        - NOTE not sure if this causes issues when comparing alpha releases

        Returns
        -------
        Version
            <Version('3.7.0')>
        """
        # if not self.app_update is None:
        #     # '3.7.0.0.9'
        #     ver_long = self.app_update.current_version
        #     ver = '.'.join(ver_long.split('.')[:3])
        # else:
        #     # fall back to current app version eg __init__.VERSION
        #     ver = self._version

        # just use current app version
        ver = self._version
        return self.check_ver(ver)

    @property
    def ver_latest(self) -> Union[Version, None]:
        """Latest version available for download

        Returns
        -------
        Union[Version, None]
            <Version('3.3.1')>
        """
        # self.app_update.version can be "3.7.1 Alpha 1"

        if not self.app_update is None:
            return self.check_ver(self.app_update.version)
        else:
            return None

    @property
    def ver_dismissed(self) -> Version:
        """Get latest version dismissed by user"""
        return self.check_ver(gbl.get_setting('ver_dismissed', '0.0.0'))

    @property
    def needs_update(self) -> bool:
        """Check if newest version either HASNT been dismissed, or is a major/minor version above"""
        v_latest = self.ver_latest
        v_dismissed = self.ver_dismissed
        log.info(f'v_latest: {v_latest}, v_dismissed: {v_dismissed}')

        if v_latest is None:
            return False

        return (
            v_latest > v_dismissed or
            v_latest.major > v_dismissed.major or
            v_latest.minor > v_dismissed.minor)
