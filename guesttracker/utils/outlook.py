import os
import re
import sys
from datetime import datetime as dt
from datetime import timedelta as delta
from typing import *

from smseventlog import getlog
from smseventlog.errors import AppNotOpenError
from smseventlog.gui import _global as gbl

if sys.platform.startswith('win'):
    import win32com.client

    OutlookFolder = NewType('OutlookFolder', win32com.client.CDispatch)
else:
    OutlookFolder = NewType('OutlookFolder', str)  # TODO set up Outlook mac

log = getlog(__name__)

# TODO confirm if this needs tzinfo files


class Outlook(object):
    wo_request_folder = 'WO Request'  # default folder

    def __init__(self):

        self.check_outlook_running()
        self.app = win32com.client.Dispatch('outlook.application')
        self.client = self.app.GetNamespace('MAPI')

    def check_outlook_running(self) -> None:
        """Check if outlook is running, try to start if not
        """
        import win32ui

        try:
            win32ui.FindWindow(None, 'Microsoft Outlook')
        except win32ui.error:
            self.set_status('Starting outlook')

            try:
                os.startfile('outlook')
            except Exception as e:
                raise AppNotOpenError(app='Outlook')

    def set_status(self, msg: str, **kw) -> None:
        """Set GUI status, or log message"""
        mw = gbl.get_mainwindow()
        if not mw is None:
            mw.update_statusbar(msg=msg, **kw)
        else:
            print(msg)

    @property
    def inbox(self) -> OutlookFolder:
        """Get inbox folder"""
        return self.client.GetDefaultFolder(6)

    @property
    def root(self) -> OutlookFolder:
        """Get top level folder"""
        return self.inbox.parent

    def get_folder(self, name: str) -> OutlookFolder:
        """Get folder by name

        Parameters
        ----------
        name : str
            folder name

        Returns
        -------
        OutlookFolder
        """

        for fldr in list(self.root.Folders) + list(self.inbox.Folders):
            if fldr.name.lower() == name.lower():
                return fldr

        self.set_status(f'Outlook folder "{name}" not found.')

    def list_accounts(self) -> List[str]:
        """Get list of all local accounts in Outlook

        Returns
        -------
        List[str]
            list of account emails
        """
        return [acc.DeliveryStore.DisplayName for acc in self.client.Accounts]

    def list_folders(self) -> List[str]:
        """Get list of name of folders at root level
        - fldr objects will be unknown until str(called)

        Returns
        -------
        List[str]
            list of folder names
        """
        return [fldr.name for fldr in self.root.Folders]

    @property
    def wo_folder(self) -> OutlookFolder:
        """Get folder to search for WO request emails

        Returns
        -------
        OutlookFolder
            folder to search for wo emails
        """
        app = gbl.get_qt_app()
        name = gbl.get_setting('wo_request_folder', self.wo_request_folder)

        return self.get_folder(name=name)

    def get_wo_from_email(self, unit: str, title: str) -> Union[str, None]:
        """Get WO number from outlook

        Parameters
        ----------
        unit : str
            event unit
        title : str
            event title

        Returns
        -------
        Union[str, None]
            WO number if found

        Raises
        ------
        WONotFoundError
            if no wo found for unit/title
        """
        expr_sub = re.compile(f'{unit}.*{title}', re.IGNORECASE)
        expr_wo = re.compile('WO[0-9]{7}', re.IGNORECASE)

        # Filter messages to received in last 15 days
        d = (dt.now() + delta(days=-15)).strftime('%m/%d/%Y %H:%M %p')
        messages = self.wo_folder.Items.Restrict(f'[ReceivedTime] >= "{d}"')

        # items not sorted by date
        for item in messages:

            # find email by Unit - Title
            if re.search(expr_sub, item.Subject):

                match = re.search(expr_wo, item.Body)

                if not match is None:
                    wo = match.group(0)
                    return wo
