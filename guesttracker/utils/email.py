import re
from pathlib import Path
from typing import *

from smseventlog import config as cf
from smseventlog import functions as f
from smseventlog import getlog

if cf.is_win:
    import win32com.client as win32
else:
    from aeosa.appscript import app, k
    from aeosa.mactypes import Alias  # type: ignore

log = getlog(__name__)

# OUTLOOK


class Outlook(object):
    def __init__(self):
        is_win = cf.is_win
        _folders = None
        _wo_folder = None

        if is_win:
            client = win32.Dispatch('Outlook.Application')
        else:
            client = app('Microsoft Outlook')

        f.set_self(vars())

    @property
    def folders(self):
        if self._folders is None:
            if self.is_win:
                pass
            else:
                self._folders = self.client.mail_folders.get()

        return self._folders

    def folder(self, name: str):
        return [fldr for fldr in self.folders if str(fldr.name()).lower() == name.lower()][1]

    def messages(self, name: str):
        """Return folder messages sorted by date_received descending"""

        folder = self.folder(name=name)

        if not self.is_win:
            messages = folder.messages()
            messages = sorted(messages, key=lambda x: x.time_received(), reverse=True)
        else:
            messages = None

        return messages

    @property
    def wo_folder(self):
        # WO Request folder, used for finding WO request emails to read back into event log
        if self._wo_folder is None:
            wo_folder_list = list(filter(lambda x: 'wo request' in str(x.name()).lower(), self.folders))
            if wo_folder_list:
                self._wo_folder = wo_folder_list[0]

        return self._wo_folder

    def get_wo_number(self, unit, title):
        # get WO number from outlook folder 'WO Request'
        # match on unit and title
        return


class Message(object):
    def __init__(self, parent=None, subject='', body=' ', to_recip=None, cc_recip=None, show_=True):
        # NOTE IMPORTANT - For mac, need to create the message, add attachments, THEN show!!
        if parent is None:
            parent = Outlook()
        is_win = parent.is_win
        client = parent.client

        font = 'Calibri'
        body = f'<div style="font-family: {font};">{body}</div>'

        if is_win:
            _msg = client.CreateItem(0)
            _msg.Subject = subject

            # GetInspector makes outlook get the message ready for display, which adds in default email sig
            _msg.GetInspector
            initial_body = _msg.HTMLBody
            body_start = re.search('<body.*?>', initial_body).group()
            _msg.HTMLBody = re.sub(
                pattern=body_start,
                repl=f'{body_start}{body}',
                string=initial_body)

        else:  # mac
            #     try:
            #         # NOTE try to force add signature while on new outlook for mac, this could be improved
            #         sig = list(filter(lambda x: x.name() == 'Jayme SMS', client.signatures()))[0]
            #         sig = sig.content()
            #         body = f'{body}{sig}'
            #     except:
            #         pass

            _msg = client.make(
                new=k.outgoing_message,
                with_properties={k.subject: subject, k.content: body})

        f.set_self(vars())

        self.add_recipients(emails=to_recip, type_='to')
        self.add_recipients(emails=cc_recip, type_='cc')

        if show_:
            self.show()

    def show(self):
        msg = self._msg

        if self.is_win:
            msg.Display(False)
        else:
            msg.open()
            msg.activate()

    def add_attachments(self, lst_attach: List[str] = None) -> None:
        """Add multiple attachments to email

        Parameters
        ----------
        lst_attach : List[str], optional
            list of files to add, by default None
        """
        if lst_attach is None:
            return

        for p in f.as_list(lst_attach):
            try:
                self.add_attachment(p=p)
            except:
                log.warning(f'Couldn\'t add attachment: {p}')

    def add_attachment(self, p: Path) -> None:
        """Add single attachment to email

        Parameters
        ----------
        p : Path
            file path to attach
        """
        msg = self._msg

        if self.is_win:
            msg.Attachments.Add(Source=str(p))
        else:
            p = Alias(str(p))  # convert string to POSIX/mactypes path idk
            attach = msg.make(new=k.attachment, with_properties={k.file: p})

    def add_recipients(self, emails: List[str], type_: str = 'to') -> None:
        if emails is None:
            return

        # ensure email list is unique, sorted alphabetically, and lowercase
        emails = sorted({x.lower() for x in set(emails)})

        if self.is_win:
            recips = ';'.join(emails)
            msg = self._msg

            if type_ == 'to':
                msg.To = recips
            elif type_ == 'cc':
                msg.CC = recips

        else:
            # mac needs to make 'recipient' objects and add emails seperately
            for email in emails:
                self.add_recipient(email=email, type_=type_)

    def add_recipient(self, email: str, type_: str = 'to') -> None:

        if type_ == 'to':
            recipient = k.to_recipient
        elif type_ == 'cc':
            recipient = k.cc_recipient

        self._msg.make(new=recipient, with_properties={k.email_address: {k.address: email}})
