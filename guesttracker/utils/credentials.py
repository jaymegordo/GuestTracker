import base64
from typing import *

import six

from jgutils.secrets import SecretsManager
from smseventlog import functions as f
from smseventlog import getlog, ic
from smseventlog.config import AZURE, AZURE_WEB

log = getlog(__name__)


class CredentialManager(object):
    """load and save credentials to QSettings, or just yaml file
        - prompt for creds when not found

    Examples
    --------
    >>> from smseventlog.utils.credentials import CredentialManager
        cm = CredentialManager(name='fluidlife', gui=False)
        cm.load()
    """
    config = {
        'tsi': {
            'id_type': 'username',
            'keys': ['id', 'password']},
        'sms': {
            'id_type': 'username',
            'keys': ['id', 'password']},
        'exchange': {
            'id_type': 'email',
            'keys': ['id', 'password']},
        'sap': {
            'id_type': 'username',
            'keys': ['id', 'password']}}

    def __init__(self, name: str, prompt: bool = False, gui: bool = True, prefix: bool = True):
        name = name.lower()
        encode_key = 'wwdlkoeedfdk'
        if AZURE:
            gui = False

        if gui:
            # if gui, will need dialogs for prompts
            from smseventlog.gui import _global as gbl
            from smseventlog.gui.dialogs import base as dlgs
            from smseventlog.gui.dialogs import passwords as pw
            self.dlgs = dlgs

            settings = gbl.get_settings()
            config_gui = {
                'tsi': {
                    'prompt': pw.TSIUserName},
                'sms': {
                    'prompt': pw.SMSUserName},
                'exchange': {
                    'prompt': pw.ExchangeLogin},
                'sap': {
                    'prompt': pw.SuncorWorkRemote}}

            # merge specific gui config into config
            for key, m in self.config.items():
                m.update(config_gui.get(key, {}))

        else:
            # load from config.yaml
            prefix = False
            name_creds = 'credentials.yaml'
            static_creds_full = SecretsManager(name_creds).load  # load creds dict from yaml
            static_creds = static_creds_full.get(name, None)

        self.config = self.config.get(name, {})
        id_type = self.config.get('id_type', 'username')

        self.name = name
        f.set_self(vars())

        if prompt:
            self.prompt_credentials()

    def load(self):
        """load id/pw from QSettings or file in secrets"""
        name = self.name
        keys = self.config.get('keys', ['username', 'password'])  # assume user/pw (wont work for eg sentry)
        m = self.load_multi(keys=keys)

        if self.gui and any(m.get(x) is None for x in ('id', 'password')):
            # no creds found in QSettings
            m = self.prompt_credentials()
            if m is False:
                return (None, None)  # user exited dialog

        # always return in order defined eg id, password, token
        return tuple(m.get(key, None) for key in keys)

    def load_db(self) -> Tuple[str, str]:
        """Load username/password from db credentials
        - TODO should probably encrypt this

        Returns
        -------
        Tuple[str, str]
            (username, password)
        """
        from smseventlog.database import db
        sql = f"select id, password from credentials where [name] = '{self.name}'"
        return db.cursor.execute(sql).fetchone()

    def update_password_db(self, password: str) -> None:
        """Update password in credentials table in db

        Parameters
        ----------
        password : str
            new password to update
        """
        from smseventlog.database import db
        _password = password
        password = password.replace("'", "''")
        sql = f"update credentials set [password] = '{password}' where [name] = '{self.name}'"
        cursor = db.cursor
        cursor.execute(sql)
        cursor.commit()
        log.info(f'Updated exchange password to: {_password}')

    def save_single(self, key: str, val: Any) -> None:

        # obfuscate pw before writing to settings
        if self.gui and 'password' in key.lower():
            val = encode(key=self.encode_key, string=val)

        if self.prefix:
            key = f'{self.name}_{key.lower()}'

        ic(key, val)
        self.settings.setValue(key, val)

    def save_multi(self, vals: dict) -> None:
        """Save multiple key/val pairs

        Parameters
        ----------
        vals : dict
        """
        if AZURE_WEB:
            return

        if self.gui:
            for key, val in vals.items():
                self.save_single(key, val)
        else:
            # for non-gui, need to just dump full file back with updates
            try:
                self.static_creds.update(vals)
                self.static_creds_full[self.name].update(self.static_creds)

                SecretsManager().write(file_data=self.static_creds_full, name=self.name_creds)

            except:
                log.warning(f'Failed to write credentials back to file: {vals}')

    def load_single(self, key):
        if self.gui:
            if self.prefix:
                key = f'{self.name}_{key}'

            val = self.settings.value(key.lower(), defaultValue=None)

            # decrypt pw before usage
            if not val is None and 'password' in key.lower():
                val = decode(key=self.encode_key, string=val)

            return val

        else:
            # load from static settings in credentials.yaml (for azure funcs)
            return self.static_creds.get(key, None)

    def load_multi(self, keys):
        # simple lookup of requested keys, return dict of key/val
        m = {}
        for key in keys:
            m[key] = self.load_single(key=key)

        return m

    def prompt_credentials(self):
        # prompt user to input id/password, can be triggered automatically or manually by user
        prompt = self.config.get('prompt', None)

        if not prompt is None:
            dlg = prompt()
            if dlg.exec():
                # return id/password from dialog and save
                m = {k.lower(): v for k, v in dlg.items.items()}  # change to lower() keys
                m['id'] = m.pop(self.id_type, None)  # change username/email to 'id'

                self.save_multi(vals=m)
                self.dlgs.msg_simple(msg='Successfully saved credentials.')

                return m
            else:
                return False
        else:
            raise AttributeError(f'Couldn\'t find credential prompt for: {self.name}')


def encode(key, string):
    encoded_chars = []
    for i in range(len(string)):
        key_c = key[i % len(key)]
        encoded_c = chr(ord(string[i]) + ord(key_c) % 256)
        encoded_chars.append(encoded_c)
    encoded_string = ''.join(encoded_chars)
    encoded_string = encoded_string.encode('latin') if six.PY3 else encoded_string
    return base64.urlsafe_b64encode(encoded_string).rstrip(b'=')


def decode(key, string):
    string = base64.urlsafe_b64decode(string + b'===')
    string = string.decode('latin') if six.PY3 else string
    encoded_chars = []
    for i in range(len(string)):
        key_c = key[i % len(key)]
        encoded_c = chr((ord(string[i]) - ord(key_c) + 256) % 256)
        encoded_chars.append(encoded_c)
    encoded_string = ''.join(encoded_chars)
    return encoded_string
