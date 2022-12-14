import logging
import re
from datetime import datetime as dt
from datetime import timedelta as delta

import exchangelib as ex
import pandas as pd
from exchangelib import (
    DELEGATE, Account, Configuration, Credentials, FaultTolerance)

from guesttracker import getlog
from guesttracker.config import AZURE_WEB
from guesttracker.utils import fileops as fl
from guesttracker.utils.credentials import CredentialManager

# silence exchangelib naive datetime on last_modified_time info log
logging.getLogger('exchangelib.fields').setLevel(logging.WARNING)

log = getlog(__name__)


class ExchangeAccount():
    def __init__(self, gui: bool = False, login: bool = True):
        self._exch = None
        self._fldr_root, self._wo_folder = None, None

        self.cred_manager = CredentialManager(name='exchange', gui=gui)

        if login:
            self.login()

    @property
    def exchange(self):
        # exchangelib account object
        if self._exch is None:
            self._exch = self.create_account()

        return self._exch

    def login(self):
        self._exch = self.create_account()

    def create_config(self, credentials, m_config=None):
        if m_config is None:
            # failed once, use hardcoded vals
            service_endpoint = 'https://outlook.office365.com/EWS/Exchange.asmx'
            auth_type = 'basic'
            version = None
        else:
            service_endpoint = m_config.get('ews_url', None)
            auth_type = m_config.get('ews_auth_type', None)
            version = m_config.get('ews_version', None)

        config = Configuration(
            retry_policy=FaultTolerance(max_wait=40),
            credentials=credentials,
            service_endpoint=service_endpoint,
            auth_type=auth_type,
            version=version)

        return config

    def create_account(self, failcount=0, config=None, autodiscover=None):
        email, password = self.cred_manager.load_db()
        credentials = Credentials(username=email, password=password)

        # first try to load saved config from QSettings
        keys = ('ews_url', 'ews_auth_type', 'ews_version')
        m = self.cred_manager.load_multi(keys=keys)

        # don't need to autodiscover if already have saved settings
        if autodiscover is None:
            autodiscover = True if m.get('ews_url', None) is None else False

        if config is None:
            config = self.create_config(credentials=credentials, m_config=m)

        try:
            account = Account(
                primary_smtp_address=email,
                config=config,
                autodiscover=autodiscover,
                access_type=DELEGATE)  # important to be delegate, otherwise tries 'Impersonate' > doesnt work

            self.save_account_settings(account=account)
        except:
            log.warning(f'Failed creating account: {failcount}')
            failcount += 1

            if failcount == 1:
                # on first fail, need to retry with manual credentials
                config = self.create_config(credentials=credentials)  # use hardcoded
                account = self.create_account(failcount=failcount, config=config, autodiscover=False)

            elif failcount <= 2:
                account = self.create_account(failcount=failcount)
            else:
                return None

        return account

    def save_account_settings(self, account):
        if AZURE_WEB:
            return

        m = dict(
            ews_url=account.protocol.service_endpoint,
            ews_auth_type=account.protocol.auth_type,
            ews_version=account.version)

        self.cred_manager.save_multi(vals=m)

    @property
    def fldr_root(self):
        if self._fldr_root is None:
            self._fldr_root = self.exchange.root / 'Top of Information Store'

        return self._fldr_root

    @property
    def wo_folder(self):
        if self._wo_folder is None:
            self._wo_folder = self.fldr_root.glob('WO Request')

        return self._wo_folder

    def get_wo_from_email(self, unit, title):
        tz = ex.EWSTimeZone.localzone()
        maxdate = dt.now() + delta(days=-15)

        messages = self.wo_folder \
            .filter(
                datetime_received__range=(
                    tz.localize(ex.EWSDateTime.from_datetime(maxdate)),
                    tz.localize(ex.EWSDateTime.now()))) \
            .filter(subject__icontains=title) \
            .filter(subject__icontains=unit)

        expr = re.compile('WO[0-9]{7}', re.IGNORECASE)

        for msg in messages.all():
            match = re.search(expr, str(msg))
            if not match is None:
                wo = match.group(0)
                return wo


def parse_attachment(attachment, d=None, header=2):
    data = fl.from_bytes(attachment.content)
    df = pd.read_csv(data, header=header)
    df['DateEmail'] = d  # only used for dt exclusions email, it doesnt have date field
    return df


def combine_email_data(folder, maxdate, subject=None, header=2):
    a = ExchangeAccount().exchange
    fldr = a.root / 'Top of Information Store' / folder
    tz = ex.EWSTimeZone.localzone()

    # filter downtime folder to emails with date_received 2 days greater than max shift date in db
    fltr = fldr.filter(
        datetime_received__range=(
            ex.EWSDateTime.from_datetime(maxdate).astimezone(tz),
            ex.EWSDateTime.now().astimezone(tz)
        ))

    # useful if single folder contains multiple types of emails
    if not subject is None:
        fltr = fltr.filter(subject__contains=subject)

    try:
        df = pd.concat([parse_attachment(
            item.attachments[0],
            header=header,
            d=item.datetime_received.date() + delta(days=-1)) for item in fltr])
    except:
        log.warning('No emails found.')
        df = pd.DataFrame()

    return df
