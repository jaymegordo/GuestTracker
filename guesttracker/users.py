import os

from smseventlog import VERSION
from smseventlog import config as cf
from smseventlog import dbtransaction as dbt
from smseventlog import dt
from smseventlog import errors as er
from smseventlog import getlog
from smseventlog.database import db, get_odbc_driver
from smseventlog.gui import _global as gbl
from smseventlog.utils.dbmodel import UserSettings

log = getlog(__name__)


class User():
    def __init__(self, username: str, mainwindow=None):
        self.row, self._e = None, None
        self.username = username
        self.dbtable = UserSettings
        self.domain = os.getenv('userdomain', None)
        # domain = 'CED'
        self.usergroup = db.domain_map_inv.get(self.domain, 'SMS')
        self.new_user = False
        self.is_admin = gbl.get_setting('is_admin', False)
        # is_admin = True if username in ('Jayme Gordon',) else False
        # is_admin = False

        if not mainwindow is None:
            s = mainwindow.settings
            self.email = s.value('email', '')
            self.minesite = mainwindow.minesite
        else:
            self.email = ''
            self.minesite = ''

        # Disable everything for those idiots over at cummins
        self.is_cummins = True if (not self.domain is None and 'CED' in self.domain) or 'cummins' in self.email.lower(
        ) or self.usergroup == 'Cummins' else False

    @classmethod
    def default(cls):
        return cls(username='Jayme Gordon')

    @property
    def e(self):
        # get existing user row from db, or create new
        if self._e is None:
            self._e = self.load()

            if self._e is None:
                self._e = self.create_new_user()
                self.new_user = True

        return self._e

    def load(self):
        self.row = dbt.Row(dbtable=self.dbtable, keys=dict(UserName=self.username))
        return self.row.create_model_from_db()

    def create_new_user(self):
        e = self.dbtable()
        e.UserName = self.username
        e.Email = self.email
        e.NumOpens = 0

        return e

    def update_vals(self, e) -> None:
        """Update user row current settings before commiting to db"""
        e.LastLogin = dt.now()
        e.Ver = VERSION
        e.NumOpens += 1
        e.Domain = self.domain
        e.UserGroup = self.usergroup
        e.MineSite = self.minesite
        e.odbc_driver = get_odbc_driver()
        e.install_dir = str(cf.p_root)

    def login(self) -> 'User':
        """create user row in UserSettings if doesn't exist

        Returns
        -------
        User
            self
        """
        try:
            e = self.e
            self.update_vals(e=e)

            # no user in db
            if self.new_user:
                db.session.add(e)

            db.safe_commit(fail_msg='User failed to login.')
        except Exception:
            er.log_error(log=log)
        finally:
            return self
