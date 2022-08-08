import argparse
import subprocess

from guesttracker import database as dtb
from guesttracker import delta, dt, getlog
from guesttracker import queries as qr

# from guesttracker.data import framecracks as frm
# from guesttracker.queries.misc import ACMotorInspections
# from guesttracker.reports import SMRReport

if True:
    from jgutils.jgutils.secrets import SecretsManager

log = getlog(__name__)
cli = argparse.ArgumentParser()

cli.add_argument(
    '--write_dbconfig',
    default=False,
    action='store_true',
    help='Create dbmodel.py')

cli.add_argument(
    '--encrypt_creds',
    default=False,
    action='store_true',
    help='Re-encrypt credentials')

cli.add_argument(
    '--ac_inspect',
    default=False,
    action='store_true',
    help='Show 3k hr ac motor inspections which need to be scheduled')

cli.add_argument(
    '--smr',
    default=False,
    action='store_true',
    help='Create SMR report for current month.')

cli.add_argument(
    '--framecracks',
    type=str,
    default=None,
    nargs='?',
    const=qr.first_last_month(d=dt.now() + delta(days=-31))[0],
    help='Update Excel Framecracks file.')

cli.add_argument(
    '--update_exch_pw',
    type=str,
    default=None,
    help='Update exchange password in db'
)


if __name__ == '__main__':
    a = cli.parse_args()

    if a.write_dbconfig:
        con_str = dtb.str_conn()
        args = [
            'poetry',
            'run',
            'sqlacodegen',
            con_str,
            '--outfile',
            'guesttracker/utils/dbmodel.py']

        subprocess.run(args)

    elif a.encrypt_creds:
        SecretsManager().encrypt_all_secrets()

    elif a.update_exch_pw:
        from guesttracker.utils.credentials import CredentialManager
        CredentialManager('exchange', gui=False).update_password_db(password=a.update_exch_pw)
