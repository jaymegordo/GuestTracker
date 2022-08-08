import re
from collections import defaultdict as dd
from pathlib import Path
from timeit import default_timer as timer
from typing import *

import pandas as pd

from guesttracker import config as cf
from guesttracker import dbtransaction as dbt
from guesttracker import dt
from guesttracker import errors as er
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker import styles as st
from guesttracker.database import db
from guesttracker.gui.dialogs import base as dlgs
from guesttracker.utils import dbmodel as dbm
from jgutils.jgutils import pandas_utils as pu

log = getlog(__name__)

# NOTE could make FCImport object to store results better


@er.errlog('Incorrect unit input', status_msg=True)
def parse_units(units: str, prefix: str = None) -> list:
    """Parse string list/range of units, add prefix

    Parameters
    ----------
    units : str
        list/range of units
    prefix : str, optional
        prefix to append to each unit, default None

    Returns
    -------
    list
        list of parsed units
    Bool
        False if unit in range cannot be converted to int

    Examples
    -------
    >>> fc.parse_units('301, 302--303', 'F')
    >>> ['F301', 'F302', 'F303']
    """

    # parse unit_range string
    units = units.split(',')

    # get upper/lower range if str has '-'
    units_range = list(filter(lambda x: '--' in x, units))
    units = list(filter(lambda x: not '--' in x, units))

    for _rng in units_range:
        rng = _rng.split('--')
        try:
            lower = int(rng[0])
            upper = int(rng[1])
        except Exception as e:
            msg = f'Values for range must be integers: "{_rng.strip()}"'
            dlgs.msg_simple(msg=msg, icon='warning')
            return False

        units.extend([unit for unit in range(lower, upper + 1)])

    # strip whitespace, convert to str, add prefix
    if prefix is None:
        prefix = ''
    units = list(map(lambda x: f'{prefix}{str(x).strip()}', units))

    return units


def create_fc_manual(
        units: list,
        fc_number: str,
        _type: str,
        subject: str,
        release_date: dt,
        expiry_date: dt,
        **kw) -> None:
    """Create manual FC from input dialog"""

    df = db.get_df_unit() \
        .pipe(lambda df: df[df.Unit.isin(units)])[['Model', 'Serial']] \
        .reset_index() \
        .assign(
            FCNumber=fc_number,
            Subject=subject,
            Classification=_type,
            StartDate=release_date,
            EndDate=expiry_date)

    import_fc(df=df, **kw)


def tblcount(tbl):
    cursor = db.cursor
    sql = f'Select count(*) From {tbl}'
    return cursor.execute(sql).fetchval()


def import_fc(
        lst_csv: List[Path] = None,
        upload: bool = True,
        df: pd.DataFrame = None,
        worker_thread: bool = False) -> Union[Tuple[str, str, List[Path]], None]:
    """Import records from fc csv to database

    Parameters
    ----------
    lst_csv : List[Path], optional
        list of csv files, by default None
    upload : bool, optional
        upload, or just return df, by default True
    df : pd.DataFrame, optional
        df to import if no list_csv, by default None
    worker_thread : bool, optional
        is import done in worker thread, by default False

    Returns
    -------
    Union[Tuple[str, str, List[Path]], None]
        - msg to display
        - status message (time taken)
        - list of files to delete
    """
    start = timer()

    if df is None:
        df = pd.concat([read_fc(p=p) for p in lst_csv], sort=False)

    # save total number before filtered
    num_rows_all = len(df)

    # filter db units
    df = df.pipe(db.filter_database_units) \
        .drop_duplicates(['FCNumber', 'Unit'])

    num_rows_units = len(df)

    if not lst_csv is None:
        log.info('Loaded ({}) FCs from {} file(s) in: {}s'
                 .format(len(df), len(lst_csv), f.deltasec(start, timer())))

    # import to temp staging table in db, then merge new rows to FactoryCampaign
    if upload:
        cursor = db.cursor
        df.to_sql(name='FactoryCampaignImport', con=db.engine, if_exists='append', index=False)

        msg = f'Rows read from import files: {num_rows_all:,.0f}' \
            + f'\nRows matched to units in database: {num_rows_units:,.0f}'

        try:
            # FactoryCampaign Import
            rows = dd(int, cursor.execute('mergeFCImport').fetchall())
            m = {
                'New rows imported': 'INSERT',
                'Rows updated': 'UPDATE',
                'KA Completion dates added': 'KADatesAdded'}

            msg += '\n\nFactoryCampaign:' + ''.join([f'\n\t{k}: {rows[v]:,.0f}' for k, v in m.items()])

            # FC Summary - New rows added
            rows = dd(int, cursor.execute('MergeFCSummary').fetchall())
            if cursor.nextset():
                msg += '\n\nFC Summary: \n\tRows added: {} \n\n\t'.format(rows['INSERT'])
                df2 = f.cursor_to_df(cursor)
                if len(df2) > 0:
                    msg += st.left_justified(df2).replace('\n', '\n\t')

                    for fc_number in df2.FCNumber:
                        create_fc_folder(fc_number=fc_number)

            cursor.commit()
        except Exception as e:
            er.log_error(log=log)
            dlgs.msg_simple(msg='Couldn\'t import FCs!', icon='critical')
        finally:
            cursor.close()

        statusmsg = 'Elapsed time: {}s'.format(f.deltasec(start, timer()))
        if worker_thread:
            return msg, statusmsg, lst_csv
        else:
            ask_delete_files(msg, statusmsg, lst_csv)


def ask_delete_files(
        msg: str = None,
        statusmsg: str = None,
        lst_csv: list = None) -> None:

    if msg is None:
        return

    if isinstance(msg, tuple):
        # came back from worker thread # NOTE kinda ugly
        msg, statusmsg, lst_csv = msg[0], msg[1], msg[2]

    msg += '\n\nWould you like to delete files?'
    if dlgs.msgbox(msg=msg, yesno=True, statusmsg=statusmsg, max_width=2000):
        for p in lst_csv:
            Path(p).unlink()


def create_fc_folder(fc_number: str) -> None:
    try:
        p = cf.p_drive / cf.config['FilePaths']['Factory Campaigns'] / fc_number
        p.mkdir(parents=True)
    except Exception as e:
        log.warning(f'Couldn\'t make fc path for: {fc_number}')


def select_fc(unit: str):
    """Show dialog to select FC from list

    Parameters
    ----------
    unit : str
        unit number

    Returns
    -------
    (bool, str, str)
        okay_status, fc_number, title
    """

    df = db.get_df_fc(default=True, unit=unit)

    ok, val = dlgs.inputbox(
        msg='Select FC:',
        dtype='choice',
        items=list(df.Title),
        editable=True,
        title='Select FC')

    fc_number, title = None, None

    if ok:
        fc_number = val.split(' - ')[0]
        title = f'FC {val}'

    return ok, fc_number, title


def link_fc_db(unit: str, uid: float, fc_number: str):
    """Add event's UID to FC in FactoryCampaign table"""
    row = dbt.Row(keys=dict(FCNumber=fc_number, Unit=unit), dbtable=dbm.FactoryCampaign)
    row.update(vals=dict(UID=uid))


def read_fc(p: Path) -> pd.DataFrame:
    """
    Read FC csv from KA
    - Removes units not in db
    """
    # Drop and reorder,  Don't import: CompletionSMR, claimnumber, ServiceLetterDate
    cols = ['FCNumber', 'Model', 'Serial', 'Unit', 'StartDate', 'EndDate',
            'DateCompleteKA', 'Subject', 'Classification', 'Branch', 'Status']

    dtypes = dict(Model='object', Serial='object', Unit='object')

    # NOTE only model colulmn has '\t' so far, but would be better to apply to all str cols
    # merges db units on Model + Serial (must be exact!!)
    expr = '|'.join('=" ')
    dfu = db.get_df_unit()
    match = ['Model', 'Serial']
    return f.read_csv_firstrow(p, dtype=dtypes, skipinitialspace=True) \
        .rename(columns=cf.config['Headers']['FCImport']) \
        .assign(Model=lambda x: x.Model.str.replace(expr, '', regex=True)) \
        .merge(right=dfu[['Model', 'Serial', 'Unit']], how='left', left_on=match, right_on=match) \
        .reset_index(drop=True)[cols]


def import_ka():
    # One time import of machine info from mykomatsu
    p = Path('C:/Users/jayme/OneDrive/Desktop/KA Machine Info')
    lst = [f for f in p.glob('*.html')]

    df = pd.concat([read_ka(p=p) for p in lst], sort=False).reset_index(drop=True)

    return df


def read_ka(p):
    from bs4 import BeautifulSoup
    with open(p) as html:
        table = BeautifulSoup(html).findAll('table')[1]

    cols = ['Unit', 'Model', 'Serial', 'LastSMR']
    data = [[col.text.replace('\n', '').replace('\t', '') for col in row.findAll('td')[2:6]]
            for row in table.findAll('tr')]
    df = pd.DataFrame(data=data, columns=cols)
    df = df[df.Serial.str.len() > 2].reset_index(drop=True)
    df.LastSMR = pd.to_datetime(df.LastSMR, format='%m/%d/%Y %H:%M %p')

    return df


def update_scheduled_db(df, **kw):
    """Update cleaned df scheduled status in FactoryCampaign db table

    Parameters
    ----------
    df : pd.DataFrame
        cleaned df
    """
    from guesttracker import dbtransaction as dbt
    from guesttracker.utils.dbmodel import FactoryCampaign

    dbt.DBTransaction(dbtable=FactoryCampaign, table_view=False, **kw) \
        .add_df(df=df, update_cols='Scheduled') \
        .update_all()


def update_scheduled_excel(df, scheduled=True):
    """Update "Scheduled" status in FactoryCampaign

    Parameters
    ----------
    df : pd.DataFrame
        df from "SAP Notification Duplicator" table, copied from clipboard
    scheduled : bool
        update FCs to be scheduled or not, default True

    Examples
    --------
    >>> df = pd.read_clipboard()
    >>> fc.update_scheduled(df=df)
    """

    # parse FC number from title
    # NOTE may need to replace prefixes other than 'FC' eg 'PSN'
    df = df \
        .pipe(f.lower_cols) \
        .dropna() \
        .assign(
            Unit=lambda x: x.unit.str.replace('F0', 'F'),
            FCNumber=lambda x: x.title.str.split(' - ', expand=True)[1]
            .str.replace('FC', '')
            .str.replace('PSN', '')
            .str.strip(),
            Scheduled=scheduled)[['Unit', 'FCNumber', 'Scheduled']]

    update_scheduled_db(df=df)


def update_scheduled_sap(df=None, exclude=None, **kw):
    """Update scheduled fc status from sap data
    - copy table in sap, then this func will read clipboard
    - NOTE rejected notifications get status COMP
    - NOTE use sap layout FC_LAYOUT

    Parameters
    ----------
    df : pd.DataFrame
        df, default None
    kw :
        used here to pass active table widget to dbtxn for update message

    Examples
    --------
    >>> fc.update_scheduled_sap(exclude=['SMSFH-008'])
    """
    if exclude is None:
        exclude = []

    # if exclude vals came from gui dialog
    if isinstance(exclude, tuple):
        ans = exclude[0]  # 1 or 0 for dialog accept/reject
        exclude = exclude[1] if ans == 1 else []

    # split string items to list
    if isinstance(exclude, str):
        exclude = [item.strip() for item in exclude.split(',')]

    df_fc = db.get_df_fc(default=False)

    # read df data from clipboard
    if df is None:
        cols = ['notification', 'order', 'date_created', 'date_reqd',
                'title', 'status', 'floc', 'workctr', 'created_by']
        df = pd.read_clipboard(names=cols, header=None) \
            .pipe(pu.parse_datecols)

    # extract FCNumber from description with regex expr matching FC or PSN then fc_number
    # F0314 FC[19H055-1] CHANGE STEERING BRACKET
    # F0314 PSN[ 19H055-1] CHANGE STEERING BRACKET
    # https://stackoverflow.com/questions/20089922/python-regex-engine-look-behind-requires-fixed-width-pattern-error
    expr = r'(?:(?<=FC)|(?<=PSN))(\s*[^\s]+)'
    expr2 = r'^[^a-zA-Z0-9]+'  # replace non alphanumeric chars at start of string eg '-'

    # set Scheduled=True for any rows which dont have 'RJCT'
    df = df \
        .assign(
            FCNumber=lambda x: x.title
            .str.extract(expr, flags=re.IGNORECASE)[0]
                .str.strip()
                .str.replace(expr2, '')
                .str.upper(),
            Unit=lambda x: x.floc
                .str.split('-', expand=True)[0]
                .str.replace('F0', 'F'),
            Scheduled=lambda x: ~x.status.astype(str).str.contains('rjct|comp|rtco', case=False)) \
        .sort_values(by=['Unit', 'FCNumber', 'date_created'], ascending=[True, True, False]) \
        .drop_duplicates(subset=['Unit', 'FCNumber'], keep='first') \
        .pipe(lambda df: df[~df.FCNumber.isin(exclude)]) \
        .pipe(lambda df: df[
            df.FCNumber.str.lower().isin(
                df_fc['FC Number'].str.lower())])[['Unit', 'FCNumber', 'Scheduled']]

    update_scheduled_db(df=df, **kw)
