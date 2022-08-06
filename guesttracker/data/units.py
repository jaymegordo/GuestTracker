import sys
from datetime import timedelta as delta

import pandas as pd

from smseventlog import config as cf
from smseventlog import errors as er
from smseventlog import getlog
from smseventlog.database import db

log = getlog(__name__)

m = dict(imptable='UnitSMRImport', impfunc='ImportUnitSMR')
cols = ['Unit', 'DateSMR', 'SMR']
m_config = dict(
    FortHills=dict(
        subject='Trucks and Shovel Hours',
        header=2),
    BaseMine=dict(
        subject='Mine Equipment Service Meter Report',
        header=0))

log = getlog(__name__)


def process_df_smr(df: pd.DataFrame, minesite: str) -> pd.DataFrame:
    process_func = getattr(sys.modules[__name__], f'process_df_{minesite.lower()}')
    return df.pipe(process_func)


def import_unit_hrs_email(minesite: str) -> None:
    from smseventlog.utils.exchange import combine_email_data
    maxdate = db.max_date_db(table='UnitSMR', field='DateSMR', minesite=minesite) + delta(days=1)

    df = combine_email_data(
        folder='SMR',
        maxdate=maxdate,
        **m_config.get(minesite, {}))

    if not df is None and len(df) > 0:
        df = df.pipe(process_df_smr, minesite=minesite)

    rowsadded = db.insert_update(a='UnitSMR', df=df)


def import_unit_hrs_email_all(email: bool = True) -> None:
    """Import SMR emails for multiple minesites

    Examples
    --------
    >>> un.import_unit_hrs_email_all(email=False)
    """
    minesites = ('FortHills', 'BaseMine')

    for minesite in minesites:
        try:
            if email:
                import_unit_hrs_email(minesite=minesite)
            else:
                import_smr_local(minesite=minesite)

        except:
            raise
            er.log_error(log=log, msg=f'Failed to import SMR email for: {minesite}', discord=True)


def import_smr_local(minesite: str) -> None:
    """Temp import unit smr data from local folder"""

    p = cf.desktop / 'smr'
    m = m_config[minesite]
    subject = m.get('subject')

    lst_paths = list(p.glob(f'*{subject}*.csv'))

    dfs = [pd.read_csv(p_csv, header=m.get('header')) for p_csv in lst_paths]

    df = pd.concat(dfs) \
        .pipe(process_df_smr, minesite=minesite)

    rowsadded = db.insert_update(a='UnitSMR', df=df)

    for p in lst_paths:
        p.unlink()


def process_df_forthills(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return None

    m_rename = dict(
        SAP_Parameter='Unit',
        Date='DateSMR',
        Hours='SMR')

    return df \
        .rename(columns=m_rename)[list(m_rename.values())] \
        .pipe(lambda df: df[df.Unit.str.contains('SMR')]) \
        .pipe(db.fix_customer_units, col='Unit') \
        .assign(
            DateSMR=lambda x: pd.to_datetime(x.DateSMR, format='%Y%m%d'),
            SMR=lambda x: x.SMR.str.replace(',', '').astype(float).astype(int)) \
        .pipe(lambda df: df[df.Unit.str.contains('F3')]) \
        .reset_index(drop=True)


def process_df_basemine(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return None

    return df[['Unit', 'BaseSmr', 'BaseReadTime']] \
        .dropna() \
        .rename(columns=dict(BaseSmr='SMR', BaseReadTime='DateSMR')) \
        .assign(
            Unit=lambda x: x.Unit.astype(str).replace(r'\.0', '', regex=True),
            SMR=lambda x: x.SMR.str.replace(',', '').astype(int),
            DateSMR=lambda x: pd.to_datetime(x.DateSMR).dt.date) \
        .pipe(db.filter_database_units)


def read_unit_hrs(p):
    df = pd.read_excel(p, engine='openpyxl')
    columns = cols.append('Description')
    df.columns = columns

    df.SMR = df.SMR.astype(int)
    df.Unit = df.Unit.str.replace('F0', 'F').replace('^0', '', regex=True)
    df = df[df.Unit.str.startswith('F') | df.Description.str.contains('SMR')]
    df.drop(columns='Description', inplace=True)

    return df


def update_comp_smr():
    from smseventlog.gui.dialogs import base as dlgs
    try:
        cursor = db.cursor
        res = cursor.execute('updateUnitComponentSMR').fetchall()[0]
        cursor.commit()
    finally:
        cursor.close()

    unit_hrs, comp_hrs = res[0], res[1]
    msg = f'Unit SMR updated: {unit_hrs}\nComponent SMR updated: {comp_hrs}'
    dlgs.msg_simple(msg=msg)
