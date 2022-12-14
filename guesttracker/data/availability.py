"""
Functions to import process raw availability data from suncor exports
- Not imported by GUI app
"""

from datetime import datetime as dt
from datetime import timedelta as delta
from pathlib import Path

import pandas as pd
import pypika as pk
from pypika import MSSQLQuery as Query

from guesttracker import config as cf
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.database import db
from guesttracker.utils.exchange import combine_email_data
from jgutils import pandas_utils as pu

log = getlog(__name__)


def import_single(p):
    df = pd.read_csv(p, header=2)
    df = process_df_downtime(df=df)
    rowsadded = db.insert_update(a='Downtime', df=df)


def import_downtime_email():
    maxdate = db.max_date_db(table='Downtime', field='ShiftDate') + delta(days=2)
    df = combine_email_data(folder='Downtime', maxdate=maxdate, subject='Equipment Downtime')
    df = process_df_downtime(df=df)
    rowsadded = db.insert_update(a='Downtime', df=df)


def import_dt_exclusions_email():
    maxdate = db.max_date_db(table='DowntimeExclusions', field='Date') + delta(days=2)
    df = combine_email_data(
        folder='Downtime',
        maxdate=maxdate,
        subject='Equipment Availability',
        header=0)

    df = process_df_exclusions(df=df)
    rowsadded = db.insert_update(a='DowntimeExclusions', df=df)


def import_avail_local(p: Path = None) -> None:
    """Temp import avail data from local folder"""
    if p is None:
        p = cf.desktop / 'downtime'
        lst_paths = list(p.glob('*.csv'))
        dfs = [pd.read_csv(p_csv, header=3) for p_csv in lst_paths]
        df = pd.concat(dfs)
    else:
        df = pd.read_csv(p, header=3)
        lst_paths = []

    df = df \
        .pipe(process_df_downtime)

    rowsadded = db.insert_update(a='Downtime', df=df)

    for p in lst_paths:
        p.unlink()


def create_dt_exclusions(dates, rng_unit):
    # manually create exclusion hrs when emails didn't get sent
    # dates is list of dates
    df = pd.concat([create_dt_exclusions_single(date, rng_unit) for date in dates])
    return df


def create_dt_exclusions_single(date, rng_unit, hrs=24):
    df = pd.DataFrame(columns=['Unit', 'Date', 'Hours', 'MA'])
    df.Unit = range_units(rng_unit)
    df.Hours = hrs
    df.MA = 1
    df.Date = date
    return df


def range_units(rng):
    # rng is tuple of first-last units eg (317, 330)
    return tuple([f'F{unit}' for unit in range(rng[0], rng[1] + 1)])


def process_df_exclusions(df: pd.DataFrame) -> pd.DataFrame:
    # convert csv from suncor to database format
    if df is None:
        return None

    m_cols = dict(
        EqmtUnit='Unit',
        TC08='OutOfSystem',
        Duration='Total',
        DateEmail='Date')

    # hours = actual out of system hrs
    # units can only have MAX 24 hrs out of system
    # assume all exclusions always apply to MA and PA, will have to manually change when needed
    return df \
        .rename(columns=m_cols)[list(m_cols.values())] \
        .pipe(db.fix_customer_units, col='Unit') \
        .assign(
            Date=lambda x: pd.to_datetime(x.Date),
            Hours=lambda x: 24 - (x.Total - x.OutOfSystem),
            MA=1) \
        .query('Hours > 0') \
        .drop(columns=['OutOfSystem', 'Total'])


def update_dt_exclusions_ma(units, rng_dates=None, dates=None):
    # set MA=0 (False) for units in given date range range

    t = pk.Table('DowntimeExclusions')
    cond = [t.Unit.isin(units)]

    if not rng_dates is None:
        cond.append(t.Date.between(*rng_dates))

    if not dates is None:
        cond.append(t.Date.isin(dates))

    q = Query().update(t).set(t.MA, 0).where(pk.Criterion.all(cond))
    sql = q.get_sql()

    cursor = db.cursor
    rows = cursor.execute(sql).rowcount
    cursor.commit()
    print(f'Rows updated: {rows}')


def process_df_downtime(df: pd.DataFrame) -> pd.DataFrame:
    """Process raw downtime data"""
    if df is None or len(df) == 0:
        return

    m_cols = dict(
        FieldId='Unit',
        StartDate='StartDate',
        EndDate='EndDate',
        Duration='Duration',
        Reason='DownReason',
        FieldComment='Comment',
        ShiftDate='ShiftDate',
        Origin='Origin')

    return df \
        .pipe(lambda df: df[df.EqmtModel == 'Komatsu 980E-OS']) \
        .assign(
            FieldId=lambda x: x.FieldId.str.replace('F0', 'F'),
            ShiftDate=lambda x: pd.to_datetime(x.FullShiftName.str.split(' ', expand=True)[0], format='%d-%b-%Y'),
            Moment=lambda x: pd.to_timedelta(x.Moment),
            StartDate=lambda x: x.apply(lambda x: parse_date(x.ShiftDate, x.Moment), axis=1),
            EndDate=lambda x: x.StartDate + pd.to_timedelta(x.Duration),
            Duration=lambda x: pd.to_timedelta(x.Duration).dt.total_seconds() / 3600) \
        .drop_duplicates(subset=['FieldId', 'StartDate', 'EndDate']) \
        .rename(columns=m_cols)[list(m_cols.values())]


def parse_date(shiftdate, timedelta):
    # if timedelta is < 6am, shift day is next day
    if timedelta.total_seconds() < 3600 * 6:
        shiftdate += delta(days=1)

    return shiftdate + timedelta


def ahs_pa_monthly():
    df = pd.read_sql_table(table_name='viewPAMonthly', con=db.engine)
    df = df.pivot(index='Unit', columns='MonthStart', values='Sum_DT')
    return df


def weekly_dt_exclusions_update(d_rng=None):
    """create all units with MA hrs below hrs in period"""
    from guesttracker.data.internal import utils as utl

    # units = []
    # units.extend(utl.all_units(rng=(300,322)))
    # units.extend(utl.all_units(rng=(323,348)))
    units = utl.all_units()

    if d_rng is None:
        d_rng = (dt(2020, 8, 7), dt(2020, 8, 23))

    update_dt_exclusions_ma(units=units, rng_dates=d_rng)


def convert_fh_old(p=None):

    if p is None:
        p = Path('/Users/Jayme/Desktop/downtime_old.csv')

    m = dict(
        EqmtID='Unit',
        StartTimeDT='StartDate',
        EndDate='EndDate',
        Duration='Duration',
        ShiftDate='ShiftDate',
        Reason='Comment',
        SMS='SMS',
        Suncor='Suncor',
        Origin='Origin'
    )

    df = pd.read_csv(p, parse_dates=['StartTimeDT']) \
        .rename(columns=m) \
        .fillna(dict(SMS=0, Suncor=0)) \
        .assign(
            Duration=lambda x: x.SMS + x.Suncor,
            EndDate=lambda x: (x.StartDate + pd.to_timedelta(x.Duration, unit='h')).round('1s'),
            ShiftDate=lambda x: x.StartDate.round('1d'),
            Origin='Staffed')[list(m.values())] \
        .drop_duplicates(subset=['Unit', 'StartDate', 'EndDate'])

    return df


def calc_eng_dt():
    """Just saving some code to calc dt from specific date for specific units"""
    from guesttracker.queries import AvailSummary
    units = """
    F301
    F304
    F308
    F316
    F327
    F337
    F341
    F343
    """.split()
    dates = """
    2019-09-29
    2019-09-21
    2019-09-18
    2019-09-01
    2019-10-07
    2019-09-30
    2019-10-16
    2019-09-30
    """.split()

    df = pd.DataFrame.from_dict(dict(unit=units, date_upgrade=dates)) \
        .pipe(pu.parse_datecols) \
        .assign(
            start_date=lambda x: x.date_upgrade.dt.to_period('W').dt.end_time.dt.date + delta(days=1),
            end_date=dt(2021, 3, 14))

    dfs = []

    for row in df.itertuples():
        # d_rng = (dt(2019,9,30), dt(2021,3,14))
        d_rng = (row.start_date, row.end_date)
        unit = row.unit
        query = AvailSummary(d_rng=d_rng, unit=unit, period='week')
        df2 = query.get_df()

        df2 = df2 \
            .groupby('Unit') \
            .agg(**query.agg_cols(df2)) \
            .pipe(f.lower_cols) \
            .rename_axis('unit')

        dfs.append(df2)

    return df \
        .set_index('unit') \
        .merge(right=pd.concat(dfs), how='right', left_index=True, right_index=True)
