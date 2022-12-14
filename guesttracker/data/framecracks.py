"""
    Instructions to process/merge SMS and Suncor frame crack data, to be completed monthly for FleetMonthlyReport

    1. Load frame cracks from sap (load_framecracks.vbs) + copy to clipboard
    2. Make sure SMS data has been categorized front, mid, rear from eventlog
    3. run pre_process_framecracks (copies merged data to clipboard)
        - NOTE make sure d_lower set correctly for last time processed
        >>> from guesttracker.data import framecracks as frm
            df = frm.pre_process_framecracks(d_lower=None)
        OR
        >>> make framecracks
    4. paste data back to working xlsx file and remove duplicates/split rows as needed
    5. Paste WO numbers back into EventLog to merge properly in future
    6. Once categorization complete, reset vals in _merge column to 'old'
"""

import re

import numpy as np
import pandas as pd

from guesttracker import config as cf
from guesttracker import delta, dt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.database import db
from guesttracker.queries import FrameCracks
from guesttracker.utils import fileops as fl
from jgutils import pandas_utils as pu

log = getlog(__name__)

p_frm_excel = cf.p_res / 'csv/FH Frame Cracks History.xlsx'


def load_df_smr(d_lower=None):
    if d_lower is None:
        d_lower = dt(2018, 1, 1)

    sql = f"select a.* \
        FROM Unitsmr a \
        LEFT JOIN UnitID b on a.unit=b.unit \
    Where b.MineSite='FortHills' and \
    a.datesmr>='{d_lower}'"

    return pd.read_sql(sql=sql, con=db.engine) \
        .pipe(pu.parse_datecols) \
        .pipe(f.lower_cols) \
        .rename(columns=dict(date_smr='date_added')) \
        .sort_values(['date_added'])


def format_int(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    return df


def load_df_sun():
    """Read suncor frame crack data from clipboard
    - Load data in sap then copy"""
    # p = Path('/Users/Jayme/OneDrive/Desktop/Import/Frame Cracks/Frame Cracks.xlsx')
    # df = pd \
    #     .read_excel(p, parse_dates=['date_created'], engine='openpyxl') \
    #     .pipe(format_int, cols=('order', 'notification'))

    # df['unit'] = df['Functional Loc.'].str.split('-').str[0].str.replace('F0', 'F')

    cols = ['notification', 'order', 'description', 'date_created', 'floc']
    return pd.read_clipboard(names=cols, header=None) \
        .pipe(pu.parse_datecols) \
        .pipe(format_int, cols=('order', 'notification')) \
        .assign(
            unit=lambda x: x.floc.str.split('-').str[0].str.replace('F0', 'F'))


def load_df_old():
    # load processed + combined history
    df = pd \
        .read_excel(p_frm_excel, engine='openpyxl') \
        .pipe(format_int, cols=('order', 'notification', 'smr'))

    df['_merge'] = 'old'
    return df


def load_processed_excel():
    # reload processed excel file
    df = load_df_old() \
        .drop(columns='_merge')
    df = df[df.location != 'x']
    df['Month'] = df.date.dt.to_period('M')

    return df


def merge_sms_sun(df_sms, df_smr):

    # df_sms  = df_sms.rename(columns=dict(SuncorWO='order'))
    # df_sun.order = df_sun.order.astype('Int64')
    d_lower = '2018-01-01'

    # merge on order when exists, then append others
    # df = df_sun[df_sun.order.notnull()] \
    # .merge(right=df_sms[df_sms.order.notnull()], how='outer', on=['order', 'unit'], indicator=True) \
    # .assign(_merge=lambda x: x._merge.astype(str)) \
    df = df_sms[df_sms.order.notnull()]  \
        .pipe(lambda df: pd.concat([df, df_sms[df_sms.order.isnull()]])) \
        .assign(floc='', _merge='')
    # .append(df_sun[df_sun.order.isnull()]) \

    # mark rows not merged as new still
    df.loc[df['_merge'].isnull(), '_merge'] = 'New'

    # use date_added > prefer sms, then sun
    # df.date_added = np.where(df.date_added.notnull(), df.date_added, df['date_created'])
    df = df \
        .query(f'date_added >= "{d_lower}"') \
        .sort_values(['date_added']) \
        # .drop(columns=['date_created']) \

    # return df
    # display(df_smr.head())
    # display(df.head())

    # merge smr where missing
    df = pd.merge_asof(left=df, right=df_smr, on='date_added', by='unit', direction='nearest')
    # return df
    df['smr'] = np.where(df.smr_x.notnull(), df.smr_x, df.smr_y)
    df.smr = df.smr.astype('Int64')

    df = df.drop(columns=['smr_x', 'smr_y']) \
        .sort_values(['unit', 'date_added']) \
        .reset_index(drop=True)

    # tag dumpbody location using floc
    df.loc[df.floc == 'STRUCTR-BODYGP-DUMPBODY', 'location'] = 'Dumpbody'

    # try and get location from sms WO Comments, else NaN
    lst = ['front', 'mid', 'rear', 'dumpbody', 'deck', 'handrail']
    pat = '({})'.format('|'.join([fr'\({item}\)' for item in lst]))  # match value = "(rear)"
    df['location'] = df.wo_comments \
        .str.extract(
            pat=pat,
            expand=False,
            flags=re.IGNORECASE) \
        .str.replace(r'[()]', '', regex=True).str.title()

    # also match on issue==Frame + failure category==crack > chose sub_category eg Rear
    df.loc[(df.issue_category == 'Frame') & (df.cause == 'Crack'), 'location'] = df.sub_category

    return df.rename(columns=dict(date_added='date'))


def pre_process_framecracks(d_lower=None, open_=True):
    if d_lower is None:
        d_lower = dt.now() + delta(days=-31)

    df_old = load_df_old()

    # load new data, merge
    query = FrameCracks(da=dict(d_lower=d_lower))
    df_sms = query.get_df() \
        .pipe(f.lower_cols)

    # df_sun = load_df_sun()
    df_smr = load_df_smr(d_lower=d_lower)

    df_new = merge_sms_sun(df_sms, df_smr) \
        .drop(columns=['issue_category', 'sub_category', 'cause'])

    # remove rows where order already exists in df_old
    df_new = df_new[(~df_new.order.isin(df_old.order) | df_new.order.isnull())]

    # concat old + new, drop duplicates
    # remember to keep first item (description, wo_comments) in SMS as original, so later duplicates are dropped
    df = pd.concat([df_old, df_new]) \
        .drop_duplicates(
            subset=[
                'order', 'description', 'title', 'unit', 'tsi_details', 'wo_comments', 'location'],
            keep='first') \
        .pipe(format_int, cols=('order', 'notification', 'smr')) \
        .reset_index(drop=True)

    # convert datetime to date
    df['date'] = df['date'].dt.date

    # copy to clipboard, paste back into sheet (could use xlwings to write to table but too much work)
    df[df['_merge'] != 'old'].to_clipboard(index=False, header=False, excel=True)

    log.info(f'Loaded new rows:\n\tdf_sms: {df_sms.shape}, Total: {len(df_new)} \
        \n\tNew Merged: {len(df) - len(df_old)}')

    if open_:
        fl.open_folder(p_frm_excel)

    return df


def df_smr_bin(df: pd.DataFrame) -> pd.DataFrame:
    """create bin labels for data at 1000hr smr intervals"""
    freq = 1000
    df['smr_bin'] = pd \
        .cut(df.smr, bins=pd.interval_range(start=0, end=df.smr.max() + freq, freq=freq))

    return df \
        .groupby(['smr_bin', 'location']) \
        .size() \
        .reset_index(name='Count') \
        .rename(columns=dict(smr_bin='SMR Bin', location='Location'))


def df_smr_avg(df):
    # get avg smr for each category
    df1 = df.groupby('location') \
        .agg(dict(smr='mean', location='size')) \
        .rename(columns=dict(smr='Mean SMR', location='Count')) \
        .reset_index()

    df1['Mean SMR'] = df1['Mean SMR'].astype(int)

    return df1


def df_month(df):
    # group into monthly bins
    return df \
        .groupby(['Month', 'location']) \
        .size() \
        .reset_index(name='Count') \
        .rename(columns=dict(location='Location'))
