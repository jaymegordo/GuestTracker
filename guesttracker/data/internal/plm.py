import time
from pathlib import Path
from typing import *

import pandas as pd
from joblib import Parallel, delayed

from smseventlog import config as cf
from smseventlog import delta, dt
from smseventlog import eventfolders as efl
from smseventlog import functions as f
from smseventlog import getlog
from smseventlog.data.internal import utils as utl
from smseventlog.database import db
from smseventlog.queries.plm import PLMUnit
from smseventlog.utils import fileops as fl

log = getlog(__name__)

m_equip = f.inverse(cf.config['EquipPaths'])


m_cols = {
    'Date': 'date',
    'Time': 'time',
    'Payload(Net)': 'payload',
    'Swingloads': 'swingloads',
    'Status Flag': 'statusflag',
    'Carry Back': 'carryback',
    'TotalCycle Time': 'cycletime',
    'L-Haul Distance': 'l_hauldistance',
    'L-Max Speed': 'l_maxspeed',
    'E MaxSpeed': 'e_maxspeed',
    'Max Sprung': 'maxsprung',
    'Truck Type': 'trucktype',
    'Tare Sprung Weight': 'sprungweight',
    'Payload Est.@Shovel(Net)': 'payload_est',
    'Quick Payload Estimate(Net)': 'payload_quick',
    'Gross Payload': 'payload_gross'}

m_cols_plm3 = {
    'Truck #': 'unit',
    'Date': 'date',
    'Time': 'time',
    'Payload (Net)': 'payload',
    'Swingloads': 'swingloads',
    'Status Flags': 'statusflag',
    'Carry Back': 'carryback',
    'Total Cycle Time': 'cycletime',
    'L-Haul Distance': 'l_hauldistance',
    'L-Max Speed': 'l_maxspeed',
    'E-Max Speed': 'e_maxspeed',
    'Max Sprung': 'maxsprung',
    'Truck Type': 'trucktype',
    'Tare Sprung Weight': 'sprungweight',
    'Payload Est. @ Shovel (Net)': 'payload_est',
    'Quick Payload Estimate (Net)': 'payload_quick',
    'Gross Payload': 'payload_gross'}

good_cols = ['unit', 'datetime']
good_cols.extend([col for col in m_cols.values() if not col in ('date', 'time')])


def update_plm_all_units(minesite='FortHills', model='980'):
    units = db.unique_units(minesite=minesite, model=model)

    # multiprocess
    job = delayed(update_plm_single_unit)
    result = Parallel(n_jobs=-1, verbose=11)(job(unit=unit, import_=False) for unit in units)

    config = utl.get_config('plm')

    # could have duplicates from file in wrong unit path, drop again to be safe
    df = pd.concat(m['df'] for m in result) \
        .drop_duplicates(subset=config['duplicate_cols'])

    rowsadded = utl.import_csv_df(
        df=df,
        ftype='plm',
        chunksize=10000)

    new_result = []
    for m in result:
        df = m['df']
        new_result.append(dict(
            unit=m['unit'],
            maxdate=m['maxdate'].strftime('%Y-%m-%d'),
            numrows=len(df)))

    return new_result


def max_date_plm(unit: str) -> dt:
    """Get max date in PLM database for specific unit

    Parameters
    ----------
    unit : str

    Returns
    -------
    dt
        max date
    """
    query = PLMUnit(unit=unit)
    maxdate = query.max_date()
    if maxdate is None:
        maxdate = dt.now() + delta(days=-731)

    return maxdate


def update_plm_single_unit(unit, import_=True, maxdate=None):
    # get max date db
    log.info(f'starting update plm, unit: {unit}')

    if maxdate is None:
        maxdate = max_date_plm(unit=unit)

    result = utl.process_files(ftype='plm', units=unit, d_lower=maxdate, import_=import_)

    # kinda sketch... bad design here
    if import_:
        rowsadded = result
        df = None
    else:
        rowsadded = None
        df = result

    m = dict(unit=unit, maxdate=maxdate, rowsadded=rowsadded, df=df)
    return m


def minesite_from_path(p: Path) -> Union[str, None]:
    """Parse file path, check if in equip paths"""
    if not isinstance(p, Path):
        p = Path(p)

    # folder not on p-drive
    if not cf.p_drive.as_posix() in p.as_posix():
        return None

    lst = list(filter(lambda x: x[0] in p.as_posix(), m_equip.items()))
    if lst:
        return lst[0][1]
    else:
        log.warning(f'Couldn\'t get minesite in path: {p}')
        return None


def read_plm_wrapped(p: Path, **kw) -> Union[pd.DataFrame, None]:
    """Wrap read_plm for errors"""
    try:
        return read_plm(p, **kw)
    except Exception as e:
        msg = f'Failed plm import, {e.args[0]}: {p}'
        log.warning(msg)
        utl.write_import_fail(msg)
        return None  # if no df, return None so not merging empty df


def unit_from_haulcycle(p: Path, raise_errors: bool = True) -> Union[str, None]:
    """Get unit number from plm haulcycle file

    Parameters
    ----------
    p : Path
        csv to check
    raise_errors : bool
        raise or suppress errors if can't find unit

    Returns
    -------
    Union[str, None]
        unit number or None
    """
    log.info(f'Checking haulcycle file: {p}')

    def excep(msg):
        """Raise or ignore exception"""
        if raise_errors:
            raise Exception(msg)
        else:
            return None

    # header, try unit, then try getting unit with serial
    s_head = pd \
        .read_csv(p, nrows=6, header=None) \
        .iloc[:, 0].str.split(':', expand=True) \
        .set_index(0) \
        .rename_axis('index').T \
        .pipe(f.lower_cols) \
        .T[1].str.strip()

    unit = s_head['cust_unit'].replace(' ', '')
    unit = f.fix_suncor_unit(unit=unit.upper())
    # unit = df_head[0][1].split(':')[1].strip()

    if unit == '' or not db.unit_exists(unit):

        # check if serial + minesite present
        minesite = minesite_from_path(p)
        # try to get unit from serial/minesite
        if minesite is None:
            excep('Couldn\'t get minesite from unit path.')
        else:
            # serial = df_head[0][0].split(':')[1].upper().strip()
            unit = db.unit_from_serial(serial=s_head['frame_sn'], minesite=minesite)

            if unit is None:
                # fallback to getting unit from path
                unit = utl.unit_from_path(p)
                if not unit is None:
                    log.warning(f'Falling back to unit from path: {unit}, {p}')
                else:
                    excep('Couldn\'t read serial from plm file.')

        if not db.unit_exists(unit):
            excep(f'Unit: {unit} does not exist in db.')

    return unit


def read_plm3(p: Path):
    """Read plm3 file from csv"""
    df = pd \
        .read_csv(
            p,
            header=7,
            engine='c',
            usecols=m_cols_plm3,
            skip_blank_lines=False,
            parse_dates=[['Date', 'Time']]) \

    # plm 3 has alarmfile csv inside, need to break at first blank row
    idx_blank = df.loc[df.loc[:, df.columns != 'Date_Time'].isna().all(1)].index[0]

    return df.iloc[:idx_blank] \
        .dropna(subset=['Date_Time']) \
        .rename(columns=m_cols_plm3) \
        .assign(
            datetime=lambda x: pd.to_datetime(x['Date_Time']),
            cycletime=lambda x: x['cycletime'].apply(utl.to_seconds),
            carryback=lambda x: x['carryback'].astype(str).str.replace(' ', '').astype(float))[good_cols] \
        .pipe(lambda df: df[df.datetime <= dt.now()])


def read_plm(p: Path, unit: str = None) -> pd.DataFrame:
    """Load single plmcycle file to dataframe"""

    # can maybe pass in unit while uploading all dls as backup
    unit_backup = unit

    # still try to get unit from haulcycle first
    try:
        unit = unit_from_haulcycle(p=p)
    except Exception as e:
        # if can't read header, try plm3
        if not unit_backup is None:
            model_base = db.get_unit_val(unit=unit_backup, field='ModelBase')
        else:
            model_base = ''

        # don't try plm3 for 980s
        if not '980' in model_base:
            return read_plm3(p=p)
        else:
            raise e

    if unit is None:
        if not unit_backup is None:
            unit = unit_backup
        else:
            return None

    # NOTE some plm files have two CHECKSUM rows, so .iloc[:-2] fails
    return pd \
        .read_csv(
            p,
            engine='c',
            header=8,
            usecols=m_cols,
            parse_dates=[['Date', 'Time']]) \
        .iloc[:-2] \
        .dropna(subset=['Date_Time']) \
        .rename(columns=m_cols) \
        .assign(
            unit=unit,
            datetime=lambda x: pd.to_datetime(x['Date_Time'], format='%m/%d/%y %H:%M:%S'),
            cycletime=lambda x: x['cycletime'].apply(utl.to_seconds),
            carryback=lambda x: x['carryback'].astype(str).str.replace(' ', '').astype(float))[good_cols]


def collect_plm_files(unit: str, d_lower: dt = None, lst: list = None):
    """Collect PLM files from p drive and save to desktop
    - Used for uploading to KA PLM report system

    - TODO this could be replaced by utl.FileProcessor now?
    """
    start = time.time()

    p = efl.UnitFolder(unit=unit).p_dls

    if d_lower is None:
        d_lower = dt.now() + delta(days=-180)

    if lst is None:
        lst = utl.FolderSearch('plm', d_lower=d_lower).search(p)

    log.info(f'{f.deltasec(start)} | Found {len(lst)} files.')

    p_dst = cf.desktop / f'plm/{unit}'

    for p in lst:
        fl.copy_file(p_src=p, p_dst=p_dst / f'{fl.date_created(p):%Y-%m-%d}_{p.name}')

    log.info(f'{f.deltasec(start)} | {len(lst)} files copied to desktop.')
    return lst
