import re
import time
from pathlib import Path
from tarfile import TarFile
from timeit import default_timer as timer
from typing import *
from zipfile import ZipFile

import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm

from guesttracker import delta, dt
from guesttracker import errors as er
from guesttracker import eventfolders as efl
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.data.internal import faults as flt
from guesttracker.data.internal import plm
from guesttracker.data.internal import utils as utl
from guesttracker.database import db
from guesttracker.utils import fileops as fl

log = getlog(__name__)

ahs_files = ['data', 'dnevent', 'sfevent']


def import_dls(p: Path, mw=None) -> dict:
    """Upload downloads folder from local computer to p-drive

    p : Path
        filepath to process
    mw : gui.gui.MainWindow
        mw object to update statusbar with progress

    Returns
    -------
    dict
        dict of result times

    Import csvs to database:
        faults
        plm

    Zip:
        dsc folder (ge files)

    Attempt to get unit from:
        - file name
        - dsc stats file
        - fault csv
        - plm csv

    - TODO check selected dir contains some correct files (eg not accidental selection)
    """
    start = time.time()
    now = lambda x: time.time() - x

    # check if unit given in file name
    unit = utl.unit_from_str(s=p.name)
    d = f.date_from_str(s=p.name)
    d_lower = dt.now() + delta(days=-365 * 2)
    m_result = {k: dict(num=0, time=0) for k in ('ge_zip', 'fault', 'plm')}

    # list of dates created as backup if no dsc
    lst_dates = [fl.date_created(p) for p in p.iterdir()]

    # callback to update statusbar
    if mw is None:
        from guesttracker.gui._global import update_statusbar as us
    else:
        us = mw.update_statusbar

    # find dsc files to use for stat file first
    lst_dsc = utl.FolderSearch('dsc', d_lower=d_lower).search(p)
    if lst_dsc:
        lst_dates = []  # use dsc for date, clear backup dates

        # try to get unit from first dsc serial file first
        try:
            p_stat = stats_from_dsc(p=lst_dsc[0])
            if unit is None:
                print('p_stat', p_stat)
                unit = unit_from_stat(p_stat)
        except Exception as e:
            # print(e)
            log.warning('Failed to get unit from stats file.')

    # save files to import after unit check
    m_import = {}
    unit_func = dict(
        fault=flt.unit_from_fault,
        plm=plm.unit_from_haulcycle)

    # check unit from fault/plm
    for ftype in unit_func.keys():
        try:
            lst_csv = utl.FolderSearch(ftype, d_lower=d_lower).search(p)

            if lst_csv:
                m_import[ftype] = lst_csv

                # try to get unit if doesn't exist yet
                if unit is None:
                    unit = unit_func[ftype](p=lst_csv[0], raise_errors=False)

        except Exception as e:
            # print(e)
            us(msg=f'Failed to read {ftype} file(s).', warn=True, log_=True)

    # get dates from ge dsc
    for p_dsc in lst_dsc:
        lst_dates.append(date_from_dsc(p_dsc))

    # check for AHS files in first level of dls folder
    ahs_folders = utl.FolderSearch('ahs', max_depth=0).search(p)
    if not ahs_folders:
        suffix = 'DLS'
    else:
        suffix = 'FRDLS'

        if unit is None:
            unit = val_from_ahs_files(ahs_folders, 'unit')

        # get date from ahs files
        if d is None:
            lst_dates.append(val_from_ahs_files(ahs_folders, 'date'))

    # final check, fail if unit doesn't exist yet
    if unit is None:
        raise er.NoUnitError()

    # sort dates and set date if not given in folder name
    if d is None and lst_dates:
        lst_dates = sorted(lst_dates, reverse=False)
        d = lst_dates[0]

    if d is None:
        raise er.NoDateError()

    name = f'{unit} - {d:%Y-%m-%d}'
    title = f'{name} - {suffix}'
    m_result['name'] = name

    from guesttracker.eventfolders import UnitFolder
    uf = UnitFolder(unit=unit)
    p_dst = uf.p_dls / f'{d.year}/{title}'

    # make sure we don't overwrite folder
    log.info(f'p_dst: {p_dst}')
    if p_dst.exists():
        raise er.FolderExistsError(p=p_dst)

    # import fault/plm
    for ftype, lst_csv in m_import.items():
        time_prev = time.time()
        # log.info(f'importing: {ftype}')

        try:
            rowsadded = utl.combine_import_csvs(lst_csv=lst_csv, ftype=ftype, unit=unit, n_jobs=-4)
            m_result[ftype] = dict(num=rowsadded or 0, time=now(time_prev))
        except Exception as e:
            # NOTE could maybe raise a custom exception here?
            us(msg=f'Failed to import {ftype} files.', warn=True, log_=True)

    # zip GE dsc files
    if lst_dsc:
        time_prev = time.time()
        for p_dsc in lst_dsc:
            # log.info(f'zipping: {p_dsc}')
            fl.zip_folder_threadsafe(p_src=p_dsc, p_dst=p_dst / p_dsc.name, delete=True)

        m_result['ge_zip'] = dict(num=len(lst_dsc), time=now(time_prev))

    # zip dnevent/sfevent folders in place
    if ahs_folders:
        time_prev = time.time()

        # copy 6 newest files > 3mb to PREVIEW dir
        make_ahs_data_preview(ahs_folders)

        for p_ahs in ahs_folders:
            # if any(item in p_ahs.name.lower() for item in ('dnevent', 'sfevent')):
            fl.zip_folder_threadsafe(p_src=p_ahs, p_dst=p_dst / p_ahs.name, delete=True)

        m_result['ahs_zip'] = dict(num=len(ahs_folders), time=now(time_prev))

    # upload all to p-drive
    us(f'Uploading files to: {p_dst}')
    fl.move_folder(p_src=p, p_dst=p_dst)

    m_result['time_total'] = now(start)

    return m_result


def make_ahs_data_preview(
        ahs_folders: List[Path],
        p_dst: Path = None,
        n_newest: int = 6) -> None:
    """Extract x newest data files > 3mb, copy to separate DATA_PREVIEW dir"""
    p_data = [p for p in ahs_folders if p.name.lower() == 'data']
    if not p_data:
        return

    p_data = p_data[0]
    min_size = 3e6  # 3mb
    lst = []

    if p_dst is None:
        p_dst = p_data.parent

    p_dst = p_dst / 'DATA_PREVIEW'

    # loop newest files, collect those > 3mb
    for p in sorted(p_data.glob('*.gz*'), reverse=True):
        if p.stat().st_size > min_size:
            lst.append(p)

        if len(lst) >= n_newest:
            break

    # move files to DATA_PREVIEW dir
    for p in lst:
        fl.copy_file(p_src=p, p_dst=p_dst / p.name)


def val_from_ahs_files(ahs_folders: List[Path], type_: str) -> Union[str, None]:
    """Get unit number/date from list of ahs FR folders

    Parameters
    ----------
    ahs_folders : List[Path]
        [data, dnevent, sfevent]
    type_ : str
        unit | date

    Returns
    -------
    Union[str, None]
        unit/date or None
    """
    val = None
    expr = r'gz$|txt$'

    if not type_ in ('unit', 'date'):
        raise ValueError(f'type_ must be unit|date, not "{type_}"')

    for p_ahs in ahs_folders:
        if val is None:
            for p2 in sorted(list(p_ahs.iterdir()), reverse=True):
                if re.search(expr, p2.name.lower()):

                    if type_ == 'unit':
                        temp = p2.name.split('_')[0]

                        if db.unit_exists(temp):
                            val = temp

                    elif type_ == 'date':
                        # get date as 6 digit date YYMMDD
                        val = re.search(r'\d{6}', p2.name)[0]
                        val = dt.strptime(val, '%y%m%d')

                    break
    return val


def is_year(name: str) -> bool:
    """Check if passed in string is a 4 digit year, eg '2020'

    Parameters
    ----------
    name : str
        String to check

    Returns
    -------
    bool
    """
    exp = re.compile('^[2][0-9]{3}$')
    ans = re.search(exp, name)
    return not ans is None


@er.errlog(msg='Couldn\'t find recent dls folder.', err=False)
def get_recent_dls_unit(unit: str) -> Path:
    """Get most recent dls folder for single unit

    Parameters
    ----------
    unit : str

    Returns
    -------
    Path
        Path to most recent dls folder
    """

    p_unit = efl.UnitFolder(unit=unit).p_unit
    p_dls = p_unit / 'Downloads'

    if not p_dls.exists():
        log.warning(f'Download folder doesn\'t exist: {p_dls}')
        return

    # get all downloads/year folders
    lst_year = [p for p in p_dls.iterdir() if p.is_dir() and is_year(p.name)]

    if not lst_year:
        log.warning('No download year folders found.')
        return

    # sort year folders by name, newest first, select first
    lst_year_sorted = sorted(lst_year, key=lambda p: p.name, reverse=True)  # sort by year
    p_year = lst_year_sorted[0]

    # sort all dls folders on date from folder title
    lst_dls = [p for p in p_year.iterdir() if p.is_dir()]
    lst_dls_sorted = sorted(filter(lambda p: f.date_from_str(p.name) is not None, lst_dls),
                            key=lambda p: f.date_from_str(p.name), reverse=True)

    return lst_dls_sorted[0]


def zip_recent_dls_unit(unit: str, _zip=True) -> Path:
    """Func for gui to find (optional zip) most recent dls folder by parsing date in folder title"""
    from guesttracker.gui import _global as gbl
    from guesttracker.gui.dialogs.base import msg_simple, msgbox

    p_dls = get_recent_dls_unit(unit=unit)

    if not p_dls is None:
        msg = f'Found DLS folder: {p_dls.name}, calculating size...'
        gbl.update_statusbar(msg)
        gbl.get_mainwindow().app.processEvents()

        size = fl.calc_size(p_dls)
        msg = f'Found DLS folder:\n\n{p_dls.name}\n{size}\n\nZip now?'

        if not msgbox(msg=msg, yesno=True):
            return

    else:
        msg = 'Couldn\'t find recent DLS folder, check folder structure for issues.'
        msg_simple(msg=msg, icon='warning')
        return

    if _zip:
        p_zip = fl.zip_folder_threadsafe(p_src=p_dls, delete=False)
        return p_zip
    else:
        return p_dls


def fix_dsc(p: Path) -> None:
    """Process/fix single dsc/dls folder"""
    # log.info(f'fix_dsc: {p}')
    start = timer()
    unit = utl.unit_from_path(p)
    uf = efl.UnitFolder(unit=unit)

    p_parent = p.parent
    d = date_from_dsc(p=p)

    # rename dls folder: UUU - YYYY-MM-DD - DLS
    newname = f'{unit} - {d:%Y-%m-%d} - DLS'

    p_new = uf.p_dls / f'{d.year}/{newname}'

    # need to make sure there is only one _dsc_ folder in path
    # make sure dsc isn't within 2 levels of 'Downloads' fodler
    dsccount = sum(1 for _ in p_parent.glob('*dsc*'))

    if dsccount > 1 or check_parents(p=p, depth=2, names=['downloads']):
        # just move dsc folder, not parent and contents
        p_src = p
        p_dst = p_new / p.name
    else:
        p_src = p_parent  # folder above _dsc_
        p_dst = p_new

    # zip and move dsc folder, then move anything else remaining in the parent dir
    is_zip = p.suffix in ('.zip', '.tar')
    is_same_folder = p_src == p_dst

    if not is_same_folder or not is_zip:
        msg = ''
        for n, _p in dict(orig=p, src=p_src, dst=p_dst).items():
            msg += f'\n\t\t{n:<4}: {_p}'

        log.info(f'fix_dsc:{msg}')

    try:
        if not is_zip:
            p_zip = fl.zip_folder_threadsafe(
                p_src=p,
                p_dst=p_new / p.name,
                delete=True)

        if not is_same_folder:
            fl.move_folder(p_src=p_src, p_dst=p_dst)

    except Exception as e:
        log.warning(f'Error fixing dsc folder: {str(p_src)}')
        raise e

    log.info(f'Elapsed time: {f.deltasec(start, timer())}s')


def fix_dls_all_units(d_lower: dt = None) -> None:
    if d_lower is None:
        d_lower = dt.now() + delta(days=-30)

    units = utl.all_units()

    # collect dsc files from all units in parallel

    result = Parallel(n_jobs=-1, verbose=11)(delayed(utl.process_files)(
        ftype='dsc',
        units=unit,
        d_lower=d_lower,
        parallel=False) for unit in units)

    # fix them


def date_from_dsc(p: Path) -> dt:
    """Parse date from dsc folder name, eg 328_dsc_20180526-072028
    - if no dsc, use date created"""
    try:
        sdate = p.name.split('_dsc_')[-1].split('-')[0]
        d = dt.strptime(sdate, '%Y%m%d')
    except Exception:
        d = fl.date_created(p)

    return d


def get_recent_dsc_single(
        unit: str,
        d_lower: dt = dt(2020, 1, 1),
        year: str = None,
        all_files: bool = False,
        ftype: str = 'dsc',
        max_depth: int = 3):
    """Return list of most recent dsc folder from each unit
    - OR most recent fault... could extend this for any filetype

    Parameters
    ----------
    d_lower : datetime, optional,
        limit search by date, default dt(2020,1,1)
    unit : str, optional
    all_files: bool
        return dict of unit: list of all sorted files

    Returns
    -------
    list | dict
    """
    lst = []
    uf = efl.UnitFolder(unit=unit)

    p_dls = uf.p_dls

    if not year is None:
        p_year = p_dls / year
        if p_year.exists():
            p_dls = p_year

    lst_unit = utl.FolderSearch(ftype, d_lower=d_lower, max_depth=max_depth).search(p_dls)

    if lst_unit:
        lst_unit.sort(key=lambda p: date_from_dsc(p), reverse=True)

        if not all_files:
            lst.append(lst_unit[0])
        else:
            lst.extend(lst_unit)

    return lst


def get_recent_dsc_all(minesite='FortHills', model='980E', all_files=True, **kw):
    """Return list of most recent dsc folders for all units"""
    lst = []

    # keep all files to try and import next most recent if file fails
    if all_files:
        lst = {}

    units = db.unique_units(minesite=minesite, model=model)

    for unit in tqdm(units):
        recent_dsc = get_recent_dsc_single(unit=unit, all_files=all_files, **kw)

        if not recent_dsc:
            print(f'\n\nNo recent dsc for: {unit}')

        if not all_files:
            lst.extend(recent_dsc)
        else:
            lst[unit] = recent_dsc

    return lst


def move_tr3(p):
    unit = utl.unit_from_path(p)  # assuming in unit folder

    p_dst_base = Path('/Users/Jayme/OneDrive/SMS Equipment/Share/tr3 export')
    p_dst = p_dst_base / f'{unit}/{p.name}'

    fl.copy_file(p_src=p, p_dst=p_dst)


def check_parents(p: Path, depth: int, names: list) -> bool:
    """Check path to make sure parents aren't top level folders

    Parameters
    ----------
    p : Path
        Path to check\n
    depth : int
        From start of folder path to this folder level\n
    names : list
        Names to check

    Returns
    -------
    bool
        If path checked is top level folder
    """
    names = [n.lower() for n in names]

    for parent in list(p.parents)[:depth]:
        if parent.name.lower() in names:
            return True

    return False


def zip_recent_dls(units, d_lower=dt(2020, 1, 1)):
    # get most recent dsc from list of units and zip parent folder for attaching to TSI
    if not isinstance(units, list):
        units = [units]
    lst = []
    for unit in units:
        lst.extend(get_recent_dsc_single(unit=unit, d_lower=d_lower))

    lst_zip = [fl.zip_folder_threadsafe(p_src=p.parent, delete=False) for p in lst]

    return lst_zip

# STATS csv


def stats_from_dsc(p):
    """Get stats file path from dsc path"""
    if p.is_dir():
        try:
            return list((p / 'stats').glob('SERIAL*csv'))[0]
        except Exception:
            return None
            print(f'Couldn\'t read stats: {p}')
    elif p.suffix == '.zip':
        return ZipFile(p)
    elif p.suffix == '.tar':
        return TarFile(p)


def import_stats(lst=None, d_lower=dt(2021, 1, 1)):
    """Use list of most recent dsc and combine into dataframe"""

    if lst is None:
        lst = get_recent_dsc_all(d_lower=d_lower)

    if isinstance(lst, dict):
        dfs = []
        for unit, lst_csv in tqdm(lst.items()):

            # try to find/load csv, or move to next if fail
            for p in lst_csv:
                try:
                    p_csv = stats_from_dsc(p)
                    df_single = get_stats(p=p_csv)
                    dfs.append(df_single)
                    break
                except Exception as e:
                    log.warning(f'Failed to load csv: {p}, \n{str(e)}')

        df = pd.concat(dfs)

    else:
        df = pd.concat([get_stats(stats_from_dsc(p)) for p in lst])

    return df


def get_list_stats(unit):
    """Return list of STATS csvs for specific unit"""
    from guesttracker.eventfolders import UnitFolder
    uf = UnitFolder(unit=unit)

    p_dls = uf.p_dls

    return p_dls.glob('SERIAL*csv')


def smr_from_stats(lst):

    return pd.concat([get_stats(p) for p in lst])


def unit_from_stat(p: Path) -> Union[str, None]:
    """Try to get unit from stats file

    Parameters
    ----------
    p : Path

    Returns
    -------
    Union[str, None]
        unit if exists else None
    """
    df = get_stats(p=p)
    unit = df.index[0]
    if not unit == 'TEMP':
        return unit


def get_stats(p, all_cols=False):
    """
    Read stats csv and convert to single row df of timestamp, psc/tsc versions + inv SNs, to be combined
    Can read zip or tarfiles"""

    # dsc folder could be zipped, just read zipped csv, easy!
    # super not dry
    # print(p)
    if isinstance(p, ZipFile):
        zf = p
        p = Path(zf.filename)
        csv = [str(file.filename) for file in zf.filelist if re.search(
            r'serial.*csv', str(file), flags=re.IGNORECASE)][0]
        with zf.open(csv) as reader:
            df = pd.read_csv(reader, index_col=0)

    elif isinstance(p, TarFile):
        tf = p
        p = Path(tf.name)
        csv = [file for file in tf.getnames() if re.search(r'serial.*csv', file, flags=re.IGNORECASE)][0]
        df = pd.read_csv(tf.extractfile(csv), index_col=0)

    else:
        df = pd.read_csv(p, index_col=0)

    df = df \
        .applymap(lambda x: str(x).strip())

    # need to keep original order after pivot
    orig_cols = df[df.columns[0]].unique().tolist()

    df = df \
        .assign(unit='TEMP') \
        .pipe(lambda df: df.drop_duplicates(subset=df.columns[0], keep='first')) \
        .pipe(lambda df: df.pivot(
            index='unit',
            columns=df.columns[0],
            values=df.columns[1]))[orig_cols] \
        .pipe(lambda df: df[[col for col in df.columns if not '-' in col]]) \
        .pipe(f.lower_cols) \
        .assign(todays_datetime=lambda x: pd.to_datetime(x.todays_datetime).dt.date) \
        .rename_axis('', axis=1)

    # shorten column names
    m_rename = dict(
        serial_number='sn',
        number='no',
        code_version='ver',
        version='ver',
        hours='hrs',
        inverter='inv')

    # need to loop instead of dict comp so can update multiple times
    rename_cols = {}
    for col in df.columns:
        orig_col = col
        for orig, new in m_rename.items():
            if orig in col:
                col = col.replace(orig, new)

        rename_cols[orig_col] = col

    rename_cols.update({
        'todays_datetime': 'date',
        'total_hours': 'engine_hrs',
        'model++': 'wm_model'})

    drop_cols = [
        'unit',
        'truck_identification',
        'end',
        'model',
        'model+',
        'model+++',
        'mine_dos_filename',
        'oe_mdos_filename',
        'ge_dos_filename']

    df = df \
        .rename(columns=rename_cols) \
        .drop(columns=drop_cols) \
        .rename(columns=dict(truck_model='model'))

    serial = df.iloc[0, df.columns.get_loc('truck_sn')]
    model = df.iloc[0, df.columns.get_loc('model')]
    unit = db.unit_from_serial(serial=serial, model=model)

    # try from path as backup
    if unit is None:
        unit = utl.unit_from_path(p)

    if not unit is None:
        df.index = [unit]

    return df
