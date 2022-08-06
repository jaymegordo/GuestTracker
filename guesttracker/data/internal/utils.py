import itertools
import multiprocessing
import re
import sys
import time
from pathlib import Path
from typing import *

import pandas as pd
from joblib import Parallel, delayed

from smseventlog import config as cf
from smseventlog import dbtransaction as dbt
from smseventlog import delta, dt
from smseventlog import eventfolders as efl
from smseventlog import functions as f
from smseventlog import getlog
from smseventlog.data.internal import dls, faults, plm
from smseventlog.database import db
from smseventlog.queries import TableKeys
from smseventlog.utils import fileops as fl

log = getlog(__name__)

"""Helper funcs to import external data, used by all other modules in /data/internal"""


def get_config(ftype: str):
    return dict(
        fault=dict(
            duplicate_cols=['unit', 'code', 'time_from'],
            imptable='FaultImport',
            impfunc='ImportFaults',
            read_func=faults.read_fault,
            filter_col='unit',
            table_name='Faults'),
        plm=dict(
            duplicate_cols=['unit', 'datetime'],
            imptable='PLMImport',
            impfunc='ImportPLM',
            read_func=plm.read_plm_wrapped,
            filter_col='unit',
            table_name='PLM')) \
        .get(ftype)


class FolderSearch():
    """Class to recursively search folder paths for specific file types"""

    # set up filepaths to exclude
    fr = ['frdls', 'event', 'data']
    base = ['stats', 'system', 'pic'] + fr
    vhms = ['vhms', 'chk']
    plm = ['plm']
    ge = ['ge', 'dsc']
    non_ge = base + ge

    cfg = dict(
        fault=dict(
            find=r'fault0.*csv$',
            exclude=non_ge + plm),
        plm=dict(
            find=r'haul.*csv$',
            exclude=[r'\d{8}'] + non_ge + vhms),
        dsc=dict(
            find=r'dsc|\d{5}_\d{6}_\d{4}',
            exclude=[r'^a\d{5}$'] + base + plm + vhms),
        tr3=dict(
            find=r'.*tr3$',
            exclude=non_ge + plm + vhms),
        ahs=dict(
            find=r'^data\d*$|^dnevent|^sfevent',
        ))

    keys = list(cfg.keys())

    def __init__(
            self,
            ftype: str,
            max_depth: int = 6,
            d_lower: dt = None):
        """
        Parameters
        ----------
        ftype : str
            file type to collect (dsc, fault | plm | tr3)
        max_depth : int, optional
            max depth to recurse, default 5
        d_lower : dt, optional
            date to filter file date created, default 2016-01-01
        """
        if not ftype in self.keys:
            raise ValueError(f'Incorrect ftype "{ftype}", must be in {self.keys}')

        if d_lower is None:
            d_lower = dt.now() + delta(days=-180)

        cfg = self.cfg.get(ftype)
        expr_exclude = self.make_re_exclude(lst=cfg.get('exclude'))
        expr_find = cfg.get('find')

        f.set_self(vars())

    def should_exclude(self, name: str) -> bool:
        """Check if folder name matches exclude pattern

        Parameters
        ----------
        name : str
            folder name to check

        Returns
        -------
        bool
            if folder matches exclude pattern
        """
        return False if self.expr_exclude is None else re.search(self.expr_exclude, name)

    def search(self, p: Path, depth: int = 0) -> list:
        """Recurse folder and find matching files/folders

        Parameters
        ----------
        p : Path
            start path
        depth : int, optional
            current search depth, default 0

        Returns
        -------
        list
            list of all collected files
        """
        lst = []

        for p in p.iterdir():
            name = p.name.lower()

            # exclude folders, enforce date greater than d_lower
            if not self.should_exclude(name) and fl.date_modified(p) > self.d_lower:

                # find matching files and add to return list
                if re.search(self.expr_find, name):
                    lst.append(p)

                # else search deeper
                elif depth < self.max_depth and p.is_dir():
                    lst.extend(self.search(p=p, depth=depth + 1))

        return lst

    def make_re_exclude(self, lst: list) -> str:
        """Make exclude regex expr for ftype

        Parameters
        ----------
        lst : list
            list to join

        Returns
        -------
        str
            exclude string item1|item2|...
        """
        return '|'.join(lst) if not lst is None else None


class FileProcessor():
    """Higher level class to manage collecting and processing files based on type"""

    def __init__(
            self,
            ftype: str,
            d_lower: dt = dt(2020, 1, 1),
            max_depth: int = 4,
            search_folders: list = ['downloads']):

        self.collected_files = []
        self.collected_files_dict = {}
        self.folder_search = FolderSearch(ftype=ftype, d_lower=d_lower, max_depth=max_depth)

        f.set_self(vars())

    def _collect_files_unit(self, unit: str) -> Dict[str, list]:

        lst = []
        p_unit = efl.UnitFolder(unit=unit).p_unit

        # start at downloads
        lst_search = [x for x in p_unit.iterdir() if x.is_dir() and x.name.lower()
                      in self.search_folders]

        # could search more than just downloads folder (eg event too)
        for p_search in lst_search:
            lst.extend(self.folder_search.search(p_search))

        return {unit: lst}

    def collect_files(self, units: list = None) -> list:
        """Collect files for all units

        Parameters
        ----------
        units : list, optional
            default all_units()

        Returns
        -------
        list
            flattened list of all files
        """
        units = f.as_list(units or all_units())

        # only use n_jobs as max of cpu count or n_units
        n_jobs = min(multiprocessing.cpu_count(), len(units))

        # parallel process collecting files per unit
        lst = Parallel(n_jobs=n_jobs, verbose=11)(delayed(self._collect_files_unit)(unit=unit) for unit in units)

        self.collected_files_dict = f.flatten_list_dict(lst)
        self.collected_files = f.flatten_list_list([v for v in self.collected_files_dict.values()])

        # log message for total number of files, and files per unit
        m_msg = {unit: len(items) for unit, items in self.collected_files_dict.items()}
        n = len(self.collected_files)
        log.info(f'Collected [{n}] files:\n{f.pretty_dict(m_msg, prnt=False)}')

        return self.collected_files

    def process(self, units: list = None, lst: list = None) -> list:

        units = f.as_list(units or all_units())
        lst = lst or self.collect_files(units=units)

        name = f'process_{self.ftype}'
        log.info(f'{name} - units: [{len(units)}], startdate: {self.d_lower}')

        proc_func = getattr(self, f'{name}')
        proc_func(lst=lst)

    def proc_dsc_batch(self, lst: List[Path]) -> int:
        """Process batch of dsc files that may be in the same top folder"""
        i = 0
        for p in lst:
            try:
                dls.fix_dsc(p)
                i += 1
            except Exception as e:
                log.error(f'Failed to fix dsc file:\n\t{e}')

        return i

    def process_dsc(self, lst: List[Path]) -> list:

        # group by "downloads/2021/F301 - 2021-01-01 - DLS" to avoid parallel collisions
        lst_grouped = [list(g) for _, g in itertools.groupby(
            lst, lambda p: fl.get_parent(p, 'downloads', offset=2).name)]

        lst_out = Parallel(n_jobs=-1, verbose=11)(delayed(self.proc_dsc_batch)(lst=lst) for lst in lst_grouped)
        log.info(f'Processed [{sum(lst_out)}/{len(lst)}] dsc files')


def combine_csv(lst_csv, ftype, d_lower=None, n_jobs: int = -1, **kw):
    """Combine list of csvs into single and drop duplicates, based on duplicate cols"""
    func = get_config(ftype).get('read_func')

    # multiprocess reading/parsing single csvs
    job = delayed(func)
    dfs = Parallel(n_jobs=n_jobs, verbose=11, prefer='threads')(job(p=p_csv, **kw) for p_csv in lst_csv)

    df = pd.concat([df for df in dfs if not df is None], sort=False) \
        .drop_duplicates(subset=get_config(ftype)['duplicate_cols'])

    # drop old records before importing
    # faults dont use datetime, but could use 'Time_From'
    if not d_lower is None and 'datetime' in df.columns:
        df = df[df.datetime >= d_lower]

    return df


def to_seconds(t):
    x = time.strptime(t, '%H:%M:%S')
    return int(delta(hours=x.tm_hour, minutes=x.tm_min, seconds=x.tm_sec).total_seconds())


def get_unitpaths(minesite='FortHills', model_base='980E') -> List[Path]:
    # TODO change this to work with other sites
    p = cf.p_drive / cf.config['UnitPaths'][minesite][model_base]
    return [x for x in p.iterdir() if x.is_dir() and 'F3' in x.name]


def all_units(rng=None) -> list:
    """Return list of FH ONLY unit names
    - TODO make this all minesites

    Returns
    ---
        list
        - eg ['F301', 'F302'...]
    """
    if rng is None:
        rng = (301, 348)
    return [f'F{n}' for n in range(*rng)]


def unit_from_str(s: str) -> Union[str, None]:
    """Try to get unit number from folder filename/string

    Parameters
    ----------
    s : str
        folder name

    Returns
    -------
    Union[str, None]
        unit if found else None
    """
    items = s.split(' ')
    for item in items:
        if db.unit_exists(unit=item):
            return item


def unit_from_path(p):
    """Regex find first occurance of unit in path
    - Needs minesite
    """
    minesite = plm.minesite_from_path(p)

    units = db.get_df_unit() \
        .pipe(lambda df: df[df.MineSite == minesite]) \
        .Unit.unique().tolist()

    match = re.search(f'({"|".join(units)})', str(p))
    if match:
        return match.groups()[0]
    else:
        log.warning(f'Couldn\'t find unit in path: {p}')


def process_files(
        ftype: str,
        units: list = None,
        search_folders: list = ['downloads'],
        d_lower: dt = dt(2020, 1, 1),
        max_depth: int = 4,
        import_: bool = True,
        parallel: bool = True) -> Union[int, pd.DataFrame]:
    """
    Top level control function - pass in single unit or list of units
    1. Get list of files (plm, fault, dsc)
    2. Process - import plm/fault or 'fix' dsc eg downloads folder structure

    TODO - make this into a FileProcessor class
    """

    if ftype == 'tr3':
        search_folders.append('vibe tests')  # bit sketch

    # assume ALL units # TODO: make this work for all minesites?
    units = f.as_list(units or all_units())
    search_folders = [item.lower() for item in search_folders]

    lst = []

    fl.drive_exists()
    for unit in units:
        p_unit = efl.UnitFolder(unit=unit).p_unit
        lst_search = [x for x in p_unit.iterdir() if x.is_dir() and x.name.lower()
                      in search_folders]  # start at downloads

        # could search more than just downloads folder (eg event too)
        for p_search in lst_search:
            lst.extend(FolderSearch(ftype, d_lower=d_lower, max_depth=max_depth).search(p_search))

        # process all dsc folders per unit as we find them
        if ftype == 'dsc':
            log.info(f'Processing dsc, unit: {unit} | dsc folders found: {len(lst)}')

            # group by "downloads/2021/F301 - 2021-01-01 - DLS" to avoid parallel collisions
            lst_grouped = [list(g) for _, g in itertools.groupby(
                lst, lambda p: fl.get_parent(p, 'downloads', offset=2).name)]

            def proc_dsc_batch(lst: List[Path]) -> None:
                """Process batch of dsc files that may be in the same top folder"""
                for p in lst:
                    dls.fix_dsc(p)

            Parallel(n_jobs=-1, verbose=11)(delayed(proc_dsc_batch)(lst=lst) for lst in lst_grouped)
            # Parallel(n_jobs=-1, verbose=11)(delayed(dls.fix_dsc)(p=p) for p in lst)
            # return lst
            # if parallel:
            # else:
            #     # when calling "all_units", process individual files per unit in sequence to avoid conflicts

            #     for p in lst:
            #         dls.fix_dsc(p=p)

            lst = []  # need to reset list, only for dsc, this is a bit sketch
        elif ftype == 'tr3':
            for p in lst:
                dls.move_tr3(p=p)

            lst = []

    # collect all csv files for all units first, then import together
    if ftype in ('plm', 'fault'):
        log.info(f'num files: {len(lst)}')
        if lst:
            df = combine_csv(lst_csv=lst, ftype=ftype, d_lower=d_lower)
            return import_csv_df(df=df, ftype=ftype) if import_ else df

        else:
            return pd.DataFrame()  # return blank dataframe


def filter_existing_records(df: pd.DataFrame, ftype: str) -> pd.DataFrame:
    """Filter dataframe to remove existing records before import to db

    Parameters
    ----------
    df : pd.DataFrame
        df to filter
    ftype : str

    Returns
    -------
    pd.DataFrame
        df with rows removed if exist in db
    """
    m = get_config(ftype)
    filter_col = m['filter_col']
    filter_val = df[filter_col].unique().tolist()
    if len(filter_val) == 1:
        filter_val = filter_val[0]

    return TableKeys(
        table_name=m['table_name'],
        filter_vals={filter_col: filter_val}) \
        .filter_existing(df=df)


def combine_import_csvs(lst_csv: list, ftype: str, **kw) -> int:
    """Convenience func to combine and import list of plm/fault csvs

    Parameters
    ----------
    lst_csv : list
        list of plm/fault csvs
    ftype : str
        fault|plm
    """
    df = combine_csv(lst_csv=lst_csv, ftype=ftype, **kw)
    return import_csv_df(df=df, ftype=ftype)


def import_csv_df(df: pd.DataFrame, ftype: str, **kw) -> int:
    """Import fault or plm df combined from csvs"""

    df = filter_existing_records(df=df, ftype=ftype)

    if len(df) == 0:
        log.info(f'0 rows to import. ftype: {ftype}')
        return 0

    m = get_config(ftype)
    table_name = m['table_name']
    keys = dbt.get_dbtable_keys(table_name)

    return db.insert_update(
        a=table_name,
        join_cols=keys,
        df=df,
        prnt=True,
        notification=False,
        **kw)


def write_import_fail(msg):
    if not sys.platform == 'darwin':
        return
    failpath = Path().home() / 'Desktop/importfail.txt'
    with open(failpath, 'a') as f:
        f.write(f'{msg}\n')
