from pathlib import Path
from typing import *

import pandas as pd

from smseventlog import delta, dt
from smseventlog import functions as f
from smseventlog import getlog
from smseventlog.data.internal import utils as utl
from smseventlog.database import db

log = getlog(__name__)


def parse_fault_time(tstr):
    arr = tstr.split('|')
    t, tz = int(arr[0]), int(arr[1])
    return dt.fromtimestamp(t) + delta(seconds=tz)


def unit_from_fault(p: Path, raise_errors: bool = True) -> Union[str, None]:
    """Get unit from faults.csv

    Parameters
    ----------
    p : Path

    Returns
    -------
    Union[str, None]
        unit if fault file has serial
    """

    s_head = pd \
        .read_csv(p, usecols=(0, 1), skiprows=1, nrows=4, header=None) \
        .set_index(0) \
        .rename_axis('index').T \
        .pipe(f.lower_cols) \
        .apply(lambda x: x.str.strip()) \
        .assign(model=lambda x: x.machine_model + x.machine_type_minor_variation_code) \
        .rename(columns=dict(machine_serial_no='serial')) \
        .T[1]

    unit = db.unit_from_serial(
        serial=s_head['serial'],
        model=s_head['model'])

    if unit is None and raise_errors:
        raise Exception('Couldn\'t get unit from fault header.')

    return unit


def read_fault(p: Path, **kw) -> Union[pd.DataFrame, None]:
    """Return dataframe from fault.csv path
    - NOTE need to handle minesites other than forthills

    Parameters
    ----------
    p : Path
    """
    newcols = ['unit', 'code', 'time_from', 'time_to', 'faultcount', 'message']

    try:
        unit = unit_from_fault(p=p)

        df = pd \
            .read_csv(p, header=None, skiprows=28, usecols=(0, 1, 3, 5, 7, 8))

        df.columns = newcols
        return df \
            .assign(
                unit=unit,
                code=lambda x: x.code.str.replace('#', ''),
                time_from=lambda x: x.time_from.apply(parse_fault_time),
                time_to=lambda x: x.time_to.apply(parse_fault_time))

    except:
        log.warning(f'Failed faults import: {p}')
        return None


def combine_fault_header(m_list: dict) -> pd.DataFrame:
    dfs = []

    for unit, lst in m_list.items():
        for p in lst:
            try:
                df = read_fault_header(p)
                dfs.append(df)
                break
            except:
                print(f'failed import: {p}')

    return pd.concat(dfs)


def read_fault_header(p):
    df = pd.read_csv(p, nrows=12, names=[i for i in range(5)])
    unit = utl.unit_from_path(p)

    m = dict(
        serial_no=df.loc[4, 1],
        eng_model=df.loc[5, 1],
        eng_sn_1=df.loc[5, 2],
        eng_sn_2=df.loc[5, 4],
        prog_ver_1=df.loc[11, 1],
        prog_ver_2=df.loc[11, 2],
    )

    return pd.DataFrame \
        .from_dict(m, orient='index', columns=[unit]).T \
        .rename_axis('unit')
