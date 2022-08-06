import inspect
import json
import pickle
import pkgutil
import re
import sys
import time
import webbrowser
from distutils.util import strtobool
from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules
from typing import *

import numpy as np
import pandas as pd

from smseventlog import config as cf
from smseventlog import date, delta, dt, getlog

log = getlog(__name__)


def flatten_list_list(lst: List[list]) -> list:
    """Flatten single level nested list of lists

    Parameters
    ----------
    lst : List[list]

    Returns
    -------
    list
        flattened list
    """
    return [item for sublist in lst for item in sublist]


def flatten_list_dict(lst: List[Dict[str, list]]) -> Dict[str, list]:
    """Flatten list of dicts to dict of lists

    Parameters
    ----------
    lst : List[Dict[str, list]]
        list of dicts of lists

    Returns
    -------
    Dict[str, list]
    """
    return {k: v for m in lst for k, v in m.items()}

# DICT & CONVERSIONS


def inverse(m: dict) -> dict:
    """Return inverse of dict"""
    return {v: k for k, v in m.items()}


def get_dict_view_db(title: str) -> Dict[str, str]:
    """return dict of {view_col: db_col}"""
    return cf.config['Headers'].get(title, {})


def get_dict_db_view(title: str) -> Dict[str, str]:
    """return dict of {db_col: view_col}"""
    return inverse(get_dict_view_db(title))


def convert_df_view_cols(df: pd.DataFrame, m: Dict[str, str]):
    """convert db cols to view cols from dict of conversions. keep original if new col not in dict"""
    df.columns = [m[c] if c in m.keys() else c for c in df.columns]
    return df


def convert_df_db_cols(title: str, df: pd.DataFrame):
    """Convert df with view_cols to db_cols from dict of conversions. keep original if new col not in dict

    Parameters
    ----------
    title : str
        table title
    df : pd.DataFrame

    Returns
    -------
    pd.DatFrame
        dataframe with cols converted to db cols
    """
    m = get_dict_view_db(title)
    df.columns = [m[c] if c in m.keys() else c for c in df.columns]
    return df


def convert_dict_db_view(title: str, m: dict, output: str = 'view'):
    """Convert dict with from either db or view, to output type cols
    - NOTE only converts columns which exist in the given table view eg 'Work Orders' or 'Event Log'"""
    func_name = 'convert_list_db_view' if output == 'view' else 'convert_list_view_db'
    func = getattr(sys.modules[__name__], func_name)
    initial_cols = list(m.keys())
    final_cols = func(title=title, cols=initial_cols)

    return {final: m[initial] for final, initial in zip(final_cols, initial_cols) if final is not None}


def convert_list_db_view(title, cols):
    # convert list of db cols to view cols, remove cols not in the view?
    m = cf.config['Headers'].get(title, None)
    if not m is None:
        m = inverse(m)
        return [m[c] if c in m.keys() else c for c in cols]
    else:
        return cols


def convert_list_view_db(title, cols):
    # convert list of view cols to db cols
    m = cf.config['Headers'][title]
    return [m[c] if c in m.keys() else c for c in cols]


def get_default_headers(title):
    return list(cf.config['Headers'][title].keys())


def convert_header(title, header, inverse_=False):
    m = cf.config['Headers'][title]
    if inverse_:
        m = inverse(m)

    try:
        return m[header]
    except KeyError:
        return header


def copy_model_attrs(model, target):
    from smseventlog import dbtransaction as dbt
    m = dbt.model_dict(model=model, include_none=True)
    copy_dict_attrs(m=m, target=target)


def copy_dict_attrs(m: dict, target: object):
    """Copy dict items to target object (lowercase)"""
    for k, v in m.items():
        setattr(target, k.lower(), v)


def two_col_list(m) -> str:
    """Create two col css list from dict, used in reports

    Parameters
    ----------
    m : dict
        Dict to convert\n
    """
    body = ''
    for name, val in m.items():
        body = f'{body}<li><span>{name}:</span><span>{val}</span></li>'

    return f'<ul class="two_col_list_narrow">{body}</ul>'


def dict_to_string(m: dict) -> dict:
    """Convert dict values to strings

    Parameters
    ----------
    m : dict

    Returns
    -------
    dict
    """
    for k, v in m.items():
        if isinstance(v, dict):
            m[k] = dict_to_string(v)
        else:
            m[k] = str(v)

    return m


def pretty_dict(m: dict, html: bool = False, prnt: bool = False, bold_keys: bool = False) -> str:
    """Print pretty dict converted to newlines
    Paramaters
    ----
    m : dict
    html: bool
        Use <br> instead of html
    prnt : bool
        print, or return formatted string
    bold_keys : bool
        if true, add ** to dict keys to bold for discord msg

    Returns
    -------
    str
        'Key 1: value 1
        'Key 2: value 2"
    """

    def _bold_keys(m):
        """Recursively bold all keys in dict"""
        if isinstance(m, dict):
            return {f'**{k}**': _bold_keys(v) for k, v in m.items()}
        else:
            return m

    if bold_keys:
        m = _bold_keys(m)

    # make sure all dict values are str
    m = dict_to_string(m)

    s = json.dumps(m, indent=4)
    newline_char = '\n' if not html else '<br>'

    # remove these chars from string
    remove = '}{\'"[]'
    for char in remove:
        s = s.replace(char, '')

        # .replace(', ', newline_char) \
    s = s \
        .replace(',\n', newline_char)

    # remove leading and trailing newlines
    s = re.sub(r'^[\n]', '', s)
    s = re.sub(r'\s*[\n]$', '', s)

    # remove blank lines (if something was a list etc)
    # s = re.sub(r'(\n\s+)(\n)', r'\2', s)

    if prnt:
        print(s)
    else:
        return s


def _pretty_dict(m: dict, html=False) -> str:
    """Return dict converted to newlines
    Paramaters
    ----
    m : dict\n
    html: bool
        Use <br> instead of html
    Returns
    -------
    str\n
        'Key 1: value 1\n
        'Key 2: value 2"
    """
    newline_char = '\n' if not html else '<br>'
    return str(m).replace('{', '').replace('}', '').replace(', ', newline_char).replace("'", '')


def first_n(m: dict, n: int):
    """Return first n items of dict"""
    return {k: m[k] for k in list(m.keys())[:n]}


def set_self(m, prnt=False, exclude=()):
    """Convenience func to assign an object's func's local vars to self"""
    if not isinstance(exclude, tuple):
        exclude = (exclude, )
    exclude += ('__class__', 'self')  # always exclude class/self
    obj = m.get('self', None)  # self must always be in vars dict

    if obj is None:
        return

    for k, v in m.items():
        if prnt:
            print(f'\n\t{k}: {v}')

        if not k in exclude:
            setattr(obj, k, v)


def truncate(val, max_len):
    val = str(val)
    s = val[:max_len]
    if len(val) > max_len:
        s = f'{s}...'
    return s


def bump_version(ver, vertype='patch'):
    if not isinstance(ver, str):
        ver = ver.base_version

    m = dict(zip(['major', 'minor', 'patch'], [int(i) for i in ver.split('.')]))
    m[vertype] += 1

    return '.'.join((str(i) for i in m.values()))


def mins_secs(seconds: int) -> str:
    """Convert seconds to mins, secs string
    03:14
    """
    return ':'.join(str(delta(seconds=seconds)).split('.')[0].split(':')[1:])


def deltasec(start, end=None):
    """Return difference from time object formatted as seconds

    Parameters
    ----------
    start : time.time
        start time obj
    end : time.time, optional
        end time, by default None

    Returns
    -------
    str
        time formatted as seconds string
    Examples
    --------
    >>> start = time()
    >>> f.deltasec(start)
    >>> '00:00:13'
    """
    if end is None:
        end = time.time()

    return str(delta(seconds=end - start)).split('.')[0]


def cursor_to_df(cursor):
    data = (tuple(t) for t in cursor.fetchall())
    cols = [column[0] for column in cursor.description]
    return pd.DataFrame(data=data, columns=cols)


def isnum(val: Any) -> bool:
    """Check if string/number can be converted to number"""
    return str(val).replace('.', '', 1).isdigit()


def conv_int_float_str(val):
    expr = r'^\d+$'  # int
    expr2 = r'^\d+\.\d+$'  # float

    if re.match(expr, val):
        return int(val)
    elif re.match(expr2, val):
        return float(val)
    else:
        return str(val)


def greeting():
    val = 'Morning' if dt.now().time().hour < 12 else 'Afternoon'
    return f'Good {val},<br><br>'


def getattr_chained(obj, methods):
    """Return value from chained method calls
    - eg a = 'A Big Thing' > getattr_chained(a, 'str.lower')
    """
    try:
        for method in methods.split('.'):
            obj = getattr(obj, method)()

        return obj
    except Exception:
        return None


def remove_bad_chars(w: str):
    """Remove any bad chars " : < > | . \\ / * ? in string to make safe for filepaths"""
    return re.sub(r'[\\":<>|.\/\*\?{}&]', '', str(w))


def nice_title(title: str) -> str:
    """Remove slashes, capitalize first letter, avoid acronyms"""
    if pd.isnull(title):
        return ''

    if title.strip() == '':
        return title

    stopwords = 'the a on in of an is to for and'.split(' ')
    title = remove_bad_chars(w=title).strip()

    # check if title is all caps (annoying cummins)
    max_upper = 3 if title.isupper() else 4

    def _check(w: str, i: int = 0) -> str:

        # check if word is longer than max_upper and does not contain numbers
        if w.lower() in stopwords:
            # exceptions
            return w.lower() if not i == 0 else w.title()
        elif len(w) > max_upper and not any(c.isdigit() for c in w):
            # if word is longer than max_upper and does not contain numbers
            return w.title()
        else:
            return w if w.isupper() else w.title()

    return ' '.join(_check(w, i) for i, w in enumerate(title.split()))


def str_to_bool(val):
    if isinstance(val, (np.bool_, np.bool)):
        return bool(val)

    return bool(strtobool(val))


def date_from_str(s: str) -> Union[dt, None]:
    """Extract date from string

    Parameters
    ----------
    s : str

    Returns
    -------
    Union[dt, None]
        datetime obj if found else None
    """
    expr = r'\d{4}-\d{2}-\d{2}'
    result = re.search(expr, s)
    if result:
        return dt.strptime(result[0], '%Y-%m-%d')


def recent_weekday(d: str) -> dt:
    """Get recent date from weekday

    Parameters
    ----------
    d : str
        day of week (eg 'fri')

    Returns
    -------
    dt
        date value
    """
    df = pd.DataFrame(
        data=pd.date_range(dt.now() + delta(days=-7), dt.now()),
        columns=['date']) \
        .assign(
            day=lambda x: x.date.dt.strftime('%a'),
            date=lambda x: x.date.dt.date) \
        .set_index('day')

    return df.date.loc[d.title()]


def convert_date(val):
    """Convert string date or datetime,  or date obj, to datetime object"""
    try:
        if isinstance(val, date):
            return dt.combine(val, dt.min.time())
        elif isinstance(val, str):
            try:
                return dt.strptime(val, '%Y-%m-%d')
            except Exception:
                return dt.strptime(val, '%Y-%m-%d %H:%M:%S')
        else:
            return val  # already a date
    except Exception:
        log.warning(f'Couldn\'t convert val to date: {val}')
        return val


def _input(msg: str) -> bool:
    """Get yes/no answer from user in terminal

    Parameters
    ----------
    msg : str
        prompt for user

    Returns
    -------
    bool
        true if y else False
    """
    reply = str(input(msg + ' (y/n): ')).lower().strip()
    if len(reply) <= 0:
        return False
    if reply[0] == 'y':
        return True
    elif reply[0] == 'n':
        return False
    else:
        return False


def save_pickle(obj, p):
    """Save object to pickle file"""
    with open(p, 'wb') as file:
        pickle.dump(obj, file)

    log.info(f'Saved pickle: {p}')


def as_list(items: Iterable):
    """Convert single item to list"""
    if not isinstance(items, list):
        items = [items]

    return items


def iter_namespace(name):
    """Get module names from top level package name
    - Used when app is frozen with PyInstaller
    - Make sure to add module to pyi's collect_submodules so they're available from it's importer"""
    prefix = name + '.'
    toc = set()

    for importer in pkgutil.iter_importers(name.partition('.')[0]):
        if hasattr(importer, 'toc'):
            toc |= importer.toc

    for name in toc:
        if name.startswith(prefix):
            yield name


def import_submodule_classes(name: str, filename: str, gbls: dict, parent_class: str):
    """Import all Query objects into top level namespace for easier access
    - eg query = qr.EventLog() instead of qr.el.EventLog()
    - NOTE issubclass doesn't work when class is defined in __init__"""

    if not '__init__' in name:
        package_dir = Path(filename).resolve().parent.as_posix()
        # log.info(package_dir)

        if not cf.SYS_FROZEN:
            module_names = [f'{name}.{module_name}' for (_, module_name, _) in iter_modules([package_dir])]
        else:
            module_names = [n for n in iter_namespace(name) if not 'init' in n]

        for module_name in module_names:
            # import the module and iterate through its attributes
            module = import_module(module_name)

            for cls_name, cls in inspect.getmembers(module, inspect.isclass):

                if parent_class in str(cls):
                    gbls[cls_name] = cls


def fix_suncor_unit(unit: str) -> str:
    """Remove suncor's leading zeros from unit"""
    if unit is None:
        return

    # make sure nexch char is also numeric, not '-'
    dbl = r'(?=\d)'
    m = {'^F0': 'F', '^03': '3', '^2': '02'}
    m = {f'{k}{dbl}': v for k, v in m.items()}

    for expr, repl in m.items():
        if re.match(expr, unit):
            unit = re.sub(expr, repl, unit)
            return unit

    return unit
# PANDAS


def left_merge(df: pd.DataFrame, df_right: pd.DataFrame) -> pd.DataFrame:
    """Convenience func to left merge df on index

    Parameters
    ----------
    df : pd.DataFrame
    df_right : pd.DataFrame

    Returns
    -------
    pd.DataFrame
        df with df_right merged
    """
    return df \
        .merge(
            right=df_right,
            how='left',
            left_index=True,
            right_index=True)


def multiIndex_pivot(df, index=None, columns=None, values=None):
    # https://github.com/pandas-dev/pandas/issues/23955
    output_df = df.copy(deep=True)

    if index is None:
        names = list(output_df.index.names)
        output_df = output_df.reset_index()
    else:
        names = index

    output_df = output_df.assign(tuples_index=[tuple(i) for i in output_df[names].values])

    if isinstance(columns, list):
        output_df = output_df.assign(tuples_columns=[tuple(i) for i in output_df[columns].values])  # hashable
        output_df = output_df.pivot(index='tuples_index', columns='tuples_columns', values=values)
        output_df.columns = pd.MultiIndex.from_tuples(output_df.columns, names=columns)  # reduced
    else:
        output_df = output_df.pivot(index='tuples_index', columns=columns, values=values)

    output_df.index = pd.MultiIndex.from_tuples(output_df.index, names=names)

    return output_df


def flatten_multiindex(df):
    """Flatten multi index columns and join with '_' unless second level is blank '' """
    df.columns = df.columns.map(lambda x: '_'.join(x) if not x[1] == '' else x[0])
    return df


def sort_df_by_list(df, lst, lst_col, sort_cols=[]):
    # sort specific column by list, with option to include other columns first
    sorterIndex = dict(zip(lst, range(len(lst))))
    df['sort'] = df[lst_col].map(sorterIndex)

    if not isinstance(sort_cols, list):
        sort_cols = [sort_cols]
    sort_cols.insert(0, 'sort')

    df.sort_values(sort_cols, inplace=True)
    df.drop(columns=['sort'], inplace=True)
    return df


def dtypes_dict(dtype, cols):
    return {col: dtype for col in cols}


def set_default_dtypes(df, m):
    """Set column dtype based on dict of defaults"""
    for col, dtype in m.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    return df


def read_excel(p, **kw):
    return pd.read_excel(p, **kw) \
        .pipe(default_data)


def read_csv(p: Path, **kw) -> pd.DataFrame:
    return pd.read_csv(p, skip_blank_lines=False, **kw) \
        .pipe(default_data)


def read_csv_firstrow(p: Path, **kw) -> pd.DataFrame:
    """Read csv at first row with headers
    - NOTE this is only used to read fc import csv currently, may need to test for others

    Parameters
    ----------
    p : Path

    Returns
    -------
    pd.DataFrame
    """
    import csv

    # get header row
    with open(p, 'r') as fin:
        # row is a list of strings - FILE header will have blank '' cols
        reader = csv.reader(fin)
        header = next(idx for idx, row in enumerate(reader) if len(row) > 1 and row[-1] != '')

    return read_csv(p=p, header=header, **kw)


def default_data(df):
    """Default func to read csv and apply transforms"""
    from jgutils import pandas_utils as pu
    return df \
        .pipe(lower_cols) \
        .pipe(pu.parse_datecols) \
        .pipe(convert_int64)


def default_df(df):
    """Simple df date/int conversions to apply to any df"""
    from jgutils import pandas_utils as pu
    return df \
        .pipe(pu.parse_datecols) \
        .pipe(convert_int64)


def terminal_df(df, date_only=True, show_na=False, **kw):
    """Display df in terminal with better formatting"""
    from tabulate import tabulate

    if date_only:
        m = {col: lambda x: x[col].dt.date for col in df.select_dtypes('datetime').columns}
        df = df.assign(**m)
        # for col in df.select_dtypes('datetime').columns:
        #     df.loc[:, col] = df.loc[:, col].dt.date

    s = tabulate(df, headers=df.columns, **kw)

    if not show_na:
        s = s.replace('nan', '   ')

    print(s)


def convert_dtypes(df, cols, col_type):
    if not isinstance(cols, list):
        cols = [cols]
    for col in cols:
        df[col] = df[col].astype(col_type)
    return df


def convert_int64(df, all_numeric=False):
    """Convert all int64 (numpy) to Int64 (pandas) to better handle null values"""
    for col, dtype in df.dtypes.items():
        if dtype == 'int64' or (all_numeric and dtype == 'float64'):
            df[col] = df[col].astype(pd.Int64Dtype())

    return df


def format_int64(df):
    """Get dict for formatting all ints with commas"""
    m = {col: '{:,.0f}' for col, dtype in df.dtypes.items() if str(dtype).lower() == 'int64'}
    return m


def clean_series(s: pd.Series, convert_str: bool = False) -> List[str]:
    """Convert series to sorted list of unique items

    Parameters
    ----------
    s : pd.Series
    convert_str : bool, optional
        convert to str before clean (eg for dates), by default False

    Returns
    -------
    List[str]
        unique list of items
    """
    if convert_str:
        s = s.astype(str)

    return sorted(list(s.replace('', pd.NA).dropna().unique()))


def append_default_row(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df dtypes aren't changed when appending new row by creating a blank row of defaults, then filling after

    Returns
    -------
        {col: val} for all columns in df
    """
    defaults = {
        'int64': pd.NA,
        'float64': np.NaN,
        'datetime64[ns]': pd.NaT,
        'bool': None,  # False makes 'truth value of array ambiguous issues'
        'object': None}

    dtypes = {k: str(v).lower() for k, v in df.dtypes.items()}  # convert dtypes to lowercase strings
    m = {col: defaults[dtype] if dtype in defaults else np.nan for col, dtype in dtypes.items()}

    return pd.concat([df, pd.DataFrame(data=m, index=[0])]) \
        .astype(df.dtypes) \
        .reset_index(drop=True)


def df_to_strings(df: pd.DataFrame, formats: dict) -> pd.DataFrame:
    """Convert df values to string values for faster display in table.

    Example
    -------
    >>> formats = {
        'StartDate': '{:%Y-%m-%d %H:%M}',
        'Total': '{:,.2f}',
    """
    df = df.copy()

    for col, fmt in formats.items():
        if col in df.columns:
            df[col] = df[col].apply(lambda x: fmt.format(x) if not pd.isnull(x) else '')

    return df.astype('object').fillna('')


def df_to_color(df: pd.DataFrame, highlight_funcs: Dict[str, Callable], role) -> pd.DataFrame:
    """Convert df of values to QColor for faster display in table.

    Parameters
    ----------
    df : pd.DataFrame

    highlight_funcs : dict
        {col_name: func_to_apply}
    """
    df_out = pd.DataFrame(data=None, index=df.index, columns=df.columns)
    for col, func in highlight_funcs.items():
        try:
            if not func is None:
                df_out[col] = df[col].apply(lambda x: func(val=x, role=role))
        except Exception:
            pass

    return df_out


def from_snake(s: str):
    """Convert from snake case cols to title"""
    return s.replace('_', ' ').title()


def to_snake(s: str):
    """Convert messy camel case to lower snake case

    Parameters
    ----------
    s : str
        string to convert to special snake case

    Examples
    --------
    """
    s = remove_bad_chars(s).strip()  # get rid of /<() etc
    s = s.replace('  ', '')
    s = re.sub(r'[\]\[()]', '', s)  # remove brackets/parens
    s = re.sub(r'[\n-]', '_', s)  # replace newline/dash with underscore
    s = re.sub(r'[%]', 'pct', s)
    s = re.sub(r"'", '', s)
    s = re.sub(r'#', 'no', s)

    # split on capital letters
    expr = r'(?<!^)((?<![A-Z])|(?<=[A-Z])(?=[A-Z][a-z]))(?=[A-Z])'

    return re \
        .sub(expr, '_', s) \
        .lower() \
        .replace(' ', '_') \
        .replace('__', '_')


def lower_cols(df: pd.DataFrame, title: bool = False) -> Union[List[str], pd.DataFrame]:
    """Convert df columns/list to snake case and remove bad characters"""
    is_list = False

    if isinstance(df, pd.DataFrame):
        cols = df.columns
    else:
        cols = as_list(df)
        is_list = True

    func = to_snake if not title else from_snake

    m_cols = {col: func(col) for col in cols}

    if is_list:
        return list(m_cols.values())

    return df.pipe(lambda df: df.rename(columns=m_cols))


def sql_from_file(p: Path) -> str:
    """Read sql string from .sql file

    Parameters
    ----------
    p : Path
        Path to .sql file

    Returns
    -------
    str
        sql string
    """
    with open(p, 'r') as file:
        return file.read()


def get_sql_filepath(name: str):
    """Find sql file in sql dir for frozen vs not frozen"""
    return list(cf.p_sql.rglob(f'*{name}.sql'))[0]


def discord(msg, channel='orders'):
    from discord import RequestsWebhookAdapter, Webhook

    from jgutils.secrets import SecretsManager

    df = SecretsManager('discord.csv').load.set_index('channel')
    r = df.loc[channel]
    if channel == 'err':
        msg += '@here'

    # Create webhook
    webhook = Webhook.partial(r.id, r.token, adapter=RequestsWebhookAdapter())

    # split into strings of max 2000 char for discord
    n = 2000
    out = [(msg[i:i + n]) for i in range(0, len(msg), n)]

    for msg in out:
        webhook.send(msg)


def open_url(url: str) -> None:
    """Open url in browser

    Parameters
    ----------
    url : str
        url to navigate to
    """
    webbrowser.open(url)
