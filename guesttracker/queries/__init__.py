import inspect
import operator as op
import time
from abc import ABCMeta
from typing import *

import pandas as pd
import pypika as pk
from dateutil.relativedelta import relativedelta
from pypika import Criterion
from pypika import Table as T

from guesttracker import config as cf
from guesttracker import date, delta, dt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker import styles as st
from guesttracker.database import db
from guesttracker.errors import SettingsError
from guesttracker.utils import dbmodel as dbm

if not cf.AZURE_WEB:
    # dont want to include seaborn in azure functions package
    from seaborn import diverging_palette
    cmap_default = diverging_palette(240, 10, sep=10, n=21, as_cmap=True)

    from guesttracker.gui import _global as gbl
else:
    cmap_default = None
    gbl = None

if TYPE_CHECKING:
    from pandas.io.formats.style import Styler

    # imports for type checking with qr.ExampleQuery. Annoying/manual but works
    from guesttracker.gui.tables import TableWidget  # noqa
    from guesttracker.queries.avail import AvailTopDowns  # noqa
    from guesttracker.queries.avail import (  # noqa
        Availability, AvailRawData, AvailShortfalls, AvailSummary)
    from guesttracker.queries.comp import ComponentSMR  # noqa
    from guesttracker.queries.comp import (  # noqa
        ComponentCO, ComponentCOReport)
    from guesttracker.queries.el import WorkOrders  # noqa
    from guesttracker.queries.el import TSI, EmailList  # noqa
    from guesttracker.queries.fc import FCSummaryReport2  # noqa
    from guesttracker.queries.fc import NewFCs  # noqa
    from guesttracker.queries.fc import FCComplete, FCDetails  # noqa
    from guesttracker.queries.misc import ACMotorInspections  # noqa
    from guesttracker.queries.misc import TableKeys  # noqa
    from guesttracker.queries.misc import TableKeysPLM  # noqa
    from guesttracker.queries.misc import FileQuery, FrameCracks, Parts  # noqa
    from guesttracker.queries.oil import OilSamplesReport  # noqa
    from guesttracker.queries.oil import OilSamples, OilSamplesRecent  # noqa
    from guesttracker.queries.plm import PLMUnit  # noqa
    from guesttracker.queries.smr import UnitSMRReport  # noqa
    from guesttracker.queries.smr import UnitSMR, UnitSMRMonthly  # noqa


log = getlog(__name__)
week_letter = 'W'
"""
- Queries control how data is queried/filtered from database.
- Can be consumed by tables/views, reports, etc
- da is 'default args' to be passed to filter when query is executed
"""


class Filter():
    def __init__(self, parent):
        # fltr has to belong to a query object
        criterion, fields = {}, {}
        select_table = parent.select_table
        f.set_self(vars())

    def add(
            self,
            field: Union[str, None] = None,
            val: Union[Any, None] = None,
            vals: Union[Dict[str, str], None] = None,
            opr: Union[Callable, None] = None,
            term: Union[str, None] = None,
            table: Union[str, None] = None,
            ct: Union[Criterion, None] = None):
        """Add query filter

        Parameters
        ----------
        field : str, optional
            field to filter on, default None
        val : str, optional
            val to filter on, default None
        vals : dict, optional
            dict of {field: val}, default None
        opr : operator, optional
            eg opr.eq, default None
        term : str, optional
            filter term eg "between", default None
        table : str | pk.Table, optional
            table if not using query's default table, default None
        ct : pk.Criterion, optional
            fully formed criterion, eg multiple statements or "or" etc, default None

        Returns
        -------
        qr.Filter
            self
        """
        if not vals is None:
            # not pretty, but pass in field/val with dict a bit easier
            field = list(vals.keys())[0]
            val = list(vals.values())[0]

        # special case when MineSite == 'CustomSites'
        if not cf.AZURE and field == 'MineSite' and val == 'CustomSites':
            sites = gbl.get_setting('custom_minesites', None)
            if sites:
                val = sites
                term = 'isin'

        if table is None:
            table = self.select_table
        elif isinstance(table, str):
            table = T(table)

        field_ = table.field(field)
        if ct is None:
            if not term is None:
                func = getattr(field_, term)
                if val:
                    if term == 'between':
                        ct = func(*val)  # between
                    else:
                        ct = func(val)  # isin, etc
                else:
                    ct = func()

            elif isinstance(val, str):
                val = val.replace('*', '%')
                if '%' in val:
                    ct = field_.like(val)
                else:
                    if opr is None:
                        opr = op.eq
                    ct = opr(field_, val)

            elif isinstance(val, (int, float)):
                if opr is None:
                    opr = op.eq
                ct = opr(field_, val)

            elif isinstance(val, (dt, date)):
                if opr is None:
                    opr = op.ge
                ct = opr(field_, val)

        self.add_criterion(ct=ct)
        return self

    def add_criterion(self, ct):
        # check for duplicate criterion, use str(ct) as dict key for actual ct
        # can also use this to pass in a completed pk criterion eg (T().field() == val)
        self.criterion[str(ct)] = ct
        if isinstance(ct, pk.terms.ComplexCriterion):
            return  # cant use fields in complexcriterion for later access but whatever

        if hasattr(ct, 'left'):
            field = ct.left.name
        elif hasattr(ct, 'term'):
            field = list(ct.term.fields_())[0].name

        self.fields[field.lower()] = ct

    def check_criterion(self, field):
        # check if field is in criterion fields - not sure if I need this
        lst = list(filter(lambda x: field.lower() in x.lower(), self.criterion))
        ans = True if lst else False
        return ans

    def get_criterion(self, field: str) -> str:
        """Return criterion containing selected field

        Parameters
        ----------
        field : str
            partial field name to match

        Returns
        -------
        str
            criterion string (need to access object through dict keys)

        Examples
        --------
        >>> ct = fltr.get_criterion('PartName')
            flter.criterion[ct] = ...
        """
        lst = list(filter(lambda x: field.lower() in x.lower(), self.criterion))
        ans = lst[0] if lst else None
        return ans

    def get_all_criterion(self):
        return self.criterion.values()

    def expand_criterion(self):
        return Criterion.all(self.get_all_criterion())

    def is_init(self):
        return len(self.criterion) > 0

    def print_criterion(self):
        for ct in self.criterion.values():
            print('\t', list(ct.tables_)[0], ct)


class QueryBase(metaclass=ABCMeta):
    def __init__(
            self,
            parent: 'TableWidget' = None,
            minesite: str = None,
            da: dict = None,
            theme: str = 'light',
            select_tablename: str = None,
            use_cached_df: bool = False,
            title: Union[str, None] = None):

        self.parent = parent
        self.minesite = minesite
        self.da = da
        self.theme = theme
        self.use_cached_df = use_cached_df
        self.formats, self.default_dtypes, self.stylemap_cols = {}, {}, {}
        self.background_gradients = []
        self._minesite_default = 'FortHills'
        self.cmap = cmap_default
        self.sql = None
        self.df = pd.DataFrame()
        self.df_loaded = False
        self.data_query_time = 0.0

        m = cf.config['TableName']
        self.color = cf.config['color']
        self.name = self.__class__.__name__
        self.query_key = f'queries/{self.name.lower()}'  # saving last_query

        # loop base classes to get first working title, need this to map view_cols
        if title is None:
            for base_class in inspect.getmro(self.__class__):
                title = m['Class'].get(base_class.__name__, None)
                if not title is None:
                    break

        # loop through base classes till we find a working select_table
        if select_tablename is None:
            for base_class in inspect.getmro(self.__class__):
                select_tablename = m['Select'].get(base_class.__name__, None)
                if not select_tablename is None:
                    break

        self.select_table = T(select_tablename)

        # try to get updatetable, if none set as name of select table
        if not select_tablename is None:
            self.update_tablename = m['Update'].get(self.name, select_tablename)
            self.update_table = getattr(dbm, self.update_tablename, None)  # type: dbm.Base

        # set dict for db > view_col conversion later
        self.view_cols = f.get_dict_db_view(title=title)
        self.title = title
        self.set_fltr()

    @property
    def minesite(self):
        # can either pass in a minesite for reports/etc, or use GUI parent's
        if hasattr(self, '_minesite') and not self._minesite is None:
            return self._minesite
        elif not self.parent is None:
            return self.parent.minesite
        else:
            from guesttracker.gui import _global as gbl
            return gbl.get_minesite()

    @minesite.setter
    def minesite(self, val):
        self._minesite = val

    def get_sql(self, last_query: bool = False, save_query: bool = True, **kw) -> str:
        """Return sql from query object.
        Parameters
        ----------
        last_query : bool, optional
            Refresh using last saved sql query, by default False
        save_query : bool, optional
            save query to settings for reuse with refresh_last_query

        Returns
        -------
        str
            SQL string, consumed in database.get_df
        """
        if last_query and not cf.AZURE:
            last_sql = gbl.get_setting(self.query_key, None)  # load from qsettings
            if not last_sql is None:
                return last_sql
            else:
                raise SettingsError('No previous query saved.')

        sql, da = self.sql, self.da

        if sql is None:
            q = self.q
            if hasattr(self, 'process_criterion'):
                self.process_criterion()

            if not da is None and hasattr(self, 'set_default_args'):
                self.set_default_args(**da)

            # NOTE could build functionality for more than one subquery
            fltr2 = self.fltr2
            if fltr2.is_init() and hasattr(self, 'sq0'):
                self.sq0 = self.sq0.where(fltr2.expand_criterion())

            if hasattr(self, 'get_query'):  # need to do this after init for queries with subqueries
                q = self.get_query()

            # no select cols defined yet
            if q.get_sql() == '':
                q = q.select(*self.cols)

            q = q.where(self.fltr.expand_criterion())

            # allow adding a wrapper eg for row numbering, but keep filters with base query
            if hasattr(self, 'wrapper_query'):
                q = self.wrapper_query(q)

            sql = str(q)

            # save previous query to qsettings
            if cf.IS_QT_APP and save_query:
                gbl.get_settings().setValue(self.query_key, sql)

        return sql

    def set_fltr(self):
        self.fltr = Filter(parent=self)
        self.fltr2 = Filter(parent=self)

    def set_lastperiod(self, days=7):
        if hasattr(self, 'date_col') and not self.date_col is None:
            vals = {self.date_col: dt.now().date() + delta(days=days * -1)}
            self.fltr.add(vals=vals, opr=op.ge)
            return True
        else:
            return False

    def set_lastweek(self):
        return self.set_lastperiod(days=7)

    def set_lastmonth(self):
        return self.set_lastperiod(days=31)

    def get_updatetable(self):
        tablename = self.select_table if self.update_table is None else self.select_table
        return getattr(dbm, tablename)  # db model definition, NOT instance

    def add_extra_cols(self, cols: list):
        """Add extra columns to query

        Parameters
        ----------
        cols : list | string
            Item(s) to add
        """
        if not isinstance(cols, list):
            cols = [cols]
        self.cols = self.cols + cols

    def add_fltr_args(self, args, subquery=False):
        """Add multiple filters to self.fltr as list

        Parameters
        ----------
        args : list
            list of key:vals with opional other args
        subquery : bool, optional
            use self.fltr2, default False
        """
        if not isinstance(args, list):
            args = [args]

        fltr = self.fltr if not subquery else self.fltr2

        for da in args:
            fltr.add(**da)

    def _set_default_filter(self, do=False, **kw):
        """Just used for piping"""
        if do and hasattr(self, 'set_default_filter'):
            self.set_default_filter(**kw)

        return self

    def _set_base_filter(self, do=False, **kw):
        """Just used for piping"""
        if do and hasattr(self, 'set_base_filter'):
            self.set_base_filter(**kw)

        return self

    def process_df(self, df):
        """Placeholder for piping"""
        return df

    def _process_df(self, df, do=True):
        """Wrapper to allow skipping process_df for testing/troubleshooting"""
        if do:
            return df.pipe(self.process_df)
        else:
            return df

    @property
    def df(self):
        if not self.df_loaded:
            self.get_df()
        return self._df

    @df.setter
    def df(self, data):
        self._df = data

    def _get_df(self, default=False, base=False, prnt=False, skip_process=False, **kw) -> pd.DataFrame:
        """Execute query and return dataframe

        Parameters
        ----------
        default : bool, optional
            self.set_default_filter if default=True, default False
        base : bool, optional
            self.set_base_filter, default False
        prnt : bool, optional
            Print query sql, default False
        skip_process : bool, optional
            Allow skipping process_df for troubleshooting, default False

        Returns
        ---
        pd.DataFrame
        """
        self._set_default_filter(do=default, **kw) \
            ._set_base_filter(do=base, **kw)

        sql = self.get_sql(**kw)
        if prnt:
            print(sql)

        return pd \
            .read_sql(sql=sql, con=db.engine) \
            .pipe(f.default_df) \
            .pipe(f.convert_df_view_cols, m=self.view_cols) \
            .pipe(self._process_df, do=not skip_process) \
            .pipe(f.set_default_dtypes, m=self.default_dtypes)

    def get_df(self, cached: bool = False, **kw) -> pd.DataFrame:
        """Wrapper for _get_df

        Parameters
        ----------
        cached : bool, default False
            Use cached df if already loaded

        Returns
        ---
        pd.DataFrame
        """
        start = time.time()

        if (self.use_cached_df or cached) and self.df_loaded:
            return self.df

        try:
            self.df = self._get_df(**kw)
            self.df_loaded = True
            self.fltr.print_criterion()
        finally:
            # always reset filter after every refresh call
            self.set_fltr()

        # save data query time to display in EL
        self.data_query_time = time.time() - start
        return self.df

    def get_stylemap(self, df: pd.DataFrame, col: str = None) -> Tuple[dict, dict]:
        """Convert irow, icol stylemap to indexes
        - Consumed by datamodel set_stylemap()

        Returns
        ------
        tuple
            tuple of defaultdicts bg, text colors
        """
        if df.shape[0] <= 0 or not hasattr(self, 'update_style'):
            return None

        if col is None:
            # calc style for full dataframe
            style = df.style.pipe(self.update_style)
        else:
            # calc style for specific cols
            # NOTE need to have manually defined dict of sort cols - functions per query
            m = self.stylemap_cols[col]
            df = df[m['cols']]  # get slice of df
            style = df.style.pipe(m['func'], **m.get('da', {}))

        style._compute()
        return st.convert_stylemap_index_color(style=style)

    def subset_notnull(self, style: 'Styler', cols: Union[str, List[str]]) -> pd.Series:
        """Subset df column(s) to only not null rows

        Parameters
        ----------
        style : Styler
        cols : Union[str, List[str]]

        Returns
        -------
        pd.Series
            true/false mask where all rows in cols are not null
        """
        cols = f.as_list(cols)
        return pd.IndexSlice[style.data[cols].notnull().all(axis=1), cols]

    def set_minesite(self, table: str = 'UnitID'):
        self.fltr.add(vals=dict(MineSite=self.minesite), table=table)

    def expand_monthly_index(
            self,
            df: pd.DataFrame,
            d_rng: Tuple[dt, dt] = None,
            group_col: str = None) -> pd.DataFrame:
        """Expand/fill monthly PeriodIndex to include missing months"""
        s = df.index
        idx_name = s.name

        if d_rng is None:
            # expand to min and max existing dates
            try:
                d_rng = (s.min().to_timestamp(), s.max().to_timestamp() + relativedelta(months=1))
            except:
                log.info('No rows in monthly index to expand.')
                return df

        # create index from overall min/max dates in df
        idx = pd.date_range(d_rng[0], d_rng[1], freq='M').to_period()

        # create index with missing months per period/group (eg Unit)
        if not group_col is None:
            idx_name = [group_col, idx_name]
            idx = pd.MultiIndex.from_product([df[group_col].unique(), idx], names=idx_name)

            df = df.reset_index(drop=False) \
                .set_index(idx_name)

        return df \
            .merge(pd.DataFrame(index=idx), how='right', left_index=True, right_index=True) \
            .rename_axis(idx_name)


def table_with_args(table, args):
    def fmt(arg):
        if isinstance(arg, bool):
            return f"'{arg}'"
        elif isinstance(arg, int):
            return str(arg)
        else:
            return f"'{arg}'"

    str_args = ', '.join(fmt(arg) for arg in args.values())
    return f'{table}({str_args})'


# data range funcs
def first_last_month(d: dt) -> Tuple[dt, dt]:
    d_lower = dt(d.year, d.month, 1)
    d_upper = d_lower + relativedelta(months=1) + delta(days=-1)
    return (d_lower, d_upper)


def last_day_month(d):
    return first_last_month(d)[1]


def df_period(freq: str, n: int = 0, ytd: bool = False, n_years: int = 1) -> pd.DataFrame:
    """Return df of periods for specified freq

    Parameters
    ----------
    freq : str
        M or W
    n : int, optional
        filter last n periods, default 0
    ytd : bool, optional
        filter periods to start of year, default False
    n_years : int
        number of previous years

    Returns
    -------
    pd.DataFrame
        df of periods
    """
    freq = dict(month='M', week='W').get(freq, freq)  # convert from month/week
    d_upper = dt.now()
    d_lower = d_upper + delta(days=-365 * n_years)
    idx = pd.date_range(d_lower, d_upper, freq=freq).to_period()

    # fmt_week = f'%Y-%{week_letter}'
    fmt_week = '%G-%V'
    m = dict(
        W=dict(fmt_str=fmt_week),
        M=dict(fmt_str='%Y-%m')) \
        .get(freq)

    def _rename_week(df, do=False):
        if not do:
            return df
        return df \
            .assign(name=lambda x: x.period.dt.strftime(f'Week %{week_letter}'))

    def _filter_ytd(df, do=ytd):
        if not do:
            return df
        return df[df.period >= str(df.period.max().year)]

    df = pd.DataFrame(index=idx)

    return df \
        .assign(
            start_date=lambda x: pd.to_datetime(x.index.start_time.date),
            end_date=lambda x: pd.to_datetime(x.index.end_time.date),
            d_rng=lambda x: list(zip(x.start_date.dt.date, x.end_date.dt.date)),
            name=lambda x: x.index.to_timestamp(freq).strftime(m['fmt_str'])) \
        .rename_axis('period') \
        .reset_index(drop=False) \
        .set_index('name', drop=False) \
        .pipe(_filter_ytd, do=ytd) \
        .pipe(_rename_week, do=freq == 'W') \
        .rename(columns=dict(name='name_title')) \
        .iloc[-1 * n:]


def df_months():
    # Month
    cols = ['StartDate', 'EndDate', 'Name']
    d_start = dt.now() + delta(days=-365)
    d_start = dt(d_start.year, d_start.month, 1)

    m = {}
    for i in range(24):
        d = d_start + relativedelta(months=i)
        name = f'{d:%Y-%m}'
        m[name] = (*first_last_month(d), name)

    return pd.DataFrame.from_dict(m, columns=cols, orient='index')


def df_weeks():
    # Week
    cols = ['StartDate', 'EndDate', 'Name']

    m = {}
    year = dt.now().year
    for wk in range(1, 53):
        s = f'2020-W{wk-1}'
        d = dt.strptime(s + '-1', '%Y-W%W-%w').date()
        m[f'{year}-{wk}'] = (d, d + delta(days=6), f'Week {wk}')

    return pd.DataFrame.from_dict(m, columns=cols, orient='index')


def df_rolling_n_months(n: int = 12):
    """Create df of n rolling months with periodindex

    Parameters
    ----------
    n : int, optional
        n months, default 12
    """
    d_upper = last_day_month(dt.now() + relativedelta(months=-1))
    d_lower = d_upper + relativedelta(months=(n - 1) * -1)
    idx = pd.date_range(d_lower, d_upper, freq='M').to_period()
    return pd.DataFrame(data=dict(period=idx.astype(str)), index=idx) \
        .assign(
            d_lower=lambda x: x.index.to_timestamp(),
            d_upper=lambda x: x.d_lower + pd.tseries.offsets.MonthEnd(1))


f.import_submodule_classes(
    name=__name__,
    filename=__file__,
    gbls=globals(),
    parent_class='guesttracker.queries')
