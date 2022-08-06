from typing import *

import pandas as pd
import pypika as pk
from dateutil.relativedelta import relativedelta
from pypika import CustomFunction as cfn
from pypika import MSSQLQuery as Query
from pypika import Table as T
from pypika import functions as fn

from guesttracker import delta, dt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker import styles as st
from guesttracker.database import db
from guesttracker.errors import ExpectedError
from guesttracker.queries import QueryBase

log = getlog(__name__)


class UnitSMR(QueryBase):
    """Return all SMR values for single unit"""

    def __init__(self, unit: str, d_rng: Tuple[dt, dt] = None, **kw):
        super().__init__(**kw)
        a = T('UnitSMR')

        if d_rng is None:
            d_upper = dt.now()
            d_lower = d_upper + delta(days=-60)
            d_rng = (d_lower, d_upper)

        cols = ['Unit', 'DateSMR', 'SMR']

        q = Query.from_(a) \
            .where(a.Unit == unit) \
            .where(a.DateSMR.between(d_rng[0], d_rng[1]))

        f.set_self(vars())

    def process_df(self, df):
        """Add index of all dates in d_rng"""
        return pd.period_range(*self.d_rng, freq='d') \
            .to_timestamp() \
            .to_frame(name='DateSMR') \
            .reset_index(drop=True) \
            .assign(Unit=self.unit) \
            .merge(right=df[['DateSMR', 'SMR']], on='DateSMR', how='left')[['Unit', 'DateSMR', 'SMR']]


class UnitSMRMonthly(QueryBase):
    """Return max smr per month per unit, grouped monthly"""

    def __init__(self, unit: str = None, minesite: str = None, **kw):
        super().__init__(**kw)
        a, b = pk.Tables('UnitSMR', 'UnitID')

        _year = cfn('YEAR', ['date'])
        _month = cfn('MONTH', ['date'])
        year = _year(a.DateSMR)
        month = _month(a.DateSMR)
        _period = fn.Concat(year, '-', month)

        cols = [a.Unit, _period.as_('Period'), fn.Max(a.SMR).as_('SMR')]
        q = Query.from_(a) \
            .left_join(b).on_field('Unit') \
            .groupby(a.Unit, _period)

        if not unit is None:
            q = q.where(a.Unit == unit)

        if not minesite is None:
            q = q.where(b.MineSite == minesite)

        f.set_self(vars())

    def process_df(self, df):
        """
        - expand_monthly_index to fill missing months per unit
        - forward fill SMR per unit
        - get SMR_worked as diff of smr per month per unit
        - drop NA rows on SMR (no SMR values for start of unit's life)
        """

        return df \
            .assign(
                Period=lambda x: pd.to_datetime(x.Period, format='%Y-%m').dt.to_period('M')) \
            .set_index('Period') \
            .pipe(self.expand_monthly_index, group_col='Unit') \
            .sort_values(['Unit', 'Period']) \
            .assign(SMR=lambda x: x.groupby('Unit').ffill()) \
            .assign(SMR_worked=lambda x: x.groupby('Unit').SMR.diff().fillna(0)) \
            .reset_index(drop=False) \
            .set_index('Period') \
            .dropna(subset=['SMR'])

    def df_monthly(self, max_period=None, n_periods=0, totals=False, **kw):
        df = self.df
        if max_period is None:
            max_period = df.index.max()

        return df \
            .pipe(lambda df: df[df.index <= max_period]) \
            .iloc[n_periods * -1:, :] \
            .pipe(self.append_totals, do=totals) \
            .rename(columns=dict(SMR_worked='SMR Operated'))

    def append_totals(self, df, do=True):
        if not do:
            return df

        max_year = df.index.max().to_timestamp().year
        data = dict(
            Period=['Total YTD', 'Total'],
            SMR_worked=[df[df.index >= str(max_year)].SMR_worked.sum(), df.SMR_worked.sum()])
        df2 = pd.DataFrame(data)

        return df \
            .reset_index(drop=False) \
            .append(df2, ignore_index=True)

    def style_f300(self, style):
        return style \
            .pipe(st.highlight_totals_row, n_cols=2)


class UnitSMRReport(QueryBase):
    """Return Unit SMR on first day of current and next month to calc usage in month"""

    def __init__(self, d: dt, minesite='FortHills', **kw):
        super().__init__(**kw)
        a, b = pk.Tables('UnitID', 'UnitSMR')

        d_lower = dt(d.year, d.month, 1)
        dates = (d_lower, d_lower + relativedelta(months=1))  # (2020-12-01, 2021-01-01)

        cols = [a.Unit, b.DateSMR, b.SMR]

        q = Query.from_(a).select(*cols) \
            .left_join(b).on_field('Unit') \
            .where((a.MineSite == minesite) & (b.DateSMR.isin(dates) & (a.ExcludeMA.isnull())))

        f.set_self(vars())

    def process_df(self, df):
        """Pivot dates for first of months, then merge unit delivery date/serial from db
        - will fail if most recent dates not in db
        """

        # warn user if most recent dates not in db
        if len(df.DateSMR.unique()) != 2:
            raise ExpectedError(f'Expected 2 dates for SMR report, got {df.DateSMR.nunique()}')

        df = df \
            .assign(DateSMR=lambda x: x.DateSMR.dt.strftime('%Y-%m-%d')) \
            .pivot(index='Unit', columns='DateSMR', values='SMR') \
            .rename_axis('Unit', axis=1) \
            .assign(Difference=lambda x: x.iloc[:, 1] - x.iloc[:, 0])

        return db.get_df_unit(minesite=self.minesite) \
            .set_index('Unit')[['Serial', 'DeliveryDate']] \
            .merge(right=df, how='right', on='Unit') \
            .reset_index()

    def update_style(self, style, **kw):

        return style \
            .apply(
                st.background_grad_center,
                subset='Difference',
                cmap=self.cmap.reversed())
