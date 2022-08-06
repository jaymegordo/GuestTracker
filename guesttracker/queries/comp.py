from typing import *

import pandas as pd
from dateutil.relativedelta import relativedelta
from pypika import Case
from pypika import MSSQLQuery as Query
from pypika import Order
from pypika import Table as T
from pypika.analytics import RowNumber

from guesttracker import delta, dt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker import styles as st
from guesttracker.queries import QueryBase, first_last_month
from guesttracker.queries.el import EventLogBase

log = getlog(__name__)


class ComponentCOBase(EventLogBase):
    def __init__(self, da=None, **kw):
        super().__init__(da=da, **kw)
        a, b, c, d, e = self.a, self.b, T('ComponentType'), self.d, T('ComponentBench')

        life_achieved = Case().when(a.SMR == 0, None).else_(a.ComponentSMR - e.bench_smr).as_('Life Achieved')

        q = self.q \
            .inner_join(c).on_field('Floc') \
            .left_join(e).on(
                (a.Floc == e.floc) &
                ((b.Model == e.model) | (d.ModelBase == e.model_base) | (d.EquipClass == e.equip_class))) \
            .orderby(a.Unit, a.DateAdded, c.Modifier, a.GroupCO)

        f.set_self(vars())

        self.default_dtypes |= f.dtypes_dict('Int64', ['Bench SMR', 'Life Achieved', 'Install SMR'])

    def wrapper_query(self, q: Query) -> Query:
        """Call to wrap query before final call, keeps refreshtable args with base query"""

        a, b, e = self.a, self.b, self.e
        q._orderbys = []  # this is a pk attr, need to remove initial orderbys and redo

        # original cols already selected, just add rn
        c = q \
            .select(
                (RowNumber()
                    .over(a.unit, a.floc, a.dateadded)
                    .orderby(e.model, e.model_base, e.equip_class, order=Order.desc)).as_('rn')) \
            .as_('sq0')

        # use process_df to drop "rn"
        return Query.from_(c) \
            .select(c.star) \
            .where(c.rn == 1) \
            .orderby(c.Unit, c.DateAdded, c.Modifier, c.GroupCO)

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=['rn'])

    def set_default_filter(self, **kw):
        super().set_default_filter(**kw)
        self.fltr.add(vals=dict(DateAdded=dt.now().date() + delta(days=-30)))

    def set_fltr(self):
        super().set_fltr()
        self.fltr.add(vals=dict(ComponentCO='True'))

    def set_allopen(self, **kw):
        self.fltr.add(field='COConfirmed', val='False')

    def style_life_achieved(self, style):
        df = style.data
        subset = pd.IndexSlice[df['Life Achieved'].notnull(), 'Life Achieved']

        return style \
            .apply(
                st.background_grad_center,
                cmap=self.cmap.reversed(),
                subset=subset)


class ComponentCO(ComponentCOBase):
    def __init__(self, **kw):
        super().__init__(**kw)
        a, b, c, d, e = self.a, self.b, self.c, self.d, self.e

        cols = [
            a.UID, b.MineSite, b.Model, a.Unit, c.Component, c.Modifier, a.GroupCO, a.DateAdded,
            a.SMR, a.ComponentSMR, e.bench_smr, self.life_achieved, a.SNRemoved, a.SNInstalled,
            a.install_smr, a.WarrantyYN, a.CapUSD, a.WorkOrder, a.SuncorWO, a.SuncorPO, a.Reman,
            a.SunCOReason, a.FailureCause, a.COConfirmed]

        f.set_self(vars())

    def update_style(self, style, **kw):
        # only using for highlight_alternating units
        color = 'navyblue' if self.theme == 'light' else 'maroon'
        col = 'Install SMR'

        return style \
            .apply(st.highlight_alternating, subset=['Unit'], theme=self.theme, color=color) \
            .pipe(self.style_life_achieved) \
            .apply(
                st.highlight_val,
                axis=None,
                subset=self.subset_notnull(style, cols=col),
                target_col=col,
                bg_color='lightyellow',
                theme=self.theme,
                masks=style.data[col] != 0)


class ComponentSMR(QueryBase):
    def __init__(self, **kw):
        super().__init__(**kw)
        a = self.select_table
        cols = [a.MineSite, a.Model, a.Unit, a.Component, a.Modifier, a.bench_smr, a.CurrentUnitSMR,
                a.SMRLastCO, a.CurrentComponentSMR, a.PredictedCODate, a.LifeRemaining, a.SNInstalled]

        q = Query.from_(a) \
            .orderby(a.MineSite, a.Unit)

        f.set_self(vars())

        self.default_dtypes |= f.dtypes_dict(
            'Int64', ['Bench SMR', 'Curr Unit SMR', 'SMR Last CO', 'Curr Comp SMR', 'Life Remaining'])

    def set_default_filter(self, **kw):
        self.set_allopen()

    def set_allopen(self, **kw):
        self.set_minesite(table=self.select_table)
        self.fltr.add(vals=dict(major=1))

    def background_gradient(self, style):
        df = style.data
        subset = pd.IndexSlice[df['Life Remaining'].notnull(), 'Life Remaining']
        return style.background_gradient(
            cmap=self.cmap.reversed(), subset=subset, axis=None, vmin=-1000, vmax=1000)

    def update_style(self, style, **kw):
        return style.pipe(self.background_gradient)


class ComponentCOReport(ComponentCOBase):
    def __init__(
            self,
            d_rng: Tuple[dt, dt],
            minesite: str = 'FortHills',
            major: bool = False,
            sort_component: bool = False, **kw):
        """da = d_rng, minesite"""
        super().__init__(da=dict(d_rng=d_rng, minesite=minesite), **kw)
        use_cached_df = True

        self.view_cols |= dict(BenchSMR='Bench SMR')

        a, b, c = self.a, self.b, self.c

        cols = [b.Model, a.Unit, c.Component, c.Modifier, a.DateAdded,
                a.ComponentSMR, c.BenchSMR, self.life_achieved, a.SunCOReason, a.GroupCO]

        if major:
            q = self.q.where(c.Major == 1)

        self.formats |= {
            'Bench_Pct_All': '{:.2%}'}

        f.set_self(vars())

    @classmethod
    def from_yearly(cls, d_upper: dt = None, **kw) -> 'ComponentCOReport':
        """Create instance from current date 12 month rolling
        - NOTE could make this from_all_time as well
        """
        if d_upper is None:
            d_upper = dt.now() + relativedelta(months=-1)
            d_upper = first_last_month(d_upper)[-1]  # get last day of prev month

        d_lower = d_upper + relativedelta(months=-12) + delta(days=1)  # first of month - 12

        return cls(d_rng=(d_lower, d_upper), **kw)

    def set_default_args(self, d_rng, minesite):
        """Set date range and minesite"""
        self.add_fltr_args([
            dict(vals=dict(DateAdded=d_rng), term='between'),
            dict(vals=dict(MineSite=minesite), table=self.b)])

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().process_df(df)
        # df[cols] = df[cols].fillna(pd.NA)
        df.pipe(f.convert_dtypes, cols=['Comp SMR', 'Life Achieved', 'Bench SMR'], col_type='Int64')

        if self.sort_component:
            df = df.sort_values(['Component', 'CO Date'])

        return df

    def update_style(self, style, **kw):
        df = style.data
        subset = pd.IndexSlice[df['Life Achieved'].notnull(), 'Life Achieved']
        return style \
            .hide(subset=['Group CO'], axis='columns') \
            .pipe(self.style_life_achieved) \
            .pipe(st.add_table_attributes, s='class="pagebreak_table"')

    @property
    def mask_planned(self):
        return self.df['Removal Reason'] == 'High Hour Changeout'

    @property
    def mask_failed(self):
        return self.df['Removal Reason'].isin(['Failure', 'Warranty'])

    def exec_summary(self):
        m = {}
        df = self.df
        s = df['Removal Reason']

        m['Planned/Unplanned'] = {
            'Planned': s[self.mask_planned].count(),
            'Unplanned': s[~self.mask_planned].count(),
            'Total': s.count()}

        m['Failures'] = {
            'Failed': s[self.mask_failed].count(),
            'Convenience/High Hour/Damage/Other': s[~self.mask_failed].count(),
            'Total': s.count()}

        return m

    def df_component_period(self, period: str = 'quarter'):
        """Group component CO records by Quarter/Component for charting"""
        period = period.title()
        df = self.df \
            .assign(
                Failed=self.mask_failed,
                **{period: lambda x: x['CO Date'].dt.to_period(period[0].upper())})

        return df.groupby([period, 'Component']) \
            .agg(dict(Component='size', Failed='sum')) \
            .assign(not_failed=lambda x: x.Component - x.Failed) \
            .rename(columns=dict(Component='Count', not_failed='Not Failed')) \
            .reset_index(drop=False)
        # .size() \
        # .reset_index(name='Count')

    def df_failures(self):
        """Group failures into failed/not failed, with pct of each group total
        - Used for chart_comp_failure_rates"""
        df = self.df.copy()
        df['Failed'] = self.mask_failed
        df2 = df.groupby(['Component', 'Failed']) \
            .size() \
            .reset_index(name='Count')

        # get percent of failed/not failed per component group
        df2['Percent'] = df2.groupby(['Component']).apply(lambda g: g / g.sum())['Count']

        return df2

    def df_mean_life(self):
        """Group by component, show mean life total, failed, not failed"""
        df = self.df.copy()

        # change 'warranty' to 'failure
        x = 'Removal Reason'
        df[x] = df[x].replace(dict(Warranty='Failure'))

        df = df[df[x].isin(['Failure', 'High Hour Changeout'])]

        df2 = df.groupby('Component').agg({'Bench SMR': 'mean', 'Comp SMR': 'mean'})

        df3 = df \
            .groupby(['Component', 'Removal Reason'])['Comp SMR'] \
            .mean() \
            .reset_index(drop=False) \
            .pivot(index='Component', columns='Removal Reason', values='Comp SMR')

        return df2.merge(right=df3, how='left', on='Component') \
            .rename(columns={
                'Comp SMR': 'Mean_All',
                'Failure': 'Mean_Failure',
                'High Hour Changeout': 'Mean_HighHour'}) \
            .astype(float).round(0).astype('Int64') \
            .reset_index(drop=False) \
            .assign(Bench_Pct_All=lambda x: x['Mean_All'] / x['Bench SMR'])

    def update_style_mean_life(self, style, **kw):
        df = style.data
        # subset = pd.IndexSlice[df['Life Achieved'].notnull(), 'Life Achieved']
        subset = ['Bench_Pct_All']
        return style \
            .background_gradient(
                cmap=self.cmap.reversed(), subset=subset, axis=None, vmin=0.5, vmax=1.5) \
            .pipe(st.format_dict, self.formats)
