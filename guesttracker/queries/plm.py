import operator as op
from typing import *

import numpy as np
import pandas as pd
from pypika import MSSQLQuery as Query
from pypika import Table as T
from pypika import functions as fn

from smseventlog import delta, dt
from smseventlog import functions as f
from smseventlog import getlog
from smseventlog import styles as st
from smseventlog.database import db
from smseventlog.queries import QueryBase, first_last_month, last_day_month
from smseventlog.queries.smr import UnitSMRMonthly

if TYPE_CHECKING:
    from pandas.io.formats.style import Styler

log = getlog(__name__)


class PLMUnit(QueryBase):
    def __init__(
            self,
            unit: str,
            d_upper: dt = None,
            d_lower: dt = None,
            **kw):
        """Select PLM report data for single unit.
        Parameters
        ----------
        unit : str
        d_upper : dt
        d_lower : dt
            If None, default to d_upper - 6 months
            Not needed if just using max_date
        """
        super().__init__(select_tablename='viewPLM')
        # use_cached_df = True # hmmm dont actually need this
        a = self.select_table
        cols = [a.star]

        if d_lower is None:
            # always start at first day of month
            if d_upper is None:
                d_upper = dt.now()

            d_lower = first_last_month(d_upper + delta(days=-180))[0]

        if not d_upper is None:
            d_rng = (d_lower, d_upper + delta(days=1))  # between cuts off at T00:00:00

        q = Query.from_(a) \
            .orderby(a.datetime)

        f.set_self(vars())
        self.set_default_args()  # NOTE not sure if need this or if report always does it

    def set_default_args(self, **kw):
        self.add_fltr_args([
            dict(vals=dict(unit=self.unit)),
            dict(vals=dict(datetime=self.d_rng), term='between')])

    @property
    def df_calc(self):
        """Calculate columns before aggregating"""
        df = self.df.copy()

        def where(cond):
            """Quicker way to assign val for summing"""
            return np.where(cond, 1, 0)

        return df \
            .assign(
                TotalLoads=1,
                Total_110=lambda x: where(
                    (x.GrossPayload_pct > 1.1) &
                    (x.GrossPayload_pct < 1.2) &  # <=
                    (x.ExcludeFlags == 0)),
                Total_120=lambda x: where(
                    (x.GrossPayload_pct >= 1.2) &
                    (x.ExcludeFlags == 0))) \
            .assign(
                Dumped_1KM_110=lambda x: where(
                    (x.Total_110 == 1) &
                    (x.L_HaulDistance <= 1)),
                Lower_110_Shovel=lambda x: where(
                    (x.Total_110 == 1) &
                    (x.L_HaulDistance > 1) &
                    (x.QuickShovelEst_pct <= 1.1)),
                Dumped_1KM_120=lambda x: where(
                    (x.Total_120 == 1) &
                    (x.L_HaulDistance < 1)),
                No_GE_Code=lambda x: where(
                    (x.Total_120 == 1) &
                    (x.L_HaulDistance > 1) &
                    # (x.QuickShovelEst_pct <= 1.1) &
                    (x.QuickPayload_pct <= 1.2)))

    def add_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipe assigning totals so can be done monthly or with final summary"""
        return df.assign(
            Total_ExcludeFlags=lambda x: x.TotalLoads - x.ExcludeFlags,
            Accepted_110=lambda x: x.Total_110 - x.Dumped_1KM_110 - x.Lower_110_Shovel,
            Accepted_120=lambda x: x.Total_120 - x.Dumped_1KM_120 - x.No_GE_Code) \
            .assign(
                Accepted_100=lambda x: x.Total_ExcludeFlags - x.Accepted_120 - x.Accepted_110,
                Overload_pct_110=lambda x: x.Accepted_110 / x.Total_ExcludeFlags,
                Overload_pct_120=lambda x: x.Accepted_120 / x.Total_ExcludeFlags)

    def df_monthly(self, add_unit_smr=False):
        """Bin data into months for charting, will include missing data = good"""

        # set DateIndex range to lower and upper of data (wouldn't show if data was missing)
        d_rng = self.d_rng
        d_rng = (d_rng[0], last_day_month(d_rng[1]))

        df = self.df_calc \
            .groupby(pd.Grouper(key='DateTime', freq='M')) \
            .sum() \
            .pipe(self.add_totals) \
            .pipe(lambda df: df.set_index(df.index.to_period())) \
            .pipe(self.expand_monthly_index, d_rng=d_rng)

        if add_unit_smr:
            # combine df_smr SMR_worked with plm df
            query_smr = UnitSMRMonthly(unit=self.unit)
            df_smr = query_smr.df_monthly()[['SMR Operated']]

            df = df.merge(df_smr, how='left', left_index=True, right_index=True)

        return df

    @property
    def df_summary(self):
        """Create single summary row from all haulcycle records"""
        # need to grouby first to merge the summed values
        df = self.df_calc
        if df.shape[0] == 0:
            log.warning(f'No results in PLM query dataframe. Unit: {self.unit}')
            return

        gb = df.groupby('Unit')

        return gb \
            .agg(
                MinDate=('DateTime', 'min'),
                MaxDate=('DateTime', 'max'),
                TargetPayload=('TargetPayload', 'median')) \
            .merge(right=gb.sum().loc[:, 'ExcludeFlags':], on='Unit', how='left') \
            .pipe(self.add_totals) \
            .reset_index(drop=False)

    def df_summary_report(self):
        """Pivot single row of PLM summary into df for report."""
        # if df is None or not 0 in df.index:
        #     raise AttributeError('Can\'t pivot dataframe!')
        m = self.df_summary.iloc[0].to_dict()

        cols = ['110 - 120%', '>120%']
        data = {
            'Total loads': (m['Total_110'], m['Total_120']),
            'Dumped within 1km': (m['Dumped_1KM_110'], m['Dumped_1KM_120']),
            '<110% at shovel': (m['Lower_110_Shovel'], ),
            'No GE code': (None, m['No_GE_Code']),
            'Loads accepted': (m['Accepted_110'], m['Accepted_120']),
            '% of loads accepted': (m['Overload_pct_110'], m['Overload_pct_120'])}

        return pd.DataFrame.from_dict(data, orient='index', columns=cols) \
            .reset_index() \
            .rename(columns=dict(index='Load Range'))

    def max_date(self):
        a = T('viewPLM')
        q = a.select(fn.Max(a.DateTime)) \
            .where(a.Unit == self.unit)

        return db.max_date_db(q=q)

    def style_overloads(self, style: 'Styler') -> 'Styler':
        """
        - highlight L_HaulDistance > 1.0 red
        - QuickPayload_pct >= 1.20
        - highlight statusflags BCDNL
        - highlight accepted = True
        """
        style.set_table_attributes('class="pagebreak_table"')

        m_rename = dict(
            StatusFlag='Flag',
            # Payload_Gross='Payload (Gross)',
            Payload_Quick_Carry='Payload_Quick',
            L_HaulDistance='Distance',
            GrossPayload_pct='Pct_Gross',
            QuickPayload_pct='Pct_Quick',
            # QuickShovelEst_pct='Shovel Est'
        )

        style.data = style.data.rename(columns=m_rename)
        style.columns = style.data.columns

        flags = 'BCDNL'
        col = 'Flag'
        m_fmt = dict(
            DateTime='{:%Y-%m-%d  %H:%M:%S}',
            Pct_Gross='{:.1%}',
            Pct_Quick='{:.1%}',
            Payload_Gross='{:.1f}',
            Payload_Quick='{:.1f}')

        return style \
            .pipe(st.format_dict, m_fmt) \
            .apply(
                st.highlight_val,
                axis=None,
                subset=self.subset_notnull(style, cols=col),
                target_col=col,
                bg_color='goodgreen',
                theme=self.theme,
                masks=style.data[col].astype(str).str.contains('|'.join([c for c in flags]))) \
            .apply(
                st.highlight_val,
                axis=None,
                subset='Overloaded',
                target_col='Overloaded',
                bg_color='lightred',
                theme=self.theme,
                val='True') \
            .pipe(
                st.highlight_numeric,
                target_col='Distance',
                val=1.0,
                opr=op.lt,
                bg_color='goodgreen',
                theme=self.theme) \
            .pipe(
                st.highlight_numeric,
                target_col='Pct_Quick',
                val=1.2,
                opr=op.lt,
                bg_color='goodgreen',
                theme=self.theme)

    def df_overloads(self) -> pd.DataFrame:
        """Show >120% overloads detailed data"""

        cols = ['DateTime', 'Payload_Gross', 'Payload_Quick_Carry', 'GrossPayload_pct',
                'QuickPayload_pct', 'StatusFlag', 'L_HaulDistance', 'Overloaded']
        # 'Payload_Net', 'QuickShovelEst_pct',

        return self.df_calc \
            .pipe(lambda df: df[df.GrossPayload_pct >= 1.2]) \
            .reset_index(drop=True) \
            .assign(
                Overloaded=lambda x: np.where(x.Total_120 & ~x.Dumped_1KM_120 & ~x.No_GE_Code, True, False))[cols]

    def highlight_accepted_loads(self, style: 'Styler') -> 'Styler':
        """Highlight accepted loads in summary table"""
        df = style.data
        subset = pd.IndexSlice[df.index[-1], df.columns[1:]]
        return style.apply(st.highlight_accepted_loads, subset=subset, axis=None)

    def format_custom(self, style: 'Styler', subset: List[str], type_: str = 'int') -> 'Styler':
        """Format first rows of summary table as int, last row as percent"""
        m = dict(int='{:,.0f}', pct='{:,.2%}')
        return style.format(m[type_], subset=subset, na_rep='')

    def update_style(self, style: 'Styler', **kw) -> 'Styler':
        df = style.data
        # s = []
        # s.append(dict(
        #     selector='th, td',
        #     props=[
        #         ('font-size', '12px')]))

        cols = df.columns[1:]

        # .pipe(st.add_table_style, s=s) \
        return style \
            .pipe(self.highlight_accepted_loads) \
            .pipe(self.format_custom, subset=pd.IndexSlice[df.index[:-1], cols], type_='int') \
            .pipe(self.format_custom, subset=pd.IndexSlice[df.index[-1], cols], type_='pct')
