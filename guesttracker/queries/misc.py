from pathlib import Path
from typing import *

import numpy as np
import pandas as pd
from pypika import MSSQLQuery as Query
from pypika import Table as T
from pypika import Tables

from jgutils import pandas_utils as pu
from smseventlog import config as cf
from smseventlog import dbtransaction as dbt
from smseventlog import dt
from smseventlog import functions as f
from smseventlog import getlog
from smseventlog import styles as st
from smseventlog.queries import QueryBase
from smseventlog.queries.el import EventLogBase

log = getlog(__name__)


class TableKeys(QueryBase):
    """Get database table key values for specific table"""

    def __init__(self, table_name: str, filter_vals: dict = None, **kw):
        super().__init__(select_tablename=table_name, **kw)

        # get dbtable obj to inspect for keys
        keys = dbt.get_dbtable_keys(table_name)
        keys = [key.lower() for key in keys]

        a = T(table_name)

        q = Query.from_(a) \
            .select(*keys)

        f.set_self(vars())

        # helper to add filter vals eg {'unit': 'F301'}
        if isinstance(filter_vals, dict):
            field = list(filter_vals.keys())[0]
            val = filter_vals[field]

            field_ = a.field(field)
            if isinstance(val, (list, tuple)):
                # key: list of vals
                self.fltr.add(ct=field_.isin(val))
            else:
                # single key: val
                self.fltr.add(vals=filter_vals)

    def get_df(self, **kw):
        """Make columns lowercase to match match cols easier"""
        return super().get_df(**kw) \
            .pipe(lambda df: df.rename(columns={col: col.lower() for col in df.columns}))

    def filter_existing(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter dataframe on keys from database, remove rows which exists in db"""
        rows = df.shape[0]
        df_keys = self.get_df()

        df = df \
            .merge(
                right=df_keys,
                on=self.keys,
                how='left',
                indicator=True) \
            .query('_merge == "left_only"') \
            .drop(columns=['_merge'])

        log.info(f'Filtered df - before: {rows}, after: {df.shape[0]}')
        return df


class TableKeysPLM(TableKeys):
    """Get all Unit, DateTime vals for specific unit
    - Used to filter PLM records before upload
    """

    def __init__(self, unit: Union[str, list], **kw):
        super().__init__(table_name='PLM', filter_vals=dict(unit=unit), **kw)


class FrameCracks(EventLogBase):
    def __init__(self, da=None, **kw):
        super().__init__(da=da, **kw)
        a, b = self.a, self.b
        cols = [a.Unit, a.DateAdded, a.Title, a.SMR, a.TSIPartName, a.TSIDetails, a.WOComments, a.TSINumber,
                a.SuncorWO, a.IssueCategory, a.SubCategory, a.Cause]

        q = self.q \
            .orderby(a.Unit, a.DateAdded)

        f.set_self(vars())
        self.set_minesite()

    def set_default_args(self, d_lower):
        a = self.a
        self.add_fltr_args([
            dict(vals=dict(DateAdded=d_lower)),
            dict(ct=(a.Title.like('crack')) | ((a.IssueCategory == 'frame') & (a.Cause == 'crack'))),
            dict(vals=dict(Model='%980%'), table=self.b)])

    def process_df(self, df):
        df = df.rename(columns=dict(SuncorWO='Order'))
        df.Order = pd.to_numeric(df.Order, errors='coerce').astype('Int64')
        return df


class FileQuery(QueryBase):
    """Query based on saved .sql file"""

    def __init__(self, p: Path = None, sql=None, **kw):
        super().__init__(**kw)
        """
        Parameters
        ----------
        p : Path
            Path to .sql file
        """

        f.set_self(vars())

    def get_sql(self, **kw):
        if self.sql is None:
            return f.sql_from_file(p=self.p)
        else:
            return self.sql


class ACMotorInspections(FileQuery):
    """
    Query to manage 17H019 AC Motor inspections

    Examples
    --------
    >>> query = qr.ACMotorInspections(use_cached_df=True)
    """

    def __init__(self, **kw):
        p = f.get_sql_filepath('ac_motor_inspections')
        super().__init__(p=p, **kw)

        # NOTE not dry, this type of thing is defined for avail and this query
        # would be better to make a func add these when style piped in first place
        sort_cols = ['unit', 'fc_number_next']
        self.stylemap_cols |= {col: dict(
            cols=sort_cols,
            func=st.pipe_highlight_alternating,
            da=dict(
                subset=sort_cols,
                color='maroon',
                theme=self.theme)) for col in sort_cols}

    def save_excel(self, open_=False):
        p = cf.desktop / f'AC Motor Inspections {dt.now().date():%Y-%m-%d}.xlsx'
        style = st.default_style(self.df) \
            .pipe(self.update_style)

        style.to_excel(p, index=False, freeze_panes=(1, 0))

        if open_:
            from smseventlog.utils import fileops as fl
            fl.open_folder(p)

    def show_required_notifications(self) -> None:
        """Print fc_number with list of units required to be scheduled"""
        df = self.get_df() \
            .sort_values('fc_number_next') \
            .query('action_reqd == True')

        for fc_number in df.fc_number_next.unique():
            units = df \
                .query('fc_number_next == @fc_number') \
                .sort_values('unit') \
                .unit

            s = ','.join(units)
            print(fc_number)
            print(s, '\n')

    def exclude_mask(self, df: pd.DataFrame) -> pd.Series:
        """Quick mask to show which ac SNs can be excluded from campaign"""
        return ~df.cur_sn.str.extract(r'^EE(\d{4})', expand=False) \
            .fillna(1108).astype(int).between(1108, 1805)

    def process_df(self, df):

        import re
        import string
        chars = string.ascii_lowercase[1:]

        def get_next_fc(fc_number):
            """
            Get current letter suffix of fc
            Replace with next letter eg 17H019-2b > 17H019-2c
            """
            expr = r'(?<=[-])*[a-z]'

            try:
                letter = re.search(expr, fc_number)[0]
                next_idx = chars.index(letter) + 1
                next_char = chars[next_idx]
                return re.sub(expr, next_char, fc_number)
            except Exception as e:
                return f'{fc_number}b'

        df = df \
            .pipe(f.lower_cols) \
            .rename(columns=dict(floc='side')) \
            .assign(
                side=lambda x: x.side.str[-2:],
                date_insp=lambda x: x.date_insp.dt.date,
                hrs_since_last_insp=lambda x: np.where(
                    x.comp_smr_cur >= x.comp_smr_at_insp,
                    x.comp_smr_cur - x.comp_smr_at_insp,
                    x.comp_smr_cur).astype(int),
                hrs_till_next_insp=lambda x: (3000 - x.hrs_since_last_insp).astype(int),
                date_next_insp=lambda x: (
                    pd.Timestamp.now() + pd.to_timedelta(x.hrs_till_next_insp / 20, unit='day')).dt.date,
                overdue=lambda x: x.hrs_till_next_insp < 0,
                fc_number_next=lambda x: x.fc_complete.apply(get_next_fc),
                scheduled=lambda x: x.fc_number_next == x.last_sched_fc,
                action_reqd=lambda x: (~x.scheduled) & (x.hrs_till_next_insp <= 1000)) \
            .pipe(f.convert_int64, all_numeric=True) \
            .pipe(pu.parse_datecols)

        self.formats |= f.format_int64(df)

        # set excluded SN values to NA
        # na_cols = df.columns[df.columns.get_loc('hrs_till_next_insp'):]
        # idx_na = df[self.exclude_mask(df) == True].index  # noqa
        # df.loc[idx_na, na_cols] = pd.NA

        return df

    def update_style(self, style, **kw):
        # exclude null values from subset
        not_null = lambda cols: self.subset_notnull(style, cols)

        return style \
            .apply(
                st.background_grad_center,
                subset=not_null('hrs_till_next_insp'),
                cmap=self.cmap.reversed(),
                center=1000,
                vmax=3000) \
            .apply(
                st.highlight_multiple_vals,
                subset=not_null(['overdue', 'action_reqd']),
                m={True: 'bad', False: 'goodgreen'},
                convert=True) \
            .apply(
                st.highlight_multiple_vals,
                subset=not_null('scheduled'),
                m={True: 'goodgreen'},
                convert=True,
                none_inherit=False) \
            .apply(
                st.highlight_alternating,
                subset=not_null(['unit', 'fc_number_next']), theme=self.theme, color='maroon')
        # .apply(
        #     st.highlight_val,
        #     axis=None,
        #     subset='cur_sn',
        #     t_color='black',
        #     target_col='cur_sn',
        #     masks=dict(
        #         lightyellow=self.exclude_mask(style.data),
        #         lightred=style.data.cur_sn.astype(str).str.len() < 8),
        #     theme=self.theme)


class Parts(QueryBase):
    """All part numbers in db
    ~ 115,300 rows
    - NOTE this is very static, parts list hasn't been updated since 2018 or so
    """

    def __init__(self, **kw):
        super().__init__(**kw)

        a, b = Tables('Parts', 'EquipType')
        cols = [a.star, b.ModelBase]
        cols = [b.ModelBase, a.Model, a.PartNo, a.PartName, a.PartNameAlt]

        q = Query.from_(a) \
            .select(*cols) \
            .left_join(b).on_field('Model')

        f.set_self(vars())

    def process_criterion(self):
        """Search PartName or PartNameAlt with same filter"""
        t = T('Parts')
        fltr = self.fltr

        ct = fltr.get_criterion(field='PartName')
        if not ct is None:
            fltr.criterion[ct] |= t.PartNameAlt.like(fltr.criterion[ct].right)
