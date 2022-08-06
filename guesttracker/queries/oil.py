
import json

import pandas as pd
from pypika import MSSQLQuery as Query
from pypika import Order
from pypika import Table as T
from pypika.analytics import RowNumber

from smseventlog import config as cf
from smseventlog import delta, dt
from smseventlog import functions as f
from smseventlog import getlog
from smseventlog import styles as st
from smseventlog.queries import QueryBase

log = getlog(__name__)


class OilSamples(QueryBase):
    def __init__(self, **kw):
        super().__init__(**kw)
        limit_top = None

        self.default_dtypes |= f.dtypes_dict('Int64', ['unit_smr', 'component_smr'])

        a, b, c = self.select_table, T('UnitId'), T('EquipType')
        # cols = [a.star]
        cols = [a.unit, a.component_id, a.modifier, a.sample_date, a.unit_smr,
                a.oil_changed, a.sample_rank, a.test_results, a.test_flags]

        q = Query.from_(a) \
            .left_join(b).on_field('Unit') \
            .left_join(c).on(b.Model == c.Model) \
            .orderby(a.unit, a.component_id, a.modifier) \
            .orderby(a.sample_date, order=Order.desc)

        f.set_self(vars())

    def set_default_filter(self, **kw):
        self.set_base_filter(**kw)
        self.set_allopen(**kw)

    def set_base_filter(self, **kw):
        # NOTE not sure if need this now with refresh dialog?
        self.set_minesite()

    def set_allopen(self, **kw):
        a = self.a

        self.add_fltr_args([
            dict(vals=dict(process_date=dt.now() + delta(days=-6))),
            # dict(vals=dict(component_id=component))
        ])

    def limit_top_component(self, n: int = 10):
        """Limit query to top n records per component/modifier"""
        self.limit_top = n

    def wrapper_query(self, q):
        """Call to wrap query before final call, keeps refreshtable args with base query"""
        if self.limit_top is None:
            return q
        else:
            a, b = self.a, self.b
            q._orderbys = []  # cant have orderbys in subquery

            # join on row_number ~5x faster
            t1 = Query().from_(a).as_('t1') \
                .select(
                    a.hist_no,
                    (RowNumber()
                     .over(a.unit, a.component_id, a.modifier)
                     .orderby(a.sample_date, order=Order.desc)).as_('rn'))

            # isin (1,2,3,4,5) way faster than <=5 for some reason
            return q \
                .right_join(t1) \
                .on(
                    (a.hist_no == t1.hist_no) &
                    (t1.rn.isin(list(range(1, self.limit_top + 1)))))

    def process_df(self, df):
        """Expand nested dicts of samples/flags to full columns"""

        # drop row number col
        if 'rn' in df.columns:
            df = df.drop(columns=['rn'])

        def expand_dict(df, col, suff):
            return df.join(
                pd.DataFrame(
                    df.pop(col)
                    .apply(json.loads).tolist()).add_suffix(suff))

        # save full df with flags for styling
        df = df \
            .pipe(expand_dict, col='test_results', suff='') \
            .pipe(expand_dict, col='test_flags', suff='_fg')

        # set iso/opc cols to Int64
        suffix = '_fg'
        int_cols = [col for col in df.columns if 'iso_' in col and not 'count' in col]
        opc_cols = [col for col in df.columns if 'opc_' in col]
        int_cols.extend(opc_cols)
        int_cols.extend(['cutting', 'sliding', 'fatigue', 'non_metal', 'fibers'])
        int_cols = [col for col in int_cols if not suffix in col]

        self.default_dtypes.update(
            **f.dtypes_dict('Int64', int_cols))

        # remove spaces between \ for iso and opc cols
        for col in ('opc', 'iso_count'):
            if col in df.columns:
                df[col] = df[col].str.replace(' ', '')

        flagged_cols = [col for col in df.columns if suffix in col]
        self.flagged_cols = flagged_cols

        self.df_flags = df.copy()
        return df.drop(columns=flagged_cols) \
            .replace('', pd.NA)

    def set_default_args(self, unit=None, component=None):
        self.add_fltr_args([
            dict(vals=dict(unit=unit)),
            dict(vals=dict(component_id=component))])

    def style_sample_rank(self, style, title_cols: bool = False, **kw):
        rank_subset = f.lower_cols(['sample_rank'], title=title_cols)[0]

        if rank_subset in style.data.columns:
            return style \
                .background_gradient(cmap=self.cmap, subset=rank_subset, axis=None, vmin=0, vmax=10.0)
        else:
            return style

    def style_flags(self, style, title_cols: bool = False, **kw):
        """Style flags red/orange/yellow and overall sample_rank
        - Can handle styling only pt1 or pt2 or oil samples df
        """
        if not hasattr(self, 'df_flags'):
            raise AttributeError('df_flags not set!')

        flagged_cols = self.flagged_cols
        df_flags = self.df_flags
        suffix = '_fg'

        if title_cols:
            flagged_cols = f.lower_cols(flagged_cols, title=True)
            df_flags = df_flags.pipe(f.lower_cols, title=True)

            df_flags = df_flags[[c for c in style.columns] + flagged_cols]
            # .pipe(lambda df: df.rename(columns={c: c.replace(' Fg', suffix) for c in df.columns}))
            suffix = ' Fg'

        # join flagged cols back to set flag colors on index col - cant use if sample_date is index!
        flagged_cols_clean = [c for c in flagged_cols if c.replace(suffix, '') in style.data.columns]

        flagged_cols_dirty = list(set(flagged_cols) - set(flagged_cols_clean))

        # print('df_flags', df_flags.columns.tolist())
        # print('flagged', flagged_cols)
        # print('dirty', flagged_cols_dirty)
        # print('clean', flagged_cols_clean)

        style.data = style.data.join(df_flags[flagged_cols_clean])
        style.columns = df_flags.drop(columns=flagged_cols_dirty).columns

        # print('style.data.columns', style.data.columns.tolist())
        # print('style.columns', style.columns.tolist())

        c = cf.config['color']['bg']
        m_color = dict(
            S=(c['lightred'], 'white'),
            U=(c['lightorange'], 'black'),
            R=(c['lightyellow'], 'black'))

        # need normal and _f cols to highlight flagged cols
        # flagged_cols = [col for col in style.data.columns if suffix in col]
        subset = flagged_cols_clean.copy()
        subset.extend([c.replace(suffix, '') for c in subset])
        # print('subset', subset)

        return style \
            .pipe(self.style_sample_rank, title_cols=title_cols, **kw) \
            .apply(
                st.highlight_flags,
                axis=None,
                subset=subset,
                m=m_color,
                theme=self.theme,
                none_inherit=False,
                convert=False,
                suffix=suffix) \
            .hide(subset=flagged_cols_clean, axis='columns')

    def update_style(self, style, **kw):
        # style = self.df_flags.style
        style.set_table_attributes('class="pagebreak_table"')
        color = 'navyblue' if self.theme == 'light' else 'maroon'

        return style \
            .pipe(self.style_flags) \
            .apply(st.highlight_alternating, subset=['unit'], theme=self.theme, color=color)


class OilSamplesReport(OilSamples):
    """Query for oil sample in failure reports"""

    def __init__(self, unit, component, modifier=None, n=10, d_lower=None, d_upper=None, **kw):
        super().__init__(**kw)
        a, b = self.a, self.b
        cols = [a.unit, a.component_id, a.modifier, a.sample_date, a.unit_smr,
                a.oil_changed, a.sample_rank, a.test_results, a.test_flags]

        q = Query \
            .from_(a) \
            .where(
                (a.unit == unit) &
                (a.component_id == component)) \
            .orderby(a.sample_date, order=Order.desc)

        if d_lower is None:
            q = q.top(n)

            if not d_upper is None:
                q = q.where(a.sample_date <= d_upper)

        else:
            q = q.where(a.sample_date >= d_lower)

        if not modifier is None:
            q = q.where(a.modifier == modifier)

        f.set_self(vars())

    @classmethod
    def example(
            cls,
            unit: str = 'F331',
            component: str = 'HYDRAULIC',
            modifier: str = None) -> 'OilSamplesReport':
        return cls(unit=unit, component=component, modifier=modifier)

    def df_split(self, part: int) -> pd.DataFrame:
        """Oil Report dfs too large for one row, need to split in 2

        Parameters
        ----------
        part : int, optional
            which table to return, default 1

        Returns
        -------
        pd.DataFrame
        """
        df = self.get_df(cached=True) \
            .set_index('sample_date') \
            .rename_axis('Sample Date') \
            .assign(
                oil_changed=lambda x: x.oil_changed.astype(str))

        split = (df.shape[1] // 2) + 6
        m_tbls = dict(
            oil1=df.iloc[:, :split],
            oil2=df.iloc[:, split:])

        df = m_tbls.get(f'oil{part}')

        # if part == 1:
        #     df = df.drop(columns=['unit', 'component_id', 'modifier'])

        return df \
            .reset_index(drop=False)

    def style_title_cols(self, style, **kw):

        df = style.data
        cols = df.columns.tolist()
        # df2 = df.copy()
        df.columns = f.lower_cols(cols, title=True)

        style.columns = df.columns
        # print(style.columns)
        # print(style.data.columns)

        style = style \
            .pipe(self.update_style, title_cols=True)

        if 'Unit' in style.data.columns:
            # print(style.columns)
            # print(style.data.columns)
            style = style.pipe(st.extend_hidden_cols, subset=['Unit', 'Component Id', 'Modifier'])

        return style

    def update_style(self, style, **kw):
        m_fmt = {k: '{:.1f}' for k in style.data.select_dtypes(float).columns} | \
            {k: '{:%Y-%m-%d}' for k in ('sample_date', 'Sample Date')}

        return style \
            .pipe(self.style_flags, **kw) \
            .pipe(st.format_dict, m_fmt) \
            .set_table_attributes('style="font-size: 8px;"')


class OilSamplesRecent(OilSamples):
    def __init__(self, recent_days=-120, da=None):
        super().__init__(da=da)
        a, b = self.a, self.b

        # subquery for ordering with row_number
        c = Query.from_(a).select(
            a.star,
            (RowNumber()
                .over(a.unit, a.component_id, a.modifier)
                .orderby(a.sample_date, order=Order.desc)).as_('rn')) \
            .left_join(b).on_field('Unit') \
            .where(a.sample_date >= dt.now() + delta(days=recent_days)) \
            .as_('sq0')

        cols = [c.star]
        sq0 = c
        f.set_self(vars())

    def get_query(self):
        c = self.sq0
        return Query.from_(c) \
            .where(c.rn == 1) \
            .orderby(c.unit, c.component_id, c.modifier)

    def process_df(self, df):
        return super().process_df(df=df) \
            .drop(columns=['rn'])


class OilReportSpindle(OilSamplesRecent):
    def __init__(self, da=None, minesite='FortHills', **kw):
        super().__init__(da=da, **kw)

    def set_default_filter(self):
        self.set_default_args()

    def set_default_args(self):
        self.add_fltr_args([
            dict(vals=dict(component='spindle'), table=self.a),
            dict(vals=dict(minesite=self.minesite), table=self.b),
            dict(vals=dict(model='980%'), table=self.b)],
            subquery=True)

    def process_df(self, df):
        from smseventlog.data import oilsamples as oil

        return super().process_df(df=df) \
            .pipe(oil.flatten_test_results, keep_cols=['visc40', 'visc100']) \
            .drop(columns=['oilChanged', 'testResults', 'results', 'recommendations', 'comments'])
