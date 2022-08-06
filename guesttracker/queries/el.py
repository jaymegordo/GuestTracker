import operator as op
from datetime import datetime as dt
from datetime import timedelta as delta
from typing import *

import pandas as pd
import pypika as pk
from dateutil.relativedelta import relativedelta
from pypika import Case
from pypika import CustomFunction as cfn
from pypika import MSSQLQuery as Query
from pypika import Order
from pypika import Table as T
from pypika import functions as fn
from pypika.terms import PseudoColumn

from smseventlog import functions as f
from smseventlog import getlog
from smseventlog import styles as st
from smseventlog.queries import QueryBase

if TYPE_CHECKING:
    from pandas.io.formats.style import Styler

log = getlog(__name__)


class EventLogBase(QueryBase):
    def __init__(self, da=None, **kw):
        super().__init__(da=da, **kw)
        a, b, c, d = self.select_table, T('UnitID'), T('UserSettings'), T('EquipType')
        date_col = 'DateAdded'

        q = Query.from_(a) \
            .where(a.deleted == 0) \
            .left_join(b).on_field('Unit') \
            .left_join(c).on(a.CreatedBy == c.UserName) \
            .left_join(d).on(b.Model == d.Model) \

        f.set_self(vars())

        self.default_dtypes |= \
            f.dtypes_dict('Int64', ['SMR', 'Unit SMR', 'Comp SMR', 'Part SMR', 'Pics']) | \
            f.dtypes_dict('bool', ['Comp CO'])

    def set_base_filter(self, **kw):
        self.set_minesite()
        self.set_usergroup(**kw)

    def set_default_filter(self, **kw):
        self.set_base_filter(**kw)
        self.set_allopen(**kw)

    def set_usergroup(self, usergroup: str = None, **kw):
        """
        NOTE don't think this is used anymore, usergroup set with TableWidget.persistent_filters
        Filter to only rows created by member of specific usergroup (eg cummins, sms)"""
        if usergroup is None:
            return

        self.fltr.add(field='UserGroup', val=usergroup, table='UserSettings')


class EventLog(EventLogBase):
    def __init__(self, **kw):
        super().__init__(**kw)
        a, b = self.a, self.b

        cols = [a.UID, a.PassoverSort, a.StatusEvent, b.MineSite, a.Unit, a.Title, a.Description, a.FailureCause,
                a.DateAdded, a.DateCompleted, a.IssueCategory, a.SubCategory, a.Cause, a.CreatedBy,
                a.TimeCalled, a.StatusTSI]

        q = self.q \
            .orderby(a.DateAdded, a.Unit)

        f.set_self(vars())

    def set_allopen(self, **kw):
        a = self.a
        ct = ((a.StatusEvent != 'complete') | (a.PassoverSort.like('x')))
        self.fltr.add(ct=ct)


class WorkOrders(EventLogBase):
    def __init__(self, **kw):
        super().__init__(**kw)
        a, b = self.a, self.b

        cols = [a.UID, a.StatusWO, a.WarrantyYN, a.WorkOrder, a.Seg, a.SuncorWO, a.SuncorPO, b.MineSite, b.Model, a.Unit, b.Serial, a.Title, a.PartNumber, a.OrderParts, a.SMR, a.DateAdded, a.DateCompleted, a.CreatedBy, a.ComponentCO, a.Pictures, a.WOComments]  # noqa

        q = self.q \
            .orderby(a.DateAdded, a.Unit)

        f.set_self(vars())

    def set_allopen(self, **kw):
        a = self.a
        ct = ((a.StatusWO.notin(('closed', 'cancelled'))) & (a.StatusWO.notnull()))
        self.fltr.add(ct=ct)


class TSI(EventLogBase):
    def __init__(self, **kw):
        super().__init__(**kw)
        a, b = self.a, self.b

        cols = [a.UID, a.StatusTSI, a.DateAdded, a.DateTSISubmission, a.TSINumber, a.WorkOrder, b.MineSite,
        b.Model, a.Unit, b.Serial, a.Title, a.SMR, a.ComponentSMR, a.TSIPartName, a.PartNumber, a.SNRemoved, a.FailureCause, a.TSIAuthor, a.Pictures, a.KAPhenomenon, a.KAComponentGroup, a.TSIDetails]  # noqa

        q = self.q \
            .orderby(a.DateAdded, a.Unit)

        f.set_self(vars())

    def set_allopen(self, **kw):
        self.fltr.add(field='StatusTSI', val='closed', opr=op.ne)

    def set_fltr(self):
        super().set_fltr()
        self.fltr.add(field='StatusTSI', term='notnull')


class TSIReport(TSI):
    def __init__(self, d_rng: Tuple[dt, dt], minesite: str):
        super().__init__()
        self.use_cached_df = True

        a, b = pk.Tables('EventLog', 'UnitID')

        date = a.DateAdded.as_('Failure Date')
        cols = [date, a.TSINumber, a.Unit, b.Model, a.Title, a.SMR, a.ComponentSMR,
                a.PartNumber, a.FailureCause]

        q = Query.from_(a) \
            .select(*cols) \
            .left_join(b).on_field('Unit') \
            .where(a.deleted == 0) \
            .where(a.StatusTSI == 'Closed') \
            .where(a.TSINumber.notnull()) \
            .where(a.DateTSISubmission.between(*d_rng)) \
            .where(b.MineSite == minesite) \
            .where(~a.Title.like('fc %')) \
            .orderby(a.DateAdded, a.Unit)

        f.set_self(vars())

    @classmethod
    def from_date(cls, d_lower: dt, minesite: str = 'FortHills') -> 'TSIReport':
        from smseventlog.queries import first_last_month
        d_rng = first_last_month(d_lower)
        return cls(d_rng=d_rng, minesite=minesite)

    def exec_summary(self) -> dict:
        """Exec summary for TSI Report, count of TSIs submitted"""
        return dict(TSI={'TSIs Submitted': str(self.df.shape[0])})

    def update_style(self, style: 'Styler', **kw) -> 'Styler':
        style.set_table_attributes('class="pagebreak_table" style="font-size: 10px;"')

        return style


class TSIHistoryRolling(TSI):
    def __init__(self, d_upper: dt, minesite: str = 'FortHills'):
        super().__init__()
        a, b = self.a, self.b

        # make full year range
        d_lower = d_upper + relativedelta(years=-1) + delta(days=1)

        _year_month = cfn('FORMAT', ['date', 'format'])  # year_month(a.DateAdded, 'yyyy-MM')
        year_month = _year_month(a.DateTSISubmission, 'yyyy-MM')
        cols = [year_month.as_('period'), fn.Count(pk.terms.Star()).as_('num')]

        q = Query.from_(a) \
            .select(*cols) \
            .left_join(b).on_field('Unit') \
            .where(a.deleted == 0) \
            .where(a.StatusTSI == 'Closed') \
            .where(b.MineSite == minesite) \
            .where(~a.Title.like('fc %')) \
            .where(a.DateTSISubmission.between(d_lower, d_upper)) \
            .groupby(year_month)

        f.set_self(vars())


class UnitInfo(QueryBase):
    def __init__(self, **kw):
        super().__init__(**kw)
        a = self.select_table
        isNumeric = cfn('ISNUMERIC', ['val'])
        left = cfn('LEFT', ['val', 'num'])

        c, d, e = pk.Tables('UnitSMR', 'EquipType', 'viewPayload')

        days = fn.DateDiff(PseudoColumn('day'), a.DeliveryDate, fn.CurTimestamp())
        remaining = Case().when(days <= 365, 365 - days).else_(0).as_('Remaining')
        remaining2 = Case().when(days <= 365 * 2, 365 * 2 - days).else_(0)

        ge_remaining = Case().when(isNumeric(left(a.Model, 1)) == 1, remaining2).else_(None).as_('GE_Remaining')

        b = c.select(c.Unit, fn.Max(c.SMR).as_('CurrentSMR'), fn.Max(c.DateSMR).as_('DateSMR')).groupby(c.Unit).as_('b')

        cols = [a.MineSite, a.Customer, d.EquipClass, a.Model, a.Serial, a.Unit,
                b.CurrentSMR, b.DateSMR, a.DeliveryDate, remaining, ge_remaining, e.TargetPayload, a.Notes]

        q = Query.from_(a) \
            .left_join(b).on_field('Unit') \
            .left_join(d).on_field('Model') \
            .left_join(e).on_field('Unit') \
            .where(a.deleted == 0) \
            .orderby(a.MineSite, a.Model, a.Unit)

        f.set_self(vars())

        # NOTE lots of duplication with this pattern btwn avail/ac inspect/units/comp co
        # can't remember how everything works and don't want to dig into it
        self.stylemap_cols |= {'Model': dict(
            cols=['Model'],
            func=st.pipe_highlight_alternating,
            da=dict(
                subset=['Model'],
                color='maroon',
                theme=self.theme))}

    def set_default_filter(self, **kw):
        self.fltr.add(vals=dict(MineSite=self.minesite))

    def update_style(self, style, **kw):
        # only using for highlight_alternating Models
        color = 'navyblue' if self.theme == 'light' else 'maroon'

        return style \
            .apply(st.highlight_alternating, subset=['Model'], theme=self.theme, color=color)


class EmailList(QueryBase):
    def __init__(self, **kw):
        """Full table for app display/editing, NOT single list for emailing"""
        super().__init__(**kw)
        a = self.select_table
        cols = [a.UserGroup, a.MineSite, a.Email, a.Passover, a.WORequest, a.FCCancelled, a.PicsDLS,
                a.PRP, a.FCSummary, a.TSI, a.RAMP, a.Service, a.Parts, a.AvailDaily, a.AvailReports,
                a.FleetReport, a.SMRReport]

        q = Query.from_(a) \
            .orderby(a.UserGroup, a.MineSite, a.Email)

        f.set_self(vars())

    def set_default_filter(self, usergroup=None, **kw):
        self.fltr.add(vals=dict(MineSite=self.minesite))

        if usergroup is None:
            usergroup = self.parent.u.usergroup

        self.fltr.add(field='UserGroup', val=usergroup)


class EmailListShort(EmailList):
    def __init__(self, col_name: str, minesite: str, usergroup: str = 'SMS', **kw):
        """Just the list we actually want to email

        Parameters
        ---
        name : str,
            column name to filter for 'x'
        minesite : str
        usergroup : str, default SMS

        Examples
        ---
        >>> email_list = EmailListShort(col_name='Passover', minesite='FortHills', usergroup='SMS').emails
        >>> ['johnny@smsequip.com', 'timmy@cummins.com']
        """
        super().__init__(**kw)
        a = self.a
        cols = [a.Email]

        q = Query.from_(a)

        f.set_self(vars())

    def set_default_filter(self, **kw):
        # Convert view headers to db headers before query
        col_name = f.convert_header(title=self.title, header=self.col_name)
        self.fltr.add(vals={col_name: 'x'})

        super().set_default_filter(usergroup=self.usergroup, **kw)

    @property
    def emails(self) -> List[str]:
        """Return the actual list of emails"""
        self.set_default_filter()  # calling manually instead of passing default=True to be more explicit here
        df = self.get_df(prnt=True)
        try:
            return list(df.Email)
        except Exception as e:
            log.warning('Couldn\'t get email list from database.')
            return []


class UserSettings(QueryBase):
    def __init__(self, parent=None, **kw):
        super().__init__(parent=parent, **kw)
        a = self.select_table
        cols = [a.UserName, a.Email, a.LastLogin, a.Ver, a.Domain, a.UserGroup,
                a.MineSite, a.odbc_driver, a.install_dir]
        q = Query.from_(a) \
            .orderby(a.LastLogin, order=Order.desc)

        # NOTE SQL Server driver doesn't auto parse datetime cols
        self.default_dtypes |= {'LastLogin': 'datetime64[ns]'}

        f.set_self(vars())

    def email_old_versions(self, min_ver: str, df: pd.DataFrame = None, last_login: dt = None) -> str:
        """send email to users withoutdated versions"""
        from pkg_resources import parse_version

        def _ver(x):
            try:
                return parse_version(x)
            except:
                return parse_version('0.0.0')

        if df is None:
            df = self.get_df()

        last_login = last_login or dt.now() + delta(days=-365)

        lst = df \
            .assign(version=lambda x: x.Version.apply(_ver)) \
            .fillna(dict(Email='')) \
            .pipe(lambda df: df[
                (df.Email.notnull()) &
                (df.LastLogin > last_login) &
                ~(df.Email.str.lower().str.contains('suncor')) &
                (df.version < parse_version(min_ver))]) \
            .Email.tolist()

        return '; '.join(sorted(lst))
