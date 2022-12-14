import sys
from pathlib import Path
from typing import *

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from guesttracker import charts as ch
from guesttracker import config as cf
from guesttracker import dbtransaction as dbt
from guesttracker import delta, dt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker import queries as qr
from guesttracker import styles as st
from guesttracker.database import db
from guesttracker.eventfolders import EventFolder
from guesttracker.utils import email as em
from guesttracker.utils.download import Gtk, Kaleido

if TYPE_CHECKING:
    from pandas.io.formats.style import Styler

    from guesttracker.queries import QueryBase

p_reports = cf.p_res / 'reports'
log = getlog(__name__)

# TODO auto email w/ email lists


class Report(object):
    def __init__(
            self,
            d: dt = None,
            d_rng: Tuple[dt] = None,
            minesite: str = None,
            mw=None,
            rep_type: str = 'pdf',
            **kw):
        # dict of {df_name: {func: func_definition, da: **da, df=None}}
        dfs, charts, sections, exec_summary, style_funcs = {}, {}, {}, {}, {}
        signatures = []
        self.html_template = 'report_template.html'
        dfs_loaded = False
        p_rep = None

        if d is None:
            d = dt.now() + delta(days=-31)
        if d_rng is None:
            d_rng = qr.first_last_month(d=d)

        # make sure everything is date not datetime
        if isinstance(d_rng[0], dt):
            d_rng = (d_rng[0].date(), d_rng[1].date())

        # don't use current ytd until first monthly report end of jan
        cur_year = dt.now().year
        d = dt(cur_year, 1, 1)
        d_end_jan = qr.first_last_month(d)[1].date()

        if d_rng[1] < d_end_jan:
            d_rng_ytd = (dt(cur_year - 1, 1, 1), dt(cur_year - 1, 12, 31))
        else:
            d_rng_ytd = (dt(cur_year, 1, 1).date(), d_rng[1])

        include_items = dict(
            title_page=False,
            truck_logo=False,
            exec_summary=False,
            table_contents=False,
            signature_block=False)

        env = Environment(loader=FileSystemLoader(str(p_reports)))

        f.set_self(vars())

    @property
    def ext(self):
        return 'pdf'

    def add_items(self, items: list):
        """Add report items eg Title Page, Executive Summary

        Parameters
        ----------
        items : list (or str),
            Items to add
        """
        if not isinstance(items, list):
            items = [items]
        for item in items:
            self.include_items.update({item: True})

    def get_section(self, name):
        """TODO not working, just use report.sections['section name']"""
        for sec in self.sections.values():
            for item in sec:
                if item['name'] == name:
                    return item

    def load_sections(self, secs: Union[List[str], str, List[Dict[str, Any]], Dict[str, Any]]):
        """Instantiate all sections passed in using getattr on this module.

        Parameters
        ----------
        secs : list or single items
        - str
        - dict
        """
        for sec in f.as_list(secs):
            # allow passing args with dict
            if not isinstance(sec, dict):
                sec = dict(name=sec)

            getattr(sys.modules[__name__], sec['name'])(report=self, **sec)

    def load_all_dfs(self, saved=False):
        # call each df's function with its args and assign to df
        print('\n\nLoading dfs:')
        for name in self.dfs:
            self.load_df(name=name, saved=saved)

        self.dfs_loaded = True
        return self

    def load_df(self, name, saved=False):
        """Load df from either function defn or query obj"""
        print(f'\t{name}')
        m = self.dfs[name]
        func, query, da, name = m['func'], m['query'], m['da'], m['name']

        if saved:
            m['df'] = pd.read_csv(p_reports / f'saved/{name}.csv')

        elif not func is None:
            m['df'] = func(**da)

        elif not query is None:
            m['df'] = query.get_df(**da)

    def load_section_data(self):
        for sec in self.sections.values():
            sec.load_subsection_data()

    def print_dfs(self):
        for i, k in enumerate(self.dfs):
            m = self.dfs[k]
            rows = 0 if m['df'] is None else len(m['df'])
            val = m['query'] if not m['query'] is None else m['func']
            func = ' '.join(str(val).split(' ')[:2]).replace('<function ', '')
            print('{}: {}\n\t{}\n\t{}\n\t{}'.format(i, k, rows, func, m['da']))

    def save_dfs(self):
        for m in self.dfs.values():
            df, name = m['df'], m['name']
            df.to_csv(p_reports / f'saved/{name}.csv', index=False)

    def style_df(
            self,
            name: str = None,
            df: pd.DataFrame = None,
            query: qr.QueryBase = None,
            outlook: bool = False,
            style_func: Callable = None) -> 'Styler':

        if not name is None:
            df = self.get_df(name=name)
            query = self.get_query(name=name)
            style_func = self.style_funcs.get(name, None)

        style = st.default_style(df, outlook=outlook)

        # outlook can't use css nth-child selectors, have to do manually
        if outlook:
            style = style.pipe(st.alternating_rows_outlook)

        # general number formats
        # formats = {'Int64': '{:,}', 'int64': '{:,}', 'datetime64[ns]': '{:%Y-%m-%d}'}
        # m_fmt = st.format_dtype(df=df, formats=formats)
        m_fmt = {}

        if not query is None:
            m_fmt |= query.formats

            if hasattr(query, 'update_style') and style_func is None:
                style = query.update_style(style, outlook=outlook)

        if not style_func is None:
            style = style_func(style)

        return style.pipe(st.format_dict, m_fmt)

    def render_dfs(self):
        # convert all dataframes to html for rendering in html template
        print('\n\nRendering dfs:')
        for m in self.dfs.values():
            if m['display']:
                print(f"\t{m['name']}")
                style = self.style_df(name=m['name'])
                m['df_html'] = style.hide(axis='index').to_html()

    def check_kaleido(self) -> bool:
        """Check if Kaleido binaries exist. If not, download

        Returns
        -------
        bool
            If Kaleido binaries exist, or were downloaded successfully
        """
        if cf.is_win:
            if not Kaleido(mw=self.mw).check():
                return False  # stop creating report

        return True

    def check_gtk(self) -> bool:
        """Check if gtk (cairo) libs exist. If not, download

        Returns
        -------
        bool
            If gtk libs exist or were downloaded successfully
        """
        if cf.is_win:
            if not Gtk(mw=self.mw).check():
                return False  # stop creating report

        return True

    def img_filepath(self, p: Path) -> str:
        """Check filepath and add file:/// for windows-pdf reports

        Parameters
        ----------
        p : Path

        Returns
        -------
        str
            path as string
        """
        if isinstance(p, Path):
            p = p.as_posix()

        return f'file:///{p}' if cf.is_win and self.rep_type == 'pdf' else str(p)

    def render_charts(self, ext: str = 'svg', scale: int = 1, p_img: Path = None):
        """Render all charts from dfs, save as svg image"""
        if not (self.check_kaleido() and self.check_gtk()):
            return  # kaleido or cairo libs not installed
        print('\n\nRendering charts:')

        # manually set executable_path for kaleido before trying to render charts
        if cf.SYS_FROZEN:
            import kaleido.scopes.base as kaleido_base
            kaleido_base.BaseScope.executable_path = lambda x: cf.kaleido_path  # need to make it a callable

        if p_img is None:
            p_img = cf.p_temp / 'images'

        if not p_img.exists():
            p_img.mkdir(parents=True)

        for m in self.charts.values():
            print(f"\t{m['name']}")
            df = self.dfs[m['name']]['df']
            fig = m['func'](df=df, **dict(title=m['title']) | m['da'])

            p = p_img / f'{m["name"]}.{ext}'
            m['path'] = p  # save so can delete later

            # need this to load images to html template in windows for some reason
            m['str_p_html'] = self.img_filepath(p)

            fig.write_image(str(p), engine='kaleido', scale=scale)

        return True

    def remove_chart_files(self):
        """Delete images saved for charts after render."""
        for m in self.charts.values():
            p = m['path']
            try:
                p.unlink()
            except:
                log.warning(f'Couldn\'t delete image at path: {p}')

    def get_all_dfs(self):
        # TODO: could probably use a filter here
        return [m['df'] for m in self.dfs.values() if not m['df'] is None]

    def get_df(self, name):
        return self.dfs[name].get('df', None)

    def get_query(self, name) -> 'QueryBase':
        """Return query obj based on df section name"""
        return self.dfs[name].get('query', None)

    def check_overwrite(self, p_base=None):
        if p_base is None:
            p_base = Path.home() / 'Desktop'

        p = p_base / f'{self.title}.{self.ext}'

        if p.exists():
            from guesttracker.gui.dialogs.dialogbase import msgbox
            msg = f'File "{p.name}" already exists. Overwrite?'
            return msgbox(msg=msg, yesno=True)

        return True

    def create_pdf(
            self,
            p_base: Path = None,
            template_vars: dict = None,
            check_overwrite: bool = False,
            write_html: bool = False,
            open_: bool = False,
            **kw):
        if not self.dfs_loaded:
            self.load_all_dfs()

        self.render_dfs()

        if self.charts:
            if not self.render_charts():
                return  # charts failed to render

        if hasattr(self, 'set_exec_summary'):
            self.set_exec_summary()

        self.load_section_data()

        template = self.env.get_template(self.html_template)

        dfs_filtered = {k: v for k, v in self.dfs.items() if v['display']}  # filter out non-display dfs

        # can pass in extra template vars but still use originals
        if template_vars is None:
            template_vars = {}

        template_vars |= dict(
            exec_summary=self.exec_summary,
            d_rng=self.d_rng,
            title=self.title,
            sections=self.sections,
            dfs=dfs_filtered,
            charts=self.charts,
            include_items=self.include_items,
            signatures=self.signatures)

        # may need to write html to file to debug issues
        html_out = template.render(template_vars)
        if write_html:
            with open('report.html', 'w+', encoding='utf-8') as file:
                file.write(html_out)

        if p_base is None:
            p_base = Path.home() / 'Desktop'
        elif not p_base.exists():
            p_base.mkdir(parents=True)

        # save pdf - NOTE can't raise dialog from worker thread
        p = p_base / f'{self.title}.pdf'
        from weasyprint import HTML
        HTML(string=html_out, base_url=str(p_reports)).write_pdf(p, stylesheets=[p_reports / 'report_style.css'])

        self.remove_chart_files()
        self.p_rep = p

        if open_:
            self.open_()

        return self

    def outlook_table(self, name):
        return self.style_df(name, outlook=True).hide(axis='index').to_html()

    def render_html(self, p_html, p_out=None):
        # for testing pre-created html
        if p_out is None:
            p_out = Path.home() / 'desktop/test.pdf'

        with open(p_html, 'r') as file:
            html_in = file.read()
        from weasyprint import HTML
        HTML(string=html_in, base_url=str(p_reports)).write_pdf(p_out, stylesheets=[p_reports / 'report_style.css'])

    @property
    def exec_summary_html(self):
        """Convert exec summary to html for email body"""
        self.set_exec_summary()
        template = self.env.get_template('exec_summary_template.html')
        template_vars = dict(
            exec_summary=self.exec_summary,
            d_rng=self.d_rng)

        return template.render(template_vars) \
            .replace('Executive Summary', '')

    @property
    def email_html(self):
        return f'{f.greeting()}{self.exec_summary_html}'

    @property
    def email_attachments(self) -> Union[Path, List[Path]]:
        """List of files to attach to report email
        - Overwrite this method to add more files than just report
        """
        return self.p_rep

    def email(self) -> None:
        """Email report with custom email list and html body"""
        msg = em.Message(subject=self.title, body=self.email_html, to_recip=self.email_list, show_=False)
        msg.add_attachments(self.email_attachments)
        msg.show()

    @property
    def email_list(self) -> List[str]:
        """List of emails for sending report
        - NOTE not sure if defining this here will work for all reports (eg usergroup?)
        """
        return qr.EmailListShort(
            col_name=self.email_col_name,
            minesite=self.minesite,
            usergroup='SMS').emails

    def open_(self):
        """Open report filepath"""
        from guesttracker.utils.fileops import open_folder
        open_folder(self.p_rep)

    def update_from_report(self, report=None) -> None:
        """Update self data from initialized report

        Parameters
        ----------
        report : Report, optional
            report to update from, default None
        """
        if not report is None:
            items = ('sections', 'dfs', 'style_funcs', 'charts')

            for item in items:
                getattr(self, item).update(getattr(report, item))


class FleetMonthlyReport(Report):
    def __init__(self, d=None, minesite='FortHills', secs=None, items=None):
        super().__init__(d=d)

        period_type = 'month'
        period = self.d_rng[0].strftime('%Y-%m')
        name = period
        title = f'{minesite} Fleet Monthly Report - {period}'
        email_col_name = 'FleetReport'
        f.set_self(vars())

        if not secs:
            secs = ['UnitSMR', 'AvailBase', 'Components', 'FCs', 'TSIs', 'FrameCracks']

        if not items:
            items = ['title_page', 'truck_logo', 'exec_summary', 'table_contents']

        self.load_sections(secs)
        self.add_items(items)

    @classmethod
    def example(cls):
        return cls(minesite='FortHills', secs=['TSIs'])

    @classmethod
    def example_rainyriver(cls):
        return cls(minesite='RainyRiver', secs=['FCs'], items=['title_page', 'exec_summary'])

    def set_exec_summary(self):
        gq = self.get_query
        ex = self.exec_summary
        sections = self.sections

        if 'Availability' in sections:
            self.sections['Availability'].set_exec_summary(ex=ex)  # avail sets its own exec_summary

        if 'Components' in sections:
            ex['Components'] = gq('Component Changeouts').exec_summary()

        if 'Factory Campaigns' in sections:
            ex['Factory Campaigns'] = gq('FC Summary').exec_summary()
            ex['Factory Campaigns'].update(gq('Completed FCs').exec_summary())

        if 'TSI' in sections:
            ex['TSI'] = gq('TSIs Submitted').exec_summary()


class SMRReport(Report):
    def __init__(self, d: dt = None, minesite: str = 'FortHills'):
        super().__init__(d=d)

        signatures = ['Suncor', 'SMS']
        period = self.d_rng[0].strftime('%Y-%m')
        title = f'{minesite} Monthly SMR - {period}'
        email_col_name = 'SMRReport'
        p_csv = None
        f.set_self(vars())

        self.load_sections('UnitSMR')
        self.add_items(['title_page', 'signature_block'])

    @property
    def email_html(self):
        """Create email html body"""
        return '{}{}{}.<br><br>{}'.format(
            f.greeting(),
            'See attached SMR Report for ',
            self.period,
            'Please sign and return as required.')

    @property
    def email_attachments(self) -> Union[Path, List[Path]]:
        return [self.p_rep, self.p_csv]

    def create_pdf(self, p_base: Path = None, csv: bool = True, **kw):
        if not self.dfs_loaded:
            self.load_all_dfs()

        if csv:
            self.save_csv(p_base=p_base)

        return super().create_pdf(p_base=p_base, **kw)

    def save_csv(self, p_base=None):
        if p_base is None:
            p_base = cf.desktop

        p = p_base / f'{self.title}.csv'

        df = self.get_df('SMR Hours Operated')
        df.to_csv(p)
        self.p_csv = p


class AvailabilityReport(Report):
    def __init__(self, name: str, period_type='week', minesite='FortHills', **kw):
        """Create availability report pdf

        Parameters
        ----------
        name : str
            Period name, week = '2021-34', month = '2021-02'
        period_type : str, optional
            default 'week'
        minesite : str, optional
            default 'FortHills'
        """
        super().__init__()
        df = qr.df_period(freq=period_type, n_years=5)
        d_rng = df.loc[name, 'd_rng']
        name_title = df.loc[name, 'name_title']
        email_col_name = 'AvailReports'

        signatures = ['Suncor Reliability', 'Suncor Maintenance', 'SMS', 'Komatsu']

        title = f'Suncor Reconciliation Report - {minesite} - {period_type.title()}ly - {name_title}'
        f.set_self(vars())  # , exclude='d_rng'

        self.load_sections('AvailStandalone')
        self.add_items(['title_page', 'exec_summary', 'table_contents'])

        if period_type == 'month':
            self.add_items(['signature_block'])

    def set_exec_summary(self):
        ex = self.exec_summary
        self.sections['Availability'].set_exec_summary(ex=ex)

    @property
    def email_list(self):
        return qr.EmailListShort(col_name='AvailReports', minesite=self.minesite, usergroup='SMS').emails

    @property
    def email_html(self):
        """Create email html with exec summary + 2 tables"""
        return '{}{}{}.<br>{}<br>{}<br><br>{}'.format(
            f.greeting(),
            'See attached report for availability ',
            self.name_title,
            self.exec_summary_html,
            self.outlook_table('Fleet Availability'),
            self.outlook_table('Summary Totals'))


class FrameCracksReport(Report):
    def __init__(self):
        super().__init__()
        self.title = 'FortHills Frame Cracks Report'
        self.load_sections('FrameCracks')
        self.add_items('title_page')


class OilSamplesReport(Report):
    def __init__(self):
        super().__init__()
        self.title = 'FortHills Spindle Oil Report'
        self.load_sections('OilSamples')
        self.add_items('title_page')


class FCReport(Report):
    def __init__(self, d=None, minesite='FortHills', **kw):
        super().__init__(d=d, **kw)

        period_type = 'month'
        period = self.d_rng[0].strftime('%Y-%m')
        title = f'{minesite} Factory Campaign Report - {period}'

        f.set_self(vars())

        items = ['title_page', 'exec_summary', 'table_contents']
        self.load_sections('FCs')
        self.add_items(items)

    @property
    def email_list(self):
        return qr.EmailListShort(col_name='FCSummary', minesite=self.minesite).emails

    @property
    def email_html(self):
        html_table = self.style_df('FC Summary', outlook=True).hide(axis='index').to_html()
        return f'{f.greeting()}See attached report for FC Summary \
        {self.period}.<br><br>{self.exec_summary_html}<br>{html_table}'

    def set_exec_summary(self):
        gq = self.get_query
        ex = self.exec_summary

        ex['Factory Campaigns'] = gq('FC Summary').exec_summary()
        ex['Factory Campaigns'].update(gq('Completed FCs').exec_summary())


class FailureReport(Report):
    def __init__(
            self,
            title: str = None,
            header_data: dict = None,
            body: str = None,
            pictures: List[Path] = None,
            e=None,
            ef: EventFolder = None,
            query_oil: qr.oil.OilSamplesRecent = None,
            rep_plm: 'PLMUnitReport' = None,
            **kw):
        super().__init__(**kw)
        self.html_template = 'failure_report.html'

        # sort pictures based on number
        if not pictures is None:
            pictures = self.sort_pics(pictures)

        if hasattr(self, 'set_pictures'):
            pictures = self.set_pictures(pictures=pictures)

        self.header_fields = [
            ('Failure Date', 'Work Order'),
            ('Customer', 'MineSite'),
            ('Author', 'TSI'),
            ('Model', 'Part Description'),
            ('Unit', 'Part No'),
            ('Unit Serial', 'Part Serial'),
            ('Unit SMR', 'Part SMR'),
        ]

        # map model fields to header_data
        header_data = self.parse_header_data(m_hdr=header_data)

        if title is None:
            title = self.create_title_model(m=header_data, e=e)

        # create header table from dict of header_data
        df_head = self.df_header(m=header_data)

        # complaint/cause/correction + event work details as paragraphs with header (H4?)

        # rep_plm is full report obj
        self.update_from_report(rep_plm)

        s = []

        if not query_oil is None:
            # query_oil is only query
            s = [dict(name='OilSamples', query=query_oil)]

        s.append(dict(name='Pictures', pictures=pictures))
        self.load_sections(s)

        # pics > need two column layout
        f.set_self(vars())

    @classmethod
    def from_model(cls, e, **kw):
        """Create report from model e, with header data from event dict + unit info"""
        header_data = dbt.model_dict(e, include_none=True)
        header_data.update(db.get_df_unit().loc[e.Unit])

        return cls(e=e, header_data=header_data, **kw)

    @classmethod
    def example(cls, uid: int = None, **kw):
        pics = []
        e = dbt.Row.example(uid=uid)

        body = dict(
            complaint='The thing keeps breaking.',
            cause='Uncertain',
            correction='Component replaced with new.',
            details=e.Description)

        # use eventfolder to get pictures
        from guesttracker import eventfolders as efl
        ef = efl.EventFolder.example(uid=uid, e=e)
        pics = ef.pics[:3]

        return cls.from_model(e=e, ef=ef, pictures=pics, body=body, **kw)

    def set_pictures(self, pictures: List[Path]) -> List[str]:
        """Make image path 'absolute' with file:///, for pdf report only, not word"""
        return [self.img_filepath(pic) for pic in pictures or []]

    def sort_pics(self, pics: list) -> List[str]:
        """Sort list of strings or path objs with ints first then strings"""

        # just convert everything to path
        pics = [Path(p) for p in pics]

        # save as dict
        # m = {p.stem if isinstance(p, Path) else p: p for p in pics}
        m = {p.stem: str(p) for p in pics}

        # use keys to sort
        pics = list(m.keys())

        # get ints
        int_pics = [p for p in pics if f.isnum(p)]

        # get strings
        str_pics = set(pics) - set(int_pics)

        out = sorted(int_pics, key=int)
        out.extend(sorted(str_pics))
        return [Path(m[item]).as_posix() for item in out]

    def create_pdf(self, p_base=None, **kw):
        if p_base is None:
            p_base = self.ef._p_event

        df_head_html = self.style_header(df=self.df_head) \
            .to_html()

        # convert back to caps, replace newlines
        body = {k.title(): v.replace('\n', '<br>') for k, v in self.body.items()}

        template_vars = dict(
            df_head=df_head_html,
            body_sections=body,
            pictures=self.pictures)

        return super().create_pdf(
            p_base=p_base,
            template_vars=template_vars,
            check_overwrite=True,
            **kw)

    def style_header(self, df):
        return self.style_df(df=df) \
            .apply(st.bold_columns, subset=['field1', 'field2'], axis=None) \
            .set_table_attributes('class="failure_report_header_table"') \
            .hide(axis='index') \
            .pipe(st.hide_headers)

    def parse_header_data(self, m_hdr):
        m = {}
        # loop header fields, get data, else check conversion_dict
        for fields in self.header_fields:
            for field in fields:
                if not field is None:
                    m[field] = m_hdr.get(field, None)  # getattr(e, field, None)

        # try again with converted headers
        m_conv = {
            'Author': 'TSIAuthor',
            'Unit SMR': 'SMR',
            'Part SMR': 'ComponentSMR',
            'Work Order': 'WorkOrder',
            'Part Description': 'TSIPartName',
            'Part No': 'PartNumber',
            'Failure Date': 'DateAdded',
            'Unit Serial': 'Serial',
            'Part Serial': 'SNRemoved', }

        for field, model_field in m_conv.items():
            m[field] = m_hdr.get(model_field, None)  # getattr(e, model_field, None)

        return m

    def create_title_model(self, m=None, e=None):
        if not e is None:
            unit, d, title = e.Unit, e.DateAdded, e.Title
        elif not m is None:
            unit, d, title = m['Unit'], m['Failure Date'], ''  # blank title
        else:
            return 'temp title'

        return f'{unit} - {d:%Y-%m-%d} - {title}'

    def df_header(self, m=None):
        if m is None:
            m = {}

        def h(val):
            return f'{val}:' if not val is None else ''

        data = [dict(
            field1=h(field1),
            field2=h(field2),
            val1=m.get(field1, None),
            val2=m.get(field2, None)) for field1, field2 in self.header_fields]

        return pd.DataFrame(data=data, columns=['field1', 'val1', 'field2', 'val2'])


class PLMUnitReport(Report):
    def __init__(self, unit: str, d_upper: dt, d_lower: dt = None, include_overloads: bool = False, **kw):
        """Create PLM report for single unit

        Parameters
        ----------
        unit : str
        d_lower : dt
        d_upper : dt
        include_overloads : bool
            include table of all overloads >120%, default False
        """
        if d_lower is None:
            d_lower = qr.first_last_month(d_upper + delta(days=-180))[0]

        d_rng = (d_lower, d_upper)

        super().__init__(d_rng=d_rng, **kw)
        title = f'PLM Report - {unit} - {d_upper:%Y-%m-%d}'

        f.set_self(vars())

        self.load_sections(dict(name='PLMUnit', include_overloads=include_overloads))

    @classmethod
    def example(cls):
        return cls(unit='F306', d_upper=dt(2020, 10, 18), d_lower=dt(2019, 10, 18))

    def create_pdf(self, **kw):
        """Need to check df first to warn if no rows."""
        sec = self.sections['PLM Analysis']
        query = sec.query
        df = query.get_df()

        if df.shape[0] == 0:
            return False  # cant create report with now rows

        return super().create_pdf(**kw)


class ComponentReport(Report):
    def __init__(self, d_rng, minesite, **da) -> None:
        super().__init__(d_rng=d_rng, minesite=minesite, **da)
        self.title = 'Component Changeout History - FH 980E'

        query = qr.ComponentCOReport(
            d_rng=d_rng,
            minesite=minesite,
            major=True,
            sort_component=True)

        query.fltr.add(vals=dict(Model='980E*'), table='UnitID')

        self.load_sections([
            dict(name='ComponentHistoryCharts', query=query),
            dict(name='Components', title='Data', query=query)])

        self.add_items(['title_page', 'exec_summary', 'table_contents'])

    @classmethod
    def example(cls):
        d_rng = (dt(2016, 1, 1), dt(2020, 12, 31))
        return cls(d_rng=d_rng, minesite='FortHills')

    def create_pdf(self, **kw):
        """Write raw data to csv after save"""
        super().create_pdf(**kw)
        p = self.p_rep.parent / 'FH Component Changeout History.csv'
        df = self.get_df('Component Changeout History')
        df.to_csv(p, index=False)

        return self

    def set_exec_summary(self):
        gq = self.get_query
        ex = self.exec_summary
        ex['Components'] = gq('Component Changeout History').exec_summary()


# REPORT SECTIONS
class Section():
    # sections only add subsections
    tab = '&emsp;'  # insert tab character in html

    def __init__(self, title, report, **kw):

        report.sections[title] = self  # add self to parent report
        sub_sections = {}
        d_rng, d_rng_ytd, minesite = report.d_rng, report.d_rng_ytd, report.minesite

        f.set_self(vars())

    def add_subsections(self, sections):
        for name in sections:
            subsec = getattr(sys.modules[__name__], name)(section=self)

    def load_subsection_data(self):
        """Load extra data (eg paragraph data) for each subsection"""
        for name, sub_sec in self.sub_sections.items():
            if not sub_sec.paragraph_func is None:
                m = sub_sec.paragraph_func
                sub_sec.paragraph = m['func'](**m['kw'])


class OilSamples(Section):
    def __init__(self, report, query: qr.oil.OilSamplesReport = None, **kw):
        super().__init__(title='Oil Samples', report=report)

        # sec = SubSection('Spindles', self) \
        #     .add_df(
        #         query=qr.OilReportSpindle(),
        #         da=dict(default=True),
        #         caption='Most recent spindle oil samples.')

        if not query is None:
            sec = SubSection('History', self) \
                .add_paragraph(
                    func=self.set_paragraph,
                    kw=dict(query=query)) \
                .add_df(
                    name='oil1',
                    func=query.df_split,
                    da=dict(part=1),
                    style_func=query.style_title_cols,
                    caption='Oil sample history pt1.') \
                .add_df(
                    name='oil2',
                    func=query.df_split,
                    da=dict(part=2),
                    style_func=query.style_title_cols,
                    caption='Oil sample history pt2.')

    def set_paragraph(self, query: qr.oil.OilSamplesReport, **kw) -> str:
        s = f'{query.unit} - {query.component}'

        if not query.modifier is None:
            s += f', {query.modifier}'

        return s


class FrameCracks(Section):
    def __init__(self, report, **kw):
        super().__init__(title='Frame Cracks', report=report)
        from guesttracker.data import framecracks as frm

        m = dict(df=frm.load_processed_excel())

        sec = SubSection('Summary', self) \
            .add_df(
                func=frm.df_smr_avg,
                da=m,
                caption='Mean SMR cracks found at specified loaction on haul truck.<br><br>\
                Rear = rear to mid torque tube<br>Mid = mid torque tube (inclusive) \
                to horse collar<br>Front = horse collar (inclusive) to front.')

        sec = SubSection('Frame Cracks Distribution', self) \
            .add_df(
                name='Frame Cracks (Monthly)',
                func=frm.df_month,
                da=m,
                display=False) \
            .add_df(
                name='Frame Cracks (SMR Range)',
                func=frm.df_smr_bin,
                da=m,
                display=False) \
            .add_chart(
                name='Frame Cracks (Monthly)',
                func=ch.chart_frame_cracks,
                caption='Frame crack type distributed by month.') \
            .add_chart(
                name='Frame Cracks (SMR Range)',
                func=ch.chart_frame_cracks,
                caption='Frame crack type distributed by Unit SMR.',
                da=dict(smr_bin=True)) \
            .force_pb = True


class UnitSMR(Section):
    def __init__(self, report, **kw):
        super().__init__(title='SMR Hours', report=report)
        d_rng = report.d_rng
        query_f300 = qr.UnitSMRMonthly(unit='F300')

        d = d_rng[0]
        month = d.month
        sec = SubSection('SMR Hours Operated', self) \
            .add_df(
                query=qr.UnitSMRReport(d=d),
                caption='SMR hours operated during the report period.')

        sec = SubSection('F300 SMR Hours Operated', self) \
            .add_df(
                func=query_f300.df_monthly,
                da=dict(max_period=d_rng[1].strftime('%Y-%m'), n_periods=12, totals=True),
                caption='F300 SMR operated, 12 month rolling.<br>*SMR is max SMR value in period.',
                style_func=query_f300.style_f300)

        f.set_self(vars())


class AvailBase(Section):
    def __init__(self, report, **kw):
        super().__init__(title='Availability', report=report)
        n = 10
        d_rng, d_rng_ytd, ms, period_type = self.d_rng, self.d_rng_ytd, self.minesite, self.report.period_type

        summary = qr.AvailSummary.from_name(name=self.report.name, period=period_type)
        summary_ytd = summary

        # need the monthly grouping too if period=week
        if period_type == 'week':
            summary_ytd = qr.AvailSummary(d_rng=d_rng_ytd, period='month')

        sec = SubSection('Fleet Availability', self) \
            .add_df(
                query=summary,
                func=summary.df_report,
                caption='Unit availability performance vs MA targets. \
                Units highlighted blue met the target. Columns [Total, SMS, Suncor] \
                highlighted darker red = worse performance.<br>*Unit F300 excluded \
                from summary calculations.') \
            .add_df(
                name='Fleet Availability YTD',  # used for chart
                query=summary_ytd,
                func=summary_ytd.df_report,
                da=dict(period='ytd'),
                display=False) \
            .add_df(
                name='Summary Totals',
                caption='Totals for units in Staffed vs AHS operation.',
                func=summary.df_totals,
                style_func=summary.style_totals) \
            .add_chart(
                func=ch.chart_fleet_ma,
                caption='Unit mechanical availabilty performance vs MA target (report period).') \
            .add_chart(
                name='Fleet Availability YTD',
                func=ch.chart_fleet_ma,
                title='Fleet MA - Actual vs Target (YTD)',
                caption='Unit mechanical availabilty performance vs MA target (YTD period).')

        title_topdowns = 'Downtime Categories'
        name_topdowns = f'Top {n} {title_topdowns}'
        name_topdowns_ytd = f'{name_topdowns} (YTD)'

        sec = SubSection(title_topdowns, self) \
            .add_df(
                name=name_topdowns,
                query=qr.AvailTopDowns(da=dict(d_rng=d_rng, minesite=ms, n=n)),
                has_chart=True,
                caption=f'Top {n} downtime categories (report period).') \
            .add_chart(
                name=name_topdowns,
                func=ch.chart_topdowns,
                linked=True) \
            .add_df(
                name=name_topdowns_ytd,
                query=qr.AvailTopDowns(da=dict(d_rng=d_rng_ytd, minesite=ms, n=n)),
                has_chart=True,
                caption=f'Top {n} downtime categories (YTD period).') \
            .add_chart(
                name=name_topdowns_ytd,
                func=ch.chart_topdowns,
                title=name_topdowns_ytd,
                linked=True)

        # needs to be subsec so can be weekly/monthly
        da = {}
        if period_type == 'month':

            # For FleetMonthlyReport re-use F300 query, else create new
            sec = report.sections.get('SMR Hours', None)
            query_f300 = sec.query_f300 if not sec is None else qr.UnitSMRMonthly(unit='F300')

            da = dict(
                totals=True,
                merge_f300=True,
                query_f300=query_f300)

        sec = SubSection('Availability History', self) \
            .add_df(
                query=summary,
                func=summary.df_history_rolling,
                da=da,
                style_func=summary.style_history,
                has_chart=False,
                caption=f'12 {period_type} rolling availability performance vs targets.') \
            .add_chart(
                func=ch.chart_avail_rolling,
                linked=False,
                caption=f'12 {period_type} rolling availability vs downtime hours.',
                da=dict(period_type=period_type))
        sec.force_pb = True

        sec = SubSection('MA Shortfalls', self) \
            .add_df(
                query=qr.AvailShortfalls(parent=summary, da=dict(d_rng=d_rng)),
                caption='Detailed description of major downtime events (>12 hrs) \
                for units which did not meet MA target.')

        f.set_self(vars())

    def set_exec_summary(self, ex):
        gq, m = self.report.get_query, {}

        m.update(gq('Fleet Availability').exec_summary(period='last'))
        m.update(gq('Fleet Availability YTD').exec_summary(period='ytd'))
        m.update(gq(self.name_topdowns).exec_summary())

        ex['Availability'] = m


class AvailStandalone(AvailBase):
    def __init__(self, report, **kw):
        super().__init__(report)

        sec = SubSection('Raw Data', self) \
            .add_df(
                query=qr.AvailRawData(da=dict(d_rng=self.d_rng)),
                caption='Raw downtime data for report period.')


class Components(Section):
    def __init__(self, report, title='Components', query=None, **kw):
        """Table of component changeout records"""
        super().__init__(title=title, report=report)
        minesite = self.minesite  # type: str
        d_rng = self.d_rng  # type: Tuple[dt, dt]

        # load query for 12 month rolling chart data
        query2 = qr.ComponentCOReport.from_yearly(d_upper=d_rng[-1], minesite=minesite, major=True)
        sec = SubSection('Component Changeouts (12 Month Rolling)', self) \
            .add_df(
                func=query2.df_component_period,
                da=dict(period='month'),
                has_chart=True,
                display=False) \
            .add_chart(
                func=ch.chart_comp_co,
                da=dict(period='month'),
                cap_align='left',
                caption='12 month rolling component changeouts by month/component type.')

        # all time failure rates
        query3 = qr.ComponentCOReport(d_rng=(dt(2015, 1, 1), d_rng[-1]), minesite=minesite, major=True)
        sec = SubSection('Component Failure Rates (All Time)', self) \
            .add_df(
                name='Mean Life',
                func=query3.df_mean_life,
                style_func=query3.update_style_mean_life,
                caption='Mean SMR at component changeout.<br><br>Notes:<br>\
                    - Bench_Pct_All is the mean SMR of all changeouts compared to the group\'s benchmark SMR.<br>\
                    - This table only includes "Failure/Warranty" and "High Hour Changeout" values.') \
            .add_df(
                name='failure_rates',
                func=query3.df_failures,
                has_chart=True,
                display=False) \
            .add_chart(
                name='failure_rates',
                func=ch.chart_comp_failure_rates,
                cap_align='left',
                da=dict(title='Major Component Failure Rates - All Time'),
                caption='All time component failure rates by category.<br>Failed = [Failed, Warranty]<br>\
                Not Failed = [Convenience, High Hour Changeout, Damage/Abuse, Pro Rata Buy-in, Other]')
        sec.force_pb = True

        # detailed data for current month
        if query is None:
            query = qr.ComponentCOReport(d_rng=d_rng, minesite=minesite)

        sec_name = 'Component Changeouts'
        sec = SubSection(sec_name, self) \
            .add_df(
                query=query,
                caption='Major component changeout history (report period only). \
                <br>Life achieved is the variance between benchmark SMR and SMR at changeout.')
        sec.force_pb = True

        f.set_self(vars())


class ComponentHistoryCharts(Section):
    def __init__(self, report, query, **kw):
        """Charts showing breakdown of component changeouts by type, quarterly and by """
        super().__init__(title='Summary', report=report)

        sec = SubSection('Mean Life', self) \
            .add_df(
                func=query.df_mean_life,
                style_func=query.update_style_mean_life,
                caption='Mean SMR at component changeout.<br><br>Notes:<br>\
                    - Bench_Pct_All is the mean SMR of all changeouts compared to the group\'s benchmark SMR.<br>\
                    - This table only includes "Failure/Warranty" and "High Hour Changeout" values.')

        sec = SubSection('Component Changeouts (Quarterly)', self) \
            .add_df(
                func=lambda: query.df_component_period(period='quarter'),
                has_chart=True,
                display=False) \
            .add_chart(
                func=ch.chart_comp_co,
                cap_align='left',
                caption='Component changeout type grouped per quarter.')
        sec.force_pb = True

        sec = SubSection('Component Failure Rates', self) \
            .add_df(
                func=query.df_failures,
                has_chart=True,
                display=False) \
            .add_chart(
                func=ch.chart_comp_failure_rates,
                cap_align='left',
                caption='Component failure rates by category.<br>Failed = [Failed, Warranty]<br>\
                Not Failed = [Convenience, High Hour Changeout, Damage/Abuse, Pro Rata Buy-in, Other]')


class PLMUnit(Section):
    def __init__(self, report, include_overloads: bool = False, **kw):
        super().__init__(title='PLM Analysis', report=report)
        d_rng = report.d_rng
        unit = report.unit

        self.query = qr.PLMUnit(unit=unit, d_lower=d_rng[0], d_upper=d_rng[1])
        query = self.query

        sec = SubSection('Summary', self) \
            .add_df(
                func=query.df_summary_report,
                style_func=query.update_style,
                caption=f'PLM Summary for unit {unit}') \
            .add_paragraph(
                func=self.set_paragraph,
                kw=dict(query=query))

        sec = SubSection('Payload History', self) \
            .add_df(
                func=query.df_monthly,
                da=dict(add_unit_smr=True),
                has_chart=True,
                display=False) \
            .add_chart(
                func=ch.chart_plm_monthly,
                cap_align='left',
                caption='PLM haul records per month.<br>*Final month may not represent complete month.\
                <br>*A large relative difference between SMR Operated and PLM records per month \
                could indicate posibble missing PLM records.')

        if include_overloads:
            sec = SubSection('Overloads', self) \
                .add_df(
                    func=query.df_overloads,
                    style_func=query.style_overloads,
                    caption='Accepted/rejected overloads >120%<br><br>' +
                    '- Payload_Quick = Payload Quick Estimate + Carryback<br>' +
                    '- "Pct_Gross" and "Pct_Quick" are percentages relative to target payload<br>' +
                    '- Distance = Loaded Haul Distance (km)<br>' +
                    '- All other units in metric tonnes<br><br>' +
                    '- Loads are discounted from ">120%" when any of the following conditions met:<br>' +
                    f'{self.tab}- flagged with any of B, C, D, N, or L<br>' +
                    f'{self.tab}- Payload_Quick % < 120%<br>' +
                    f'{self.tab}- Loaded Haul Distance < 1.0 km<br>') \
                .force_pb = True

    def format_header(self, m: Dict[str, Any]) -> Dict[str, str]:
        """Convert summary row to formatted header dict

        Parameters
        ----------
        m : Dict[str, Any]
            plm summary row dict

        Returns
        -------
        Dict[str, str]
        """
        return {
            'Unit': m['Unit'],
            'Target Payload': f'{m["TargetPayload"]}',
            'MinDate': f"{m['MinDate']:%Y-%m-%d}",
            'MaxDate': f"{m['MaxDate']:%Y-%m-%d}",
            'Total Loads': f"{m['TotalLoads']:,.0f}",
            'Accepted Loads': f"{m['Total_ExcludeFlags']:,.0f}"}

    def set_paragraph(self, query, **kw) -> str:
        """Set paragraph data from query
        - NOTE query needs to already be loaded, must call after load_dfs
        """
        m = query.df_summary.iloc[0].to_dict()
        return f.two_col_list(self.format_header(m))


class FCs(Section):
    def __init__(self, report, **kw):
        super().__init__(title='Factory Campaigns', report=report)
        d_rng, minesite = self.d_rng, self.minesite

        sec = SubSection('New FCs', self) \
            .add_df(
                query=qr.NewFCs(d_rng=d_rng, minesite=minesite),
                caption='All new FCs released during the report period.')

        sec = SubSection('Completed FCs', self) \
            .add_df(
                query=qr.FCComplete(d_rng=d_rng, minesite=minesite),
                caption='FCs completed during the report period, using minimum of (Date Completed, Date Claimed).')

        query_history = qr.FCHistoryRolling(d_rng=d_rng, minesite=minesite)
        sec = SubSection('FC History', self) \
            .add_df(
                func=query_history.df_history,
                display=False) \
            .add_chart(
                func=ch.chart_fc_history,
                linked=False,
                caption='Outstanding (M) vs Completed (All) count vs labour hours. (Measured at end of month).')

        fcsummary = qr.FCSummaryReport(minesite=minesite)  # need to pass parent query to FC summary 2 to use its df
        sec = SubSection('FC Summary', self) \
            .add_df(
                query=fcsummary,
                da=dict(default=True),
                caption='Completion status of currently open FCs.') \
            .add_df(
                name='FC Summary (2)',
                query=qr.FCSummaryReport2(parent=fcsummary),
                caption='Completion status of FCs per unit. (Extension of previous table, mandatory \
                FCs highlighted navy blue).\nY = Complete, N = Not Complete, S = Scheduled')


class Pictures(Section):
    def __init__(self, report, pictures: list, **kw):
        super().__init__(title='Pictures', report=report)

        sec = SubSection('Pictures', self, show_title=False) \
            .add_pictures(pictures)


class TSIs(Section):
    def __init__(self, report, **kw):
        super().__init__(title='TSI', report=report)

        query_history = qr.TSIHistoryRolling(d_upper=self.d_rng[1], minesite=self.minesite)
        sec = SubSection('TSI History', self) \
            .add_df(
                query=query_history,
                display=False) \
            .add_chart(
                func=ch.chart_tsi_history,
                linked=False,
                caption='TSIs submitted per month.')

        # tsis completed in last month
        # unit, title, smr, part smr, part name, part number, failure cause
        query = qr.TSIReport(d_rng=self.d_rng, minesite=self.minesite)
        sec = SubSection('TSIs Submitted', self) \
            .add_df(
                query=query,
                caption='TSIs submitted during the report period.')


class SubSection():
    """SubSections add dfs/charts/paragraphs/images"""

    def __init__(
            self,
            title: str,
            section: 'Section',
            show_title: bool = True):

        section.sub_sections[title] = self  # add self to parent's section
        report = section.report
        elements = []
        paragraph = None
        paragraph_func = None
        force_pb = False
        f.set_self(vars())

    def add_df(
            self,
            name=None,
            func=None,
            query=None,
            da={},
            display=True,
            has_chart=False,
            caption=None,
            style_func=None):
        if name is None:
            name = self.title

        if not caption is None:
            caption = caption.replace('\n', '<br>')

        self.report.dfs[name] = dict(
            name=name,
            func=func,
            query=query,
            da=da,
            df=None,
            df_html=None,
            display=display,
            has_chart=has_chart)

        self.report.style_funcs |= {name: style_func}

        if display:
            self.elements.append(dict(name=name, type='df', caption=caption))

        return self

    def add_chart(self, func, name=None, linked=False, title=None, caption=None, da={}, cap_align='center'):
        if name is None:
            name = self.title

        # pass name of existing df AND chart function
        self.report.charts[name] = dict(name=name, func=func, path='', title=title, da=da)

        # don't add to its own section if linked to display beside a df
        if not linked:
            cap_class = f'figcaption_{cap_align}'  # align chart caption left or center
            self.elements.append(dict(name=name, type='chart', caption=caption, cap_class=cap_class))

        return self

    def add_paragraph(self, func, kw=None):
        if kw is None:
            kw = {}

        self.paragraph_func = dict(func=func, kw=kw)
        return self

    def add_pictures(self, pictures: list) -> None:
        self.elements.append(dict(type='pictures', pictures=pictures))

        return self
