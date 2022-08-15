import operator as op
import sys
from typing import *

from PyQt6.QtWidgets import QPushButton

from guesttracker import dbtransaction as dbt
from guesttracker import delta, dt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker import queries as qr
from guesttracker.database import db
from guesttracker.gui.dialogs.base import InputField, InputForm, check_app

if TYPE_CHECKING:

    from guesttracker.gui.formfields import CheckBox
    from guesttracker.gui.tables import TableWidget

log = getlog(__name__)

# Dialogs to show and allow user to create filters before refreshing table data

# save settings on accept. ClassName > form name? > value
# https://stackoverflow.com/questions/23279125/python-pyqt4-functions-to-save-and-restore-ui-widget-values


class RefreshTable(InputForm):
    """Qdialog class to create menus to refresh tables"""

    # set default db tables for field
    tables = dict(
        model='UnitID',
        minesite='UnitID',
        model_base='EquipType',
        model_base_all='EquipType',
        type='FCSummary',
        manual_closed='FCSummaryMineSite',
        component='ComponentType',
        user_group='UserSettings',
        major_components='ComponentType'
    )

    def __init__(self, parent: Union['TableWidget', None] = None, name: Union[str, None] = None, **kw):

        # initialize with proper table when called on its own for testing
        if parent is None:
            if name is None:
                name = self.__class__.__name__

            from guesttracker.gui import tables
            parent_name = getattr(tables, name, None)
            if parent_name:
                parent = parent_name()

        super().__init__(parent=parent, window_title='Refresh Table', **kw)
        # self.minesite = gbl.get_minesite()

    def toggle(self, state: int) -> None:

        source = self.sender()  # type: CheckBox
        box = source.box
        field = box.field

        # if Qt.CheckState(state) == Qt.CheckState.Checked:
        self.cb_changed(field_name=field.name, on_toggle=True)

        super().toggle(state)

    def add_features(self, features: List[Union[str, Dict[str, str]]]) -> None:
        """Add multiple features to refresh menu"""
        for kw in features:

            # allow passing in dict of params or just str name
            if isinstance(kw, str):
                kw = dict(name=kw)

            self.add_feature(**kw)

    def add_feature(self, name: str, table: str = None, cb_enabled: bool = False) -> None:
        """Add single feature to refresh menu"""
        parent, ms = self.parent, self.minesite
        name, title = name.lower(), name.replace('_', ' ').title()
        IPF = InputField
        add_input, add_refresh_button = self.add_input, self.add_refresh_button

        if table is None:
            table = self.tables.get(name, None)

        # this is ugly, would rather set up objects and only call when needed, but hard to initialize
        if name == 'all_open':
            add_refresh_button(name=title, func=parent.refresh_allopen)

        elif name == 'last_week':
            add_refresh_button(name=title, func=lambda: parent.refresh_lastweek(base=False))

        elif name == 'last_month':
            add_refresh_button(name=title, func=lambda: parent.refresh_lastmonth(base=False))

        elif name == 'minesite':
            lst = db.get_list_minesite()

            add_input(
                field=IPF(
                    text='MineSite',
                    default=ms,
                    table=table),
                items=lst,
                checkbox=True,
                box_changed=lambda x: self.cb_changed('MineSite'))

        elif name == 'work_order':
            add_input(field=IPF(text=title), checkbox=True, cb_enabled=False)

        elif name == 'unit':
            df = db.get_df_unit()
            # lst = df[df.MineSite == ms].Unit.pipe(f.clean_series)
            lst = df.Unit.pipe(f.clean_series)
            add_input(
                field=IPF(text=title),
                items=lst,
                checkbox=True,
                cb_enabled=False,
                box_changed=lambda x: self.cb_changed('Unit'))

        elif 'model' in name:

            col = 'ModelBase' if 'model_base' in name else 'Model'
            df = db.get_df_unit()
            # if not 'all' in name:
            #     df = df.query('MineSite == @ms')

            lst = df[col].pipe(f.clean_series)
            add_input(
                field=IPF(
                    text=f.lower_cols(col)[0].replace('_', ' ').title(),
                    table=table),
                items=lst,
                checkbox=True,
                cb_enabled=False,
                box_changed=lambda x: self.cb_changed(col))

        elif name == 'type':
            add_input(
                field=IPF(
                    text=title,
                    col_db='Classification',
                    table=table),
                items=['M', 'FAF', 'DO', 'FT'],
                checkbox=True,
                cb_enabled=False,
                box_changed=lambda x: self.cb_changed('Type'))

        elif name == 'fc_number':
            df = db.get_df_fc(default=False, minesite=ms)
            lst = f.clean_series(s=df['FC Number'])
            add_input(field=IPF(text='FC Number'), items=lst, checkbox=True, cb_enabled=False)

        elif name == 'fc_complete':
            add_input(
                field=IPF(
                    text=title,
                    col_db='Complete'),
                items=['False', 'True'],
                checkbox=True,
                cb_enabled=False)

        elif name == 'fc_subject':
            df = db.get_df_fc(default=False, minesite=ms)
            lst = f.clean_series(df.Subject)
            add_input(
                field=IPF(
                    text='FC Number',
                    table='FCSummary',
                    col_db='SubjectShort'),
                items=lst,
                checkbox=True,
                cb_enabled=False)

        elif name == 'fc_title':
            df = db.get_df_fc(default=False, minesite=ms)
            lst = f.clean_series(df.Title)
            add_input(
                field=IPF(
                    text='FC Title',
                    table='FCSummary',
                    col_db='FCNumber',
                    func=FCBase.fc_from_title),
                items=lst,
                checkbox=True,
                cb_enabled=False)

        elif name == 'manual_closed':
            add_input(
                field=IPF(text=title, default='False', table=table), items=['False', 'True'],
                checkbox=True,
                tooltip='"Manual Closed = True" means FCs that HAVE been closed/removed from the FC Summary table. \
                This is used to manage which FCs are "active".')

        elif name == 'start_date':
            # TODO: only set up for eventlog table currently
            add_input(
                field=IPF(
                    text=title,
                    dtype='date',
                    col_db=self.col_db_startdate),
                checkbox=True,
                cb_enabled=False)

        elif name == 'end_date':
            add_input(
                field=IPF(
                    text=title,
                    dtype='date',
                    col_db=self.col_db_enddate,
                    opr=op.le),
                checkbox=True,
                cb_enabled=False)

        elif name == 'component':
            df = db.get_df_component()
            lst = f.clean_series(df.Component)
            add_input(field=IPF(text=title, table=table), items=lst, checkbox=True, cb_enabled=False)

        elif name == 'component_oil':
            add_input(
                field=IPF(
                    text='Component',
                    col_db='component_id'),
                items=[],
                checkbox=True,
                cb_enabled=False)

        elif name == 'limit_component':
            title = 'Limit per Component'
            field = add_input(
                field=IPF(
                    text=title,
                    dtype='int',
                    default=5),
                checkbox=True,
                cb_enabled=True)
            field.exclude_filter = True

        elif name == 'tsi_author':
            add_input(
                field=IPF(
                    text='TSI Author',
                    default=self.mainwindow.username,
                    col_db='TSIAuthor'),
                checkbox=True,
                cb_enabled=False)

        elif name == 'tsi_number':
            add_input(field=IPF(text='TSI Number'), checkbox=True, cb_enabled=False)

        elif name == 'major_components':

            add_input(field=IPF(text=title, default='True', table=table, col_db='Major'),
                      items=['True', 'False'], checkbox=True, cb_enabled=cb_enabled)

        elif name == 'title':
            add_input(field=IPF(text=title, col_db=title, like=True), checkbox=True, cb_enabled=False,
                      tooltip='Use wildcards * to match results containing partial text.\nEg:\n\
                - Steering* > return "Steering Pump" and "Steering Arm", but not "Change Steering Pump"\n\
                - *MTA* > return "The MTA Broke" and "MTA Failure"')

        elif name == 'user_group':
            u = self.mainwindow.u
            enabled = False if self.parent.title in ('Event Log', 'Work Orders') and not u.is_cummins else True

            field = IPF(
                text=title,
                default=db.domain_map_inv.get(u.domain, 'SMS'),
                col_db='UserGroup',
                table=table)

            add_input(
                field=field,
                items=db.domain_map.keys(),
                checkbox=True,
                cb_enabled=enabled,
                tooltip='Limit results to only those created by users in your domain.\n\
                (Ensure users have been correctly initialized.)')

        elif 'part_name' in name:

            if 'alt' in name:
                col_db = 'PartNameAlt'
            elif 'tsi' in name:
                col_db = 'TSIPartName'
            else:
                col_db = 'PartName'

            add_input(
                field=IPF(text='Part Name', col_db=col_db, like=True),
                checkbox=True,
                cb_enabled=False,
                tooltip='This searches both "Part Name" and "Alt Part Name" (Parts table).')

        elif 'part_number' in name:
            if 'el' in name:
                col_db = 'PartNumber'
            else:
                col_db = 'PartNo'  # parts table

            add_input(field=IPF(text='Part Number', col_db=col_db, like=True), checkbox=True, cb_enabled=False)

        elif name == 'order_parts':
            add_input(field=IPF(text='Order Parts Number', like=True,
                      col_db='OrderParts'), checkbox=True, cb_enabled=False)

        elif name == 'failure_cause':
            add_input(field=IPF(text=title, like=True), checkbox=True, cb_enabled=False)

    def add_refresh_button(self, name, func):
        layout = self.v_layout
        btn = QPushButton(name, self)
        btn.setMaximumWidth(60)
        layout.insertWidget(0, btn)
        btn.clicked.connect(self._add_items_to_filter)
        btn.clicked.connect(super().accept)
        btn.clicked.connect(func)

    def accept(self):
        self._add_items_to_filter()
        self.parent.refresh()
        return super().accept()

    def get_fltr(self):
        # get filter from parent table or just create default for testing
        parent = self.parent
        if not parent is None:
            return parent.query.fltr
        else:
            return qr.EventLog().fltr  # default


class HBABase(RefreshTable):

    def __init__(self, name: str, parent: Union['TableWidget', None] = None):
        super().__init__(parent=parent, name=name)
        self.name = name

        self.table = dbt.get_table_model(table_name=name)
        self.add_default_fields(input_type='refresh')


class EventLogBase(RefreshTable):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.col_db_startdate, self.col_db_enddate = 'DateAdded', 'DateAdded'

        features = ['last_month', 'last_week', 'all_open', 'minesite',
                    'unit', 'model', 'model_base', 'title', 'work_order', 'start_date', 'end_date']
        self.add_features(features=features)
        self.insert_linesep(i=3, layout_type='vbox')


class EventLog(EventLogBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.add_feature(name='user_group')
        self.add_features(features=['user_group', 'failure_cause'])


class WorkOrders(EventLogBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.add_feature(name='user_group')
        self.add_features(features=['user_group', 'el_part_number', 'order_parts'])


class ComponentCO(EventLogBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.add_features(
            features=['component', dict(name='major_components', cb_enabled=False), 'failure_cause'])


class ComponentSMR(RefreshTable):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        # Component/major comp need different table
        t = 'viewPredictedCO'
        features = [
            dict(name='minesite', table=t),
            dict(name='unit', table=t),
            dict(name='model', table=t),
            dict(name='model_base', table=t),
            dict(name='component', table=t),
            dict(name='major_components', cb_enabled=True, table=t)]

        self.add_features(features)


class TSI(EventLogBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.add_features(
            features=['tsi_author', 'tsi_number', 'tsi_part_name',
                      'el_part_number', 'failure_cause'])


class FCBase(RefreshTable):
    def __init__(self, parent=None):
        super().__init__(parent=parent, max_combo_width=400)
        self.df = db.get_df_fc(default=False)

        # Add Model/ModelBase to self.df_filter for filtering on cb changes
        dfu = db.get_df_unit()[['Model', 'ModelBase']]
        self.df_filter = self.df \
            .merge(right=dfu, how='left', right_index=True, left_on='Unit')

    @staticmethod
    def fc_from_title(title: str) -> str:
        """get fc_number from combined title

        Parameters
        ----------
        title : str
            eg '17H019 - Inspect Things'

        Returns
        -------
        str
            eg '17H019'
        """
        return title.split(' - ')[0].strip()


class FCSummary(FCBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        features = ['all_open', 'minesite', 'fc_title', 'type',
                    'manual_closed', 'model', 'model_base', 'Unit']
        self.add_features(features=features)


class FCDetails(FCBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        features = ['all_open', 'minesite', 'fc_title', 'type',
                    'manual_closed', 'fc_complete', 'model', 'model_base', 'unit']
        self.add_features(features=features)


class UnitInfo(RefreshTable):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        features = ['minesite', 'model', 'model_base']
        self.add_features(features=features)


class EmailList(RefreshTable):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        features = [
            dict(name='MineSite', table='EmailList'),
            dict(name='user_group', table='EmailList')]
        self.add_features(features=features)


class Availability(RefreshTable):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        col_db_startdate, col_db_enddate = 'ShiftDate', 'ShiftDate'

        df_week = qr.df_period(freq='week')
        df_month = qr.df_period(freq='month')

        d = dt.now() + delta(days=-6)
        default_week = df_week[df_week.start_date < d].iloc[-1, :].name  # index name

        d = dt.now() + delta(days=-30)
        default_month = df_month[df_month.start_date < d].iloc[-1, :].name  # index name

        f.set_self(vars())

        self.add_input(field=InputField(text='Week', default=default_week),
                       items=df_week.index, checkbox=True, cb_enabled=False)
        self.add_input(field=InputField(text='Month', default=default_month),
                       items=df_month.index, checkbox=True, cb_enabled=False)

        self.add_features(['start_date', 'end_date', 'unit'])
        self.insert_linesep(i=2)

    def set_rng(self):
        fMonth, fWeek = self.fMonth, self.fWeek

        if fMonth.cb.isChecked():
            name = fMonth.val
            period_type = 'month'
        elif fWeek.cb.isChecked():
            name = fWeek.val
            period_type = 'week'

        df = qr.df_period(freq=period_type)
        d_rng = df.loc[name, 'd_rng']
        f.set_self(vars())

    def accept(self):
        if any([self.fWeek.cb.isChecked(), self.fMonth.cb.isChecked()]):

            self.set_rng()
            self.parent.query.fltr.add(vals=dict(ShiftDate=self.d_rng), term='between')
            self.parent.refresh()
            return super().accept_()
        else:
            return super().accept()


class AvailReport(Availability):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

    def accept(self):
        self.set_rng()
        return super().accept_()


class OilSamples(RefreshTable):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.col_db_startdate, self.col_db_enddate = 'sample_date', 'sample_date'

        features = ['minesite', 'unit', 'model_base', 'component_oil', 'limit_component', 'start_date', 'end_date']
        self.add_features(features=features)
        # self.insert_linesep(i=3, layout_type='vbox')

        # load all components to start
        self.df_oil = db.get_df_oil_components()
        self.set_oil_components(field='minesite', force=True)

        self.fUnit.box.changed.connect(lambda: self.set_oil_components(field='unit'))
        self.fModelBase.box.changed.connect(lambda: self.set_oil_components(field='model_base'))
        self.fMineSite.box.changed.connect(lambda: self.set_oil_components(field='minesite'))

    def set_oil_components(self, field='minesite', force: bool = False) -> None:
        """Reset components list when unit or minesite changed
        - Give checked preference to unit > model_base > minesite
        """
        cb_unit = self.fUnit.cb
        cb_model = self.fModelBase.cb
        cb_minesite = self.fMineSite.cb

        if field == 'unit':
            # don't change components if unit isn't enabled
            if not cb_unit.isChecked():
                return

            val = self.fUnit.val

        elif field == 'model_base':

            if not cb_model.isChecked() or cb_unit.isChecked():
                return

            val = self.fModelBase.val

        elif field == 'minesite':
            if not force:
                if (not cb_model.isChecked()
                    or cb_unit.isChecked()
                        or cb_model.isChecked()):
                    return

            val = self.fMineSite.val

        items = self.df_oil \
            .pipe(lambda df: df[df[field] == val]) \
            .component_id \
            .pipe(f.clean_series)

        self.fComponent.box.set_items(items)

    def accept(self):

        field = self.fLimitperComponent

        if field.cb.isChecked():
            self.parent.query.limit_top_component(n=field.box.val)

        return super().accept()


class Parts(RefreshTable):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        features = ['part_name', 'part_number', 'model_all', 'model_base_all']
        self.add_features(features=features)


def show_item(name, parent=None):
    # show message dialog by name eg gbl.show_item('InputUserName')
    app = check_app()
    dlg = getattr(sys.modules[__name__], name)(parent=parent)
    return dlg, dlg.exec()
