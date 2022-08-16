import time
import uuid
from datetime import datetime as dt
from typing import TYPE_CHECKING, Any, Dict, List, Union

from PyQt6.QtCore import QSize, Qt, pyqtSlot
from PyQt6.QtWidgets import QHBoxLayout, QLabel, QPushButton

from guesttracker import config as cf
from guesttracker import dbtransaction as dbt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.data import factorycampaign as fc
from guesttracker.database import db
from guesttracker.gui import _global as gbl
from guesttracker.gui import formfields as ff
from guesttracker.gui.dialogs.dialogbase import (
    InputField, InputForm, add_linesep, msg_simple, unit_exists)
from guesttracker.gui.dialogs.tables import UnitOpenFC
from guesttracker.utils import dbmodel as dbm

if TYPE_CHECKING:
    from guesttracker.gui.tables import TableWidget

log = getlog(__name__)


class AddRow(InputForm):
    def __init__(
            self,
            parent: Union['TableWidget', None],
            window_title: str = 'Add Item',
            **kw):
        super().__init__(parent=parent, window_title=window_title, **kw)
        self.m = {}  # type: Dict[str, Any] # need dict for extra cols not in dbm table model (eg UnitID)
        self.queue = []
        self.add_keys = []
        self.row = self.create_row()  # kinda sketch, row is actually dmb.Base, not dbt.Row

        self.parent = parent  # type: TableWidget

    def create_row(self) -> dbm.Base:
        parent = self.parent
        if not parent is None:
            self.data_model = parent.view.data_model
            self.title = parent.title
            self.dbtable = parent.dbtable
        else:
            # Temp testing vals
            self.data_model = None
            self.title = 'Event Log'
            self.tablename = 'EventLog'
            self.dbtable = getattr(dbm, self.tablename)

        return self.dbtable(uid=uuid.uuid4())

    def add_row_table(self, row: dbm.Base, m: Union[Dict[str, Any], None] = None) -> None:
        """Convert row model to dict of values and append to current table without updating db

        Parameters
        ----------
        row : dbm.Base
        m : Union[Dict[str, Any], None], optional
            alternate/modified dict to insert to table, default None
        """
        m = m or self.m

        m |= dbt.model_dict(model=row)
        m = f.convert_dict_db_view(title=self.title, m=m, output='view')

        if not self.data_model is None:
            self.data_model.insertRows(m=m, select=True)

    def add_row_queue(self, row):
        # add row (model) to queue
        self.queue.append(row)

    def flush_queue(self) -> None:
        """Bulk update all rows in self.queue to db"""
        update_items = [dbt.model_dict(row) for row in self.queue]  # list of dicts

        txn = dbt.DBTransaction(data_model=self.data_model, dbtable=self.dbtable, title=self.title) \
            .add_items(update_items=update_items) \
            .update_all(operation_type='insert')

    def set_row_attrs(self, row: dbm.Base, exclude: Union[List[str], None] = None) -> None:
        """Copy values to dbmodel from current dialog field values"""
        if exclude is None:
            exclude = []
        elif not isinstance(exclude, list):
            exclude = [exclude]

        # create dict of values to set from dialog fields
        vals = {field.col_db: field.val
                for field in self.fields.values()
                if not field.isna and not field.text in exclude and field.box.isEnabled()}

        row = dbt.set_row_vals(model=row, vals=vals)

    def accept_2(self):
        # not sure if need this yet
        return super().accept()

    def accept(self):
        if not self.check_enforce_items():
            return

        super().accept()
        row = self.row
        self.set_row_attrs(row=row)
        # dbt.print_model(row)

        if not db.add_row(row=row):
            self.update_statusbar('Failed to add row to database.', warn=True)
            return False

        self.add_row_table(row=row)

        self.parent.view.resizeRowsToContents()

        self.success_message()

        return True  # row successfully added to db

    def success_message(self) -> None:
        """Update statusbar with info on new row added
        - Must set self.add_keys
        """
        name = getattr(self, 'name', 'row')
        m = self.items
        msg = f'New {name} added to database: ' \
            + ', '.join([m.get(k, None) for k in self.add_keys])

        self.update_statusbar(msg, success=True)


class HBAAddRow(AddRow):
    def __init__(self, name: str, **kw):
        super().__init__(window_title=f'Add {name}', use_saved_settings=False, **kw)
        self.name = name

        self.add_default_fields(input_type='update')


class Reservations(HBAAddRow):
    def __init__(self, **kw):
        super().__init__(**kw)

        self.df_cust = db.get_df_customers()

        self.add_input(
            field=InputField(
                text='Customer Name',
                col_db='customer_id',
                enforce=True,
            ),
            items=f.clean_series(self.df_cust['name']),
        )

    def accept(self):
        row = self.row
        row.status = 6  # TODO change this
        self.m['customer_name'] = self.fields_db['customer_id'].val

        # show unit table and allow multi select - db.get_df_units()

        # add "select units" button

        # validate unit availability

        return super().accept()


class Customers(HBAAddRow):
    def accept(self):
        row = self.row
        row.name = self.fields_db['name_first'].val + ' ' + self.fields_db['name_last'].val
        row.first_contact = dt.now().date()

        return super().accept()


class AddEmail(AddRow):
    def __init__(self, parent=None):
        super().__init__(parent=parent, window_title='Add Email')
        IPF = InputField
        self.add_input(field=IPF(text='MineSite', default=gbl.get_minesite()),
                       items=db.get_list_minesite(include_custom=False))
        self.add_input(field=IPF(text='Email'))
        self.add_input(field=IPF(text='User Group', default=self.mw.u.usergroup), items=db.domain_map.keys())

        self.name = 'email'
        self.add_keys = ['MineSite', 'Email']


class AddEvent(AddRow):
    def __init__(self, parent=None):
        super().__init__(parent=parent, window_title='Add Event')
        fc_number = None
        IPF, add = InputField, self.add_input
        is_cummins = parent.u.is_cummins if not parent is None else False
        self._save_items = ('unit_qcombobox',)
        self.name = 'event'
        self.add_keys = ['Unit', 'Title']

        layout = self.v_layout
        df = db.get_df_unit()

        add(
            field=IPF(text='MineSite', default=self.minesite),
            items=db.get_list_minesite(include_custom=False),
            box_changed=lambda x: self.cb_changed('MineSite'))
        add(field=IPF(text='Unit', enforce=True), items=list(df[df.MineSite == self.minesite].Unit))
        add(field=IPF(text='Date', dtype='date', col_db='DateAdded'))
        add(
            field=IPF(text='Time Called', dtype='datetime', col_db='TimeCalled'),
            checkbox=True,
            cb_enabled=False)

        # Add btn to check smr
        btn = self.create_button('Get SMR')
        btn.clicked.connect(self.get_smr)
        self.add_input(field=IPF(text='SMR', dtype='int'), btn=btn)

        if not is_cummins:
            # Checkboxes
            cb_eventfolder = ff.CheckBox('Create Event Folder', checked=True)
            # cb_onedrive = ff.CheckBox('Create OneDrive Folder', checked=True)

            tsi_checked = True if parent and parent.name == 'TSI' else False
            cb_tsi = ff.CheckBox('Create TSI', checked=tsi_checked)

            # FC
            cb_fc = ff.CheckBox('Link FC')
            cb_fc.stateChanged.connect(self.select_fc)
            btn_open_fcs = btn = self.create_button('Open FCs')
            btn_open_fcs.setToolTip('View all open FCs for current unit.')
            btn_open_fcs.clicked.connect(self.show_open_fcs)
            box_fc = QHBoxLayout()
            box_fc.addWidget(cb_fc)
            box_fc.addWidget(btn_open_fcs)

            # outstanding FCs
            box_fc_out = QHBoxLayout()
            label_fc = QLabel('Outstanding FCs: ')
            label_fc_m = QLabel('M: ')
            label_fc_other = QLabel('Other: ')
            box_fc_out.addWidget(label_fc)
            box_fc_out.addStretch(1)
            box_fc_out.addWidget(label_fc_m)
            box_fc_out.addWidget(label_fc_other)

            # cb_eventfolder.stateChanged.connect(self.toggle_ef)

            self.form_layout.addRow('', cb_eventfolder)
            # self.form_layout.addRow('', cb_onedrive)
            self.form_layout.addRow('', cb_tsi)

            add_linesep(self.form_layout)
            self.form_layout.addRow('', box_fc)
            self.form_layout.addRow('', box_fc_out)
            add_linesep(self.form_layout)

        add(field=IPF(text='Title', dtype='textbox', enforce=True))
        add(field=IPF(text='Failure Cause', dtype='textbox'))

        # Warranty Status
        if is_cummins:
            wnty_default = 'WNTY'
            list_name = 'WarrantyTypeCummins'
        else:
            list_name = 'WarrantyType'
            wnty_default = 'Yes'

        add(
            field=IPF(
                text='Warranty Status',
                col_db='WarrantyYN',
                default=wnty_default),
            items=cf.config['Lists'][list_name])

        add(field=IPF(text='Work Order', col_db='WorkOrder'))
        add(field=IPF(text='WO Customer', col_db='SuncorWO'))
        add(field=IPF(text='PO Customer', col_db='SuncorPO'))

        self.add_component_fields()

        self._restore_settings()
        self.fUnit.box.select_all()
        f.set_self(vars())

        self.accepted.connect(self.close)
        self.rejected.connect(self.close)
        self.fUnit.box.currentIndexChanged.connect(self.update_open_fc_labels)
        self.update_open_fc_labels()
        self.show()

    def update_open_fc_labels(self):
        """Update outstanding FC labels for M and Other when unit changed"""
        if self.is_cummins:
            return  # fc labels not implemented for cummins

        unit = self.fUnit.val

        s = db.get_df_fc(default=True, unit=unit) \
            .groupby(['Type']).size()

        count_m = s.get('M', 0)
        count_other = sum(s[s.index != 'M'].values)

        self.label_fc_m.setText(f'M: {count_m}')
        self.label_fc_other.setText(f'Other: {count_other}')

        color = '#ff5454' if count_m > 0 else 'white'
        self.label_fc_m.setStyleSheet(f"""QLabel {{color: {color}}}""")

    def toggle_ef(self, state):
        """Toggle OneDrive folder when EventFolder change"""
        # source = self.sender()
        # box = source.box
        cb = self.cb_onedrive

        if Qt.CheckState(state) == Qt.CheckState.Checked:
            cb.setEnabled(True)
        else:
            cb.setEnabled(False)

    @pyqtSlot(int)
    def component_changed(self, ix):
        # Update Title text when Component selected in combobox
        combo = self.sender()
        val = combo.val
        if not val.strip() == '':
            self.fTitle.val = f'{val} - CO'

    def create_row(self):
        row = super().create_row()
        row.UID = self.create_uid()
        row.CreatedBy = self.mainwindow.username if not self.mainwindow is None else ''
        row.StatusEvent = 'Work In Progress'
        row.StatusWO = 'Open'
        row.Seg = 1
        row.Pictures = 0

        return row

    def create_button(self, name, width=80):
        btn = QPushButton(name, self)
        btn.setFixedSize(QSize(width, btn.sizeHint().height()))
        return btn

    def add_component_fields(self):
        # Add fields to select component/component SMR 1 + 2
        IPF = InputField

        def _add_component(text):
            field = self.add_input(
                field=IPF(
                    text=text,
                    dtype='combobox',
                    col_db='Floc',
                    enforce=True),
                checkbox=True,
                cb_enabled=False)

            field.cb.stateChanged.connect(self.load_components)
            return field

        def _add_removal(text):
            field = self.add_input(
                field=IPF(
                    text=text,
                    dtype='combobox',
                    col_db='SunCOReason',
                    enforce=True),
                enabled=False)

            return field

        def _add_smr(text):
            btn = self.create_button('Get SMR')
            btn.clicked.connect(self.get_component_smr)
            field = self.add_input(field=IPF(text=text, dtype='int', col_db='ComponentSMR'), btn=btn)
            field.box.setEnabled(False)
            return field

        add_linesep(self.form_layout)

        for suff in ('', ' 2'):
            field_comp = _add_component(f'Component CO{suff}')
            field_smr = _add_smr(f'Component SMR{suff}')
            field_removal = _add_removal(f'Removal Reason{suff}')

            field_comp.box_smr = field_smr.box  # lol v messy
            field_smr.box_comp = field_comp.box
            field_comp.box_removal = field_removal.box

        add_linesep(self.form_layout)
        self.fComponentCO.box.currentIndexChanged.connect(self.component_changed)

    @property
    def df_comp(self):
        if not hasattr(self, '_df_comp') or self._df_comp is None:
            self._df_comp = db.get_df_component()
        return self._df_comp

    def get_floc(self, component_combined):
        df = self.df_comp
        return df[df.Combined == component_combined].Floc.values[0]

    def load_components(self, state, *args):
        """Reload components to current unit when component co toggled
        - Also toggle smr boxes"""

        source = self.sender()  # source is checkbox
        box = source.box
        box_smr = box.field.box_smr
        box_removal = box.field.box_removal

        if Qt.CheckState(state) == Qt.CheckState.Checked:
            df = self.df_comp
            unit = self.fUnit.val
            equip_class = db.get_unit_val(unit=unit, field='EquipClass')
            s = df[df.EquipClass == equip_class].Combined
            lst = f.clean_series(s)
            box.set_items(lst)

            # add removal reason items
            lst_removal = cf.config['Lists']['RemovalReason']
            box_removal.set_items(lst_removal)
            # box_removal.val = 'High Hour Changeout'
            box_removal.val = ''
            box_removal.setEnabled(True)

            box.lineEdit().selectAll()
            box_smr.setEnabled(True)
            box_removal.setEnabled(True)
        else:
            box_smr.setEnabled(False)
            box_removal.setEnabled(False)

    def get_component_smr(self):
        source = self.sender()
        box = source.box  # box is linked to btn through add_input

        df = self.df_comp
        unit, smr, date = self.fUnit.val, self.fSMR.val, self.fDate.val
        component = box.field.box_comp.val

        # spinbox returns None if val is 0
        if smr is None:
            smr = 0

        if smr <= 0:
            msg = 'Set Unit SMR first!'
            msg_simple(msg=msg, icon='warning')
            return

        # get last CO from EL by floc
        floc = self.get_floc(component_combined=component)
        smr_last = smr - db.get_smr_prev_co(unit=unit, floc=floc, date=date)

        if not smr_last is None:
            box.val = smr_last
        else:
            box.val = smr
            m = dict(Unit=unit, component=component)
            msg = f'No previous component changeouts found for: \
            \n{f.pretty_dict(m)}\n\nSetting Component SMR to current unit SMR: {smr}'
            msg_simple(msg=msg, icon='warning')

    def get_smr(self):
        # NOTE could select all nearby dates in db and show to user
        unit, date = self.fUnit.val, self.fDate.val
        smr = db.get_smr(unit=unit, date=date)

        if not smr is None:
            self.fSMR.val = smr
        else:
            msg = f'No SMR found for\n\n \
                Unit: {unit}\nDate: {date}\n\n \
                Note - Daily SMR values are not uploaded to the database until 12:20 MST.'
            msg_simple(msg=msg, icon='warning')

    def create_uid(self):
        return str(time.time()).replace('.', '')[:12]

    def show_open_fcs(self):
        """Toggle open FCs dialog for current unit"""
        if not hasattr(self, 'dlg_fc') or self.dlg_fc is None:
            unit = self.fUnit.val
            dlg_fc = UnitOpenFC(parent=self, unit=unit)

            # move fc top left to AddEvent top right
            dlg_fc.move(self.frameGeometry().topRight())
            dlg_fc.show()
            self.dlg_fc = dlg_fc
        else:
            self.dlg_fc.close()
            self.dlg_fc = None

    def select_fc(self, state):
        """Show dialog to select FC from list"""

        if not Qt.CheckState(state) == Qt.CheckState.Checked:
            return

        ok, fc_number, title = fc.select_fc(unit=self.fUnit.val)

        if not ok:
            self.cb_fc.setChecked(False)
            return

        self.fTitle.val = title
        self.fc_number = fc_number

    def accept(self):
        """AddEvent accept adds rows differently (can queue multiple)
        - just bypass everthing and call base qt accept"""
        row, m = self.row, self.m
        unit = self.fUnit.val
        rows = []

        if not self.check_enforce_items():
            return

        self.add_row_queue(row=row)  # need to update at least row1

        if not unit_exists(unit=unit):
            return

        # add these values to display in table
        m['Model'] = db.get_unit_val(unit=unit, field='Model')
        m['Serial'] = db.get_unit_val(unit=unit, field='Serial')

        # Make sure title is good
        self.fTitle.val = f.nice_title(self.fTitle.val)

        if self.is_cummins:
            row.IssueCategory = 'Engine'
        else:
            # create TSI row (row 1 only)
            if self.cb_tsi.isChecked():
                row.StatusTSI = 'Open'

                if not self.mainwindow is None:
                    row.TSIAuthor = self.mainwindow.get_username()

        self.set_row_attrs(
            row=row,
            exclude=['Component CO 2', 'Component SMR 2', 'Removal Reason 2'])

        # Component CO 1
        if self.fComponentCO.cb.isChecked():
            row.ComponentCO = True
            row.Floc = self.get_floc(component_combined=self.fComponentCO.box.val)

        self.add_row_table(row=row)

        # Component CO 2 > duplicate self.row
        if self.fComponentCO2.cb.isChecked():
            row2 = self.create_row()
            self.set_row_attrs(row=row2)

            component = self.fComponentCO2.box.val
            row2.Floc = self.get_floc(component_combined=component)
            row2.Title = f'{component} - CO'
            row2.ComponentSMR = self.fComponentSMR2.box.val
            row2.ComponentCO = True
            row2.GroupCO = True
            self.row2 = row2
            self.add_row_queue(row=row2)
            self.add_row_table(row=row2)

            row.GrouCO = True

        self.flush_queue()
        self.accept_()
        self.parent.view.resizeRowsToContents()
        self.items = self.get_items()  # needed before success message
        self.success_message()

        if not self.is_cummins:
            if self.cb_fc.isChecked():
                fc.link_fc_db(unit=unit, uid=row.UID, fc_number=self.fc_number)

            if self.cb_eventfolder.isChecked():
                from guesttracker import eventfolders as efl
                ef = efl.EventFolder.from_model(e=row)
                ef.create_folder(ask_show=True)

    @classmethod
    def _get_handled_types(cls):
        """Don't save any settings except unit_qcombobox"""
        return tuple()

    def closeEvent(self, event):
        """Reimplement just to close the FC dialog too, couldn't find a better way"""
        try:
            self.dlg_fc.close()
        except Exception as e:
            pass

        self._save_settings()
        return super().closeEvent(event)


class CreateModelbase(AddRow):
    def __init__(self, model, parent=None):
        super().__init__(parent=parent, window_title='Create ModelBase')

        lst = []  # get list of equiptypes
        df = db.get_df_equiptype()
        lst = f.clean_series(df.EquipClass)

        text = f'No ModelBase found for: "{model}". Select an EquipClass and create a ModelBase.\n\n' \
            '(This is used for grouping models into a base folder structure. Eg "980E-4" > "980E")\n'
        label = QLabel(text)
        label.setMaximumWidth(300)
        label.setWordWrap(True)
        self.v_layout.insertWidget(0, label)

        self.add_input(field=InputField(text='Equip Class'), items=lst)
        self.add_input(field=InputField(text='Model Base'))

        self.setMinimumSize(self.sizeHint())
        f.set_self(vars())

    def set_row_attrs(self, row, exclude=None):
        row.Model = self.model
        super().set_row_attrs(row=row, exclude=exclude)

    def accept(self):
        # check model base isn't blank
        model_base = self.fModelBase.val
        if model_base.strip() == '':
            msg_simple(msg='Model Base cannot be blank!', icon='warning')
            return

        row = self.row
        row.Model = self.model
        self.set_row_attrs(row=row)
        db.add_row(row=row)

        self.accept_2()

    def create_row(self):
        # not linked to a parent table, just return a row instance of EquipType table
        return dbm.EquipType()


class AddUnit(AddRow):
    def __init__(self, parent=None):
        super().__init__(parent=parent, window_title='Add Unit')
        df = db.get_df_unit()
        self.tablename = 'UnitID'
        self.name = 'unit'
        self.add_keys = ['Unit', 'Model']

        msg = 'NOTE: Newly added units will not show up in dropdown menus' \
            + ' until you do "Database > Reset Database Tables".\n'
        label = QLabel(msg)
        label.setMaximumWidth(300)
        label.setWordWrap(True)
        self.v_layout.insertWidget(0, label)

        self.add_input(field=InputField(text='Unit', enforce=True))
        self.add_input(field=InputField(text='Serial', enforce=True))
        self.add_input(field=InputField(text='Model'), items=f.clean_series(df.Model))
        self.add_input(field=InputField(text='MineSite', default=self.minesite),
                       items=db.get_list_minesite(include_custom=False))
        self.add_input(field=InputField(text='Customer'), items=f.clean_series(df.Customer))
        self.add_input(field=InputField(text='Engine Serial'))
        self.add_input(field=InputField(text='Delivery Date', dtype='date'))
        self.add_input(
            field=InputField(text='Is Component', col_db='is_component', dtype='bool'),
            items=cf.config['Lists']['TrueFalse'][::-1])

        self.show()

    def accept(self):
        # when model is set, check if model_base exists. If not prompt to create one
        model, unit = self.fModel.val, self.fUnit.val
        modelbase = db.get_modelbase(model=model)

        if modelbase is None:
            dlg = CreateModelbase(model=model, parent=self)
            if not dlg.exec():
                return

        super().accept()


class AddPart(AddRow):
    def __init__(self, parent=None):
        super().__init__(parent=parent, window_title='Add Part', enforce_all=True)
        df = db.get_df_unit()
        self.tablename = 'Parts'
        self.name = 'part'
        self.add_keys = ['Part Number', 'Part Name']

        self.add_input(field=InputField(text='Part Number', col_db='PartNo', enforce='no_space'))
        self.add_input(field=InputField(text='Part Name', col_db='PartName'))
        self.add_input(field=InputField(text='Model'), items=f.clean_series(df.Model))

        self.show()
