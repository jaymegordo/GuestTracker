import re
import sys
from pathlib import Path
from typing import *

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from markdown_it import MarkdownIt  # type: ignore
from PyQt6.QtCore import (
    QEvent, QItemSelectionModel, QModelIndex, QPoint, QRect, QSize, Qt,
    pyqtSignal, pyqtSlot)
from PyQt6.QtGui import QFont, QPixmap
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QButtonGroup, QCheckBox, QComboBox,
    QDateEdit, QDialog, QDialogButtonBox, QFileDialog, QFormLayout, QFrame,
    QGridLayout, QHBoxLayout, QInputDialog, QLabel, QLineEdit, QListView,
    QMessageBox, QPushButton, QRadioButton, QSizePolicy, QSlider, QSpinBox,
    QStyle, QStyleOptionTab, QStylePainter, QTabBar, QTableWidget,
    QTableWidgetItem, QTabWidget, QTextBrowser, QTextEdit, QTreeView,
    QVBoxLayout, QWidget)

from guesttracker import VERSION, IntNone, StrNone
from guesttracker import config as cf
from guesttracker import dbtransaction as dbt
from guesttracker import delta, dt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker import queries as qr
from guesttracker.database import db
from guesttracker.gui import _global as gbl
from guesttracker.gui import formfields as ff
from guesttracker.utils import dbconfig as dbc
from guesttracker.utils import fileops as fl
from jgutils import pandas_utils as pu

if TYPE_CHECKING:
    from sqlalchemy.sql.schema import Column, ForeignKey, Table

    from guesttracker.gui.tables import TableView, TableWidget
    from guesttracker.queries import Filter

log = getlog(__name__)


def add_items_to_filter(field: 'InputField', fltr: 'Filter'):
    """Add field items to query filter
    - Can be called from dialog, or table's persistent filter"""
    if field.box.isEnabled():
        if field.dtype == 'text' and field.val == '':
            return

        print(f'adding input | field={field.col_db}, table={field.table}')
        fltr.add(field=field.col_db, val=field.val, table=field.table, opr=field.opr)


class InputField():
    def __init__(
            self,
            text: str,
            col_db: Union[str, None] = None,
            box: ff.FormFields = None,
            dtype: str = 'text',
            default: Union[str, None] = None,
            table: Union[str, None] = None,
            opr: Union[Callable, None] = None,
            enforce: Union[bool, str] = False,
            like: bool = False,
            exclude_filter: bool = False,
            func: Union[Callable, None] = None):

        self.text = pu.to_title(text)
        self.name = text.replace(' ', '')

        if col_db is None:
            col_db = self.name

        self.col_db = col_db
        self.box = box
        self.dtype = dtype
        self.default = default
        self.table = table
        self.opr = opr
        self.enforce = enforce
        self.like = like
        self.exclude_filter = exclude_filter
        self.func = func
        self._box_layout = None
        self.cb = None  # type: ff.CheckBox

    @property
    def val(self) -> Any:
        val = self.box.val

        if self.dtype == 'bool':
            val = f.str_to_bool(val)

        # make any value 'like'
        if self.like and isinstance(val, str):
            val = f'*{val}*' if not val.strip() == '' else None
        elif not self.func is None:
            val = self.func(val)

        return val

    @val.setter
    def val(self, val: Any):
        self.box.val = val

    @property
    def isna(self) -> bool:
        """Check if value is null"""
        val = self.val
        if isinstance(val, str):
            return val.strip() == ''
        else:
            return bool(pd.isna(val))

    @property
    def box_layout(self) -> QHBoxLayout:
        return self._box_layout

    @box_layout.setter
    def box_layout(self, layout: QHBoxLayout):
        self._box_layout = layout

    def set_default(self):
        if not self.box is None and not self.default is None:
            self.val = self.default


class BaseDialog(QDialog):
    """Base dialog"""

    def __init__(self, parent=None, window_title='HBA Guest Tracker'):
        super().__init__(parent=parent)
        self.setWindowTitle(window_title)
        self.parent = parent
        self.mainwindow = gbl.get_mainwindow()
        self.settings = gbl.get_settings()
        # self.minesite = gbl.get_minesite()
        self.mw = self.mainwindow
        self.grid_layout = QGridLayout(self)
        self.v_layout = QVBoxLayout()
        self.grid_layout.addLayout(self.v_layout, 0, 0, 1, 1)

    def show(self):
        # NOTE should actually be max of window height/width, otherwise dialog can overflow screen
        self.setFixedSize(self.sizeHint())
        return super().show()

    def update_statusbar(self, msg, *args, **kw):
        if not self.mw is None:
            self.mw.update_statusbar(msg=msg, *args, **kw)
            self.mw.app.processEvents()


class FormLayout(QFormLayout):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        self.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)


class InputForm(BaseDialog):
    # NOTE could define these individually per RefreshTable if names overlap
    dependents = dict(
        MineSite=('Unit', 'Model', 'ModelBase', 'FCTitle'),
        Model=('Unit', 'FCTitle'),
        ModelBase=('Unit', 'FCTitle'),
        Type=('FCTitle',),
        Unit=('FCTitle',))

    # map dialog col name to df col name
    col_map = dict(FCTitle='Title')

    def __init__(
            self,
            parent: Union['TableWidget', None] = None,
            enforce_all: bool = False,
            max_combo_width: int = 300,
            use_saved_settings: bool = True,
            name: StrNone = None,
            **kw):
        super().__init__(parent=parent, **kw)

        if name is None:
            name = self.__class__.__name__

        self.name = name
        self._names_to_avoid = ('minesite_qcombobox',)  # always want to use 'current' minesite
        self._save_items = tuple()

        # NOTE could make formlayout reusable other places (eg oil samples in failure report)
        self.form_layout = FormLayout()
        self.v_layout.addLayout(self.form_layout)
        self.fields = {}  # type: Dict[str, InputField]
        self.fields_db = {}  # type: Dict[str, InputField]
        self.items = None
        self.parent = parent
        self.enforce_all = enforce_all
        self.max_combo_width = max_combo_width
        self.use_saved_settings = use_saved_settings

        add_okay_cancel(dlg=self, layout=self.v_layout)

    def exec(self):
        """Loop form fields and filter active (cb checked) fields"""
        for field in self.fields.values():
            if field.cb and field.name in self.dependents.keys() and field.cb.isChecked():
                self.cb_changed(field_name=field.name)

        if self.use_saved_settings:
            self._restore_settings()
        return super().exec()

    @property
    def existing_dependents(self) -> List[str]:
        """Return existing top level dependent cols"""
        return [name for name in self.dependents.keys() if hasattr(self, f'f{name}')]

    def dependent_on(self, field_name: str) -> List[str]:
        """Return fields which field is dependent on"""
        return [col for col, cols in self.dependents.items() if field_name in cols]

    def existing_dependent_cols(self, field_name: str) -> List[str]:
        """Get list of columns which are dependent on other cols
        - NOTE not used
        """
        lst = []
        deps = self.existing_dependents  # eg MineSite, Model, ModelBase

        for name in deps:
            field = getattr(self, f'f{name}')

            # if not same as field_name being checkd
            if not name == field_name and not field_name in self.dependents[name] and field.cb.isChecked():
                lst.extend(self.dependents[name])

        return list(set(lst))

    def cb_changed(self, field_name: str, on_toggle: bool = False) -> None:
        """Update units/models/model_base lists when minesite item changed OR toggled

        Parameters
        ----------
        field_name : str
            name of checkbox being changed/toggled
        """

        # don't adjust combobox if combobox not checked
        # only refreshtables have checkboxes with comboboxes
        field = getattr(self, f'f{field_name}')  # type: InputField
        if (field.cb and (not field.cb.isChecked() and not on_toggle)
                or not field_name in self.dependents.keys()):
            return

        val = field.val

        if not hasattr(self, 'df_filter'):
            df = db.get_df_unit()
        else:
            # FC tables need extra data to filter FCs
            df = self.df_filter

        dependent_cols = self.dependents[field_name]

        for col in dependent_cols:
            name = f'f{col}'

            if hasattr(self, name) and isinstance(getattr(self, name).box, ff.ComboBox):

                # Evaluate each field which is changing based on the active filters it is dependent on
                conds = []
                for dep_col in self.dependent_on(col):
                    if self.field_active(dep_col):

                        df_col = self.col_map.get(dep_col, dep_col)
                        cond = df[df_col] == self.fields[dep_col].val
                        conds.append(cond)

                # map display name eg "FC Title" to df name eg "Title"
                df_col = self.col_map.get(col, col)
                df1 = df[np.all(conds, axis=0)] if conds else df
                items = df1[df_col].pipe(f.clean_series)

                field_change = getattr(self, name)  # type: InputField

                prev_item = field_change.box.val
                box = field_change.box  # type: ff.ComboBox
                box.set_items(items)

                # restore val before filter if still exists
                if prev_item in items:
                    field_change.box.val = prev_item

    def add_field(self, field: InputField) -> None:
        """Add field to form layout"""
        # field already exists, remove it from layout
        if field.name in self.fields:
            field_remove = self.fields.pop(field.name)
            index = self.form_layout.removeRow(field_remove.box_layout)
            self.fields_db.pop(field_remove.col_db)

        self.fields[field.name] = field
        self.fields_db[field.col_db] = field

    def add_input(
            self,
            field: InputField,
            items: Union[List[str], None] = None,
            layout: Union[FormLayout, None] = None,
            checkbox: bool = False,
            cb_enabled: bool = True,
            index: IntNone = None,
            btn: Union[QPushButton, None] = None,
            tooltip: StrNone = None,
            cb_spacing: IntNone = None,
            enabled: bool = True,
            box_changed: Union[Callable, None] = None) -> InputField:
        """Add input field to form"""
        text, dtype = field.text, field.dtype

        if not items is None or dtype == 'combobox':
            box = ff.ComboBox(items=items)
            box.setMaximumWidth(self.max_combo_width)
        elif dtype == 'text':
            box = ff.LineEdit()
        elif dtype == 'textbox':
            box = ff.TextEdit()
            box.setMaximumSize(box.sizeHint().width(), 60)
        elif dtype == 'int':
            box = ff.SpinBox(range=(0, 200000))
        elif dtype == 'date':
            box = ff.DateEdit()
        elif dtype == 'time':
            box = ff.TimeEdit()  # NOTE not used
        elif dtype == 'datetime':
            box = ff.DateTimeEdit()
        elif dtype == 'bool':
            box = ff.ComboBox(items=['True', 'False'])
        else:
            raise ValueError(f'Incorrect dtype: {dtype}')

        box_layout = QHBoxLayout()
        box_layout.addWidget(box)
        field.box_layout = box_layout
        box.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        box.set_name(name=text)

        if dtype == 'text':
            checkbox = False
            enabled = True

        # add checkbox to form line to enable/disable field
        if checkbox:
            cb = ff.CheckBox(checked=cb_enabled)
            box.setEnabled(cb_enabled)
            cb.box = box  # attach box to toggle later
            cb.changed.connect(self.toggle)
            if cb_spacing:
                box_layout.addSpacing(cb_spacing)

            box_layout.addWidget(cb)
            field.cb = cb
            cb.set_name(name=text)
        elif btn:
            # btn.setFixedSize(btn.size)
            box_layout.addWidget(btn)
            btn.box = box
        else:
            #     # add spacer
            box.setEnabled(enabled)

        setattr(self, f'f{field.name}', field)
        field.box = box
        box.field = field
        field.set_default()

        if not box_changed is None:
            box.changed.connect(box_changed)

        if layout is None:
            layout = self.form_layout

        self.add_field(field)

        label = QLabel(f'{text}:')
        if not tooltip is None:
            label.setToolTip(tooltip)

        if index is None:
            layout.addRow(label, box_layout)
        else:
            layout.insertRow(index, label, box_layout)

        return field

    def accept_(self):
        # always access the QT base accept
        return super().accept()

    def accept(self, save_settings: bool = True):
        if not self.check_enforce_items():
            return

        self.items = self.get_items()

        if save_settings:
            self._save_settings()
        super().accept()

    @property
    def active_fields(self) -> List[InputField]:
        """Return fields with checkboxes which are checked"""
        return [field for field in self.fields.values() if hasattr(field, 'cb') and field.cb.isChecked()]

    def field_active(self, name: str) -> bool:
        """Check if field exists and is active (checked)"""
        field = self.fields.get(name, None)
        if field is None:
            return False

        cb = field.cb
        if not cb is None:
            return name in self.fields.keys() and cb.isChecked()
        else:
            # doesn't have a checkbox, always active
            return True

    def add_default_fields(self, input_type: str = 'refresh'):
        """Add all default input fields for refresh/addrows dialogs

        Parameters
        ----------
        input_type : str, optional
            default 'refresh'
        """
        table = dbt.get_table_model(table_name=self.name)

        kw = {}
        if input_type == 'refresh':
            kw['checkbox'] = True
            kw['cb_enabled'] = False
            exclude_check_field = 'exclude_refresh_fields'
            enforce_fields = []
        elif input_type == 'update':
            enforce_fields = dbc.table_data[self.name].get('add_enforce', [])
            exclude_check_field = 'exclude_add_fields'
        else:
            raise ValueError(f'Incorrect input_type: {input_type}')

        for col in table.columns:
            exclude = ['uid'] + dbc.table_data[self.name].get(exclude_check_field, [])
            col = col  # type: Column
            text = col.name

            # check if col has a foreign key
            fks = list(col.foreign_keys)
            if fks:
                # assume only one fk for now
                fk = fks[0]  # type: ForeignKey
                col_rel = fk.column
                table_rel = fk.column.table  # type: Table

                # use "name" column as column to add filter for instead of uid
                text = col.name.replace('_id', '_name')
                col = table_rel.columns.get('name', None)  # type: Column

            # add filter for column
            if not col is None and not col.name in exclude:
                self.add_input(
                    field=InputField(
                        text=pu.to_title(text),
                        col_db=col.name,
                        table=col.table.name,
                        dtype=dbt.get_dtype_map(col),
                        enforce=input_type == 'update' and col.name in enforce_fields),
                    **kw)

    def check_enforce_items(self):
        """Loop all enforceable fields, make sure vals arent blank/default"""
        if self.enforce_all:
            no_blank = self.fields.values()
        else:
            no_blank = list(filter(
                lambda field: (
                    field.enforce is True and
                    isinstance(field.box.val, str) and
                    field.box.isEnabled()), self.fields.values()))

        # enforce no spaces in field name
        no_spaces = [field for field in self.fields.values() if field.enforce == 'no_space']

        # (list of field items, failure message, condition fail func)
        conds = [
            (no_blank, 'cannont be blank', lambda x: len(x) == 0),
            (no_spaces, 'cannot have spaces', lambda x: ' ' in x)
        ]

        # for field in fields:
        for lst, fail_msg, cond in conds:
            for field in lst:
                if cond(field.val):
                    msg = f'"{field.text}" {fail_msg}.'
                    dlg = msg_simple(msg=msg, icon='warning')
                    field.box.select_all()
                    return False

        return True

    def get_items(self, lower: bool = False) -> Dict[str, Any]:
        """Return dict of all {field items: values}"""
        m = {}
        for field in self.fields.values():
            m[field.text] = field.val

        if lower:
            m = {k.lower(): v for k, v in m.items()}

        return m

    def _add_items_to_filter(self):
        # loop params, add all to parent filter
        fltr = self.parent.query.fltr
        for field in self.fields.values():
            if not field.exclude_filter:
                add_items_to_filter(field=field, fltr=fltr)

    def toggle(self, state: int) -> None:
        """Toggle input field enabled/disabled based on checkbox

        - This can be overridden in child class (eg RefreshTable) to perform extra toggle actions
        """
        source = self.sender()
        box = source.box

        if Qt.CheckState(state) == Qt.CheckState.Checked:
            box.setEnabled(True)
            box.select_all()
        else:
            box.setEnabled(False)

    def insert_linesep(self, i=0, layout_type='form'):
        line_sep = create_linesep(self)

        if layout_type == 'form':
            self.form_layout.insertRow(i, line_sep)

        else:
            self.v_layout.insertWidget(i, line_sep)

    @classmethod
    def _get_handled_types(cls):
        return QComboBox, QLineEdit, QTextEdit, QCheckBox, QRadioButton, QSpinBox, QSlider, QDateEdit

    @classmethod
    def _is_handled_type(cls, widget):
        return any(isinstance(widget, t) for t in cls._get_handled_types())

    def _save_settings(self):
        """Save ui controls and values to QSettings"""
        for obj in self.children():
            name = obj.objectName()

            if self._is_handled_type(obj) or name in self._save_items:
                val = obj.val

                if not val is None:
                    self.settings.setValue(f'dlg_{self.name}_{name}', val)

    def _restore_settings(self):
        """Set saved value back to ui control"""
        for obj in self.children():
            name, val = obj.objectName(), None

            if \
                ((self._is_handled_type(obj) and
                  not name in self._names_to_avoid and
                  not len(name) == 0) or
                 name in self._save_items):

                val = self.settings.value(f'dlg_{self.name}_{name}')
                if not val is None:
                    obj.val = val


class InputUserName(InputForm):
    def __init__(self, parent=None):
        super().__init__(parent=parent, window_title='Enter User Name', enforce_all=True)
        layout = self.v_layout
        layout.insertWidget(0, QLabel('Welcome to the HBA Guest Tracker! \
            \nPlease enter your first/last name and work email to begin:\n'))

        self.add_input(field=InputField(text='First'))
        self.add_input(field=InputField(text='Last'))
        self.add_input(field=InputField(text='Email'))

        self.show()

    def accept(self):
        self.username = '{} {}'.format(self.fFirst.val, self.fLast.val).title()
        self.email = self.fEmail.val.lower()
        super().accept()


class CustomFC(InputForm):
    def __init__(self, **kw):
        super().__init__(window_title='Create Custom FC')
        add, IPF = self.add_input, InputField

        text = 'Enter list or range of units to create a custom FC.\n\
        \nRanges must be defined by two dashes "--", Eg)\nPrefix: F\nUnits: 302--310, 312, 315--317\n\n'
        label = QLabel(text)
        label.setMaximumWidth(300)
        label.setWordWrap(True)
        self.v_layout.insertWidget(0, label)

        add(field=IPF(text='FC Number', enforce=True))
        add(field=IPF(text='Prefix'))
        add(field=IPF(text='Units', enforce=True))
        add(field=IPF(text='Subject', enforce=True))
        add(field=IPF(text='Type'), items=['M', 'FAF', 'DO', 'FT'])
        add(field=IPF(text='Release Date', dtype='date', col_db='ReleaseDate'))
        add(field=IPF(text='Expiry Date', dtype='date', col_db='ExpiryDate'))

    def accept(self):
        if not self.check_enforce_items():
            return

        from guesttracker.data import factorycampaign as fc
        units = fc.parse_units(units=self.fUnits.val, prefix=self.fPrefix.val)
        self.units = units

        # units have incorrect input
        if not units:
            return

        units_bad = db.units_not_in_db(units=units)
        if units_bad:
            # tried with units which don't exist in db
            msg = f'The following units do not exist in the database. Please add them then try again:\n{units_bad}'
            msg_simple(msg=msg, icon='warning')
            return

        fc_number = self.fFCNumber.val
        lst_units = '\n'.join(units)
        msg = f'Create new FC "{fc_number}"?\n\nUnits:\n{lst_units}'
        if not msgbox(msg=msg, yesno=True):
            return

        super().accept(save_settings=False)


class ChangeMinesite(InputForm):
    def __init__(self, parent=None):
        super().__init__(parent=parent, window_title='Change MineSite')
        lst = db.get_list_minesite()
        self.add_input(field=InputField('MineSite', default=self.minesite), items=lst) \
            .box.select_all()

        self.show()

    def accept(self):
        super().accept()
        if not self.parent is None:
            self.parent.minesite = self.fMineSite.val


class ComponentCO(InputForm):
    def __init__(self, parent=None, window_title='Select Component'):
        super().__init__(parent=parent, window_title=window_title)

        df = db.get_df_component()

        # filter components to unit based on equip_class
        if not parent is None:
            e = self.parent.e
            equip_class = db.get_unit_val(unit=e.Unit, field='EquipClass')
            df = df[df.EquipClass == equip_class]

        lst = f.clean_series(s=df.Combined)
        self.add_input(field=InputField('Component'), items=lst) \
            .box.select_all()

        f.set_self(vars())

    def accept(self):
        df, table_widget = self.df, self.parent
        val = self.fComponent.val
        floc = df[df.Combined == val].Floc.values[0]

        if not table_widget is None:
            # only need to update floc in database
            row = table_widget.row
            row.update(vals=dict(Floc=floc, ComponentCO=True))
            table_widget.update_statusbar(msg=f'Component updated: {val}')

        super().accept()


class TracebackTextEdit(QTextEdit):
    """QTextEdit to display python tracebacks
    """

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.setReadOnly(True)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def set_traceback(self, text: str) -> None:
        """Convert traceback raw text to colored/formatted html

        Parameters
        ----------
        text : str
            traceback text
        """
        import pygments.lexers
        from pygments.formatters import HtmlFormatter

        lexer = pygments.lexers.get_lexer_by_name('pytb', stripall=True)

        formatter = HtmlFormatter(
            style='jayme',
            nobackground=True,
            noclasses=True,
            cssstyles=None,
            prestyles='font-family: Calibri',
            wrapcode=False)

        tb_html = pygments.highlight(text, lexer, formatter)

        self.insertHtml(tb_html)

    def sizeHint(self) -> QSize:
        """Resize QTextEdit to html document width/height"""
        size = self.document().size()
        width = size.width() + self.verticalScrollBar().width() + 8
        height = size.height() + self.horizontalScrollBar().height() + 8
        return QSize(width, height)


class ErrMsg(QDialog):
    def __init__(self, text: str = 'ERROR', tb_text: str = None, *args, **kw):
        super().__init__(*args, **kw)
        self.setWindowTitle('Error')

        layout = QVBoxLayout(self)

        label = QLabel(text, self)
        label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        label.setWordWrap(True)
        layout.addWidget(label)

        self.btn_show = QPushButton('Show Error Details', self)
        self.btn_show.clicked.connect(self.show_hide_tb)

        # button to copy tb text for issue
        btn_copy = QPushButton('Copy Text', self)
        btn_copy.clicked.connect(self.copy_text)
        btn_copy.setToolTip('Copy error traceback for submitting bug report.')

        # oppen create new issue link in browser
        btn_issue = QPushButton('Submit Bug', self)
        btn_issue.clicked.connect(lambda: f.open_url(cf.config['url']['issues']))
        btn_issue.setToolTip('Open github issues link in web browser to submit bug report.')

        # use stretch w button box so buttons stay on left
        hbox = QHBoxLayout()
        btnbox = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        btnbox.addButton(self.btn_show, QDialogButtonBox.ButtonRole.ActionRole)
        btnbox.addButton(btn_copy, QDialogButtonBox.ButtonRole.ActionRole)
        btnbox.addButton(btn_issue, QDialogButtonBox.ButtonRole.ActionRole)
        hbox.addWidget(btnbox)
        hbox.addStretch(1)

        btnbox.accepted.connect(self.accept)

        layout.addLayout(hbox)
        self.layout = layout  # type: QVBoxLayout
        self.tb_init = False
        self.tb_visible = False

        # statusbar
        self.statusbar = QLabel('', self)
        self.statusbar.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.statusbar.setVisible(False)
        layout.addWidget(self.statusbar)

        f.set_self(vars())

    def copy_text(self) -> None:
        """Copy traceback text to clipboard"""
        app = gbl.get_qt_app()
        app.clipboard().setText(self.tb_text)
        self.statusbar.setText('Traceback copied to clipboard.')
        self.statusbar.setVisible(True)
        self.adjustSize()

    def init_tb(self, text: str) -> None:
        textedit = TracebackTextEdit()
        textedit.set_traceback(text)
        # textedit.verticalScrollBar().setMinimumHeight(600)
        # textedit.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)

        # make textedit expand vertical/horizontal when dialog size changed
        # textedit.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        # would work if the traceback wasnt wrapped in <pre>
        # doc = textedit.document()
        # textedit.setLineWidth(doc.size().width())

        # need show() to init sizeHint
        textedit.show()
        self.textedit_tb = textedit
        self.layout.insertWidget(2, textedit)
        self.tb_init = True

    def show_hide_tb(self) -> None:
        """Show/Hide error tb QTextEdit"""
        if not self.tb_init:
            self.init_tb(text=self.tb_text)

        # hide/show textedit
        self.textedit_tb.setVisible(not self.tb_visible)

        # Change text when button toggled
        self.tb_visible = not self.tb_visible
        text = 'Hide' if self.tb_visible else 'Show'
        text = f'{text} Error Details'
        self.btn_show.setText(text)

        self.adjustSize()


class MsgBoxAdvanced(QDialog):
    """More customizeable messagebox
    - TODO need to make this display a dataframe better
    """

    def __init__(
            self,
            msg: str = '',
            window_title: str = '',
            yesno: bool = False,
            statusmsg: str = None,
            min_width: int = 200,
            max_width: int = 1000,
            markdown_msg: str = None,
            parent=None):
        super().__init__(parent=parent)
        self.setWindowTitle(window_title)
        self.setMinimumSize(gbl.minsize)
        self.setMinimumWidth(min_width)
        self.setMaximumWidth(max_width)

        layout = QVBoxLayout(self)

        label = QLabel(msg, self)
        label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        label.setWordWrap(True)
        layout.addWidget(label)

        # used to set changelog
        if not markdown_msg is None:
            label_markdown = HTMLTextEdit(msg=markdown_msg, min_width=800, min_height=600)
            layout.addWidget(label_markdown)

        if not yesno:
            btn = QPushButton('Okay', self)
            btn.setMaximumWidth(100)
        else:
            btn = QDialogButtonBox(QDialogButtonBox.StandardButton.Yes | QDialogButtonBox.StandardButton.No, self)
            btn.accepted.connect(self.accept)
            btn.rejected.connect(self.reject)

        btn.clicked.connect(self.close)

        statusbar = QLabel(statusmsg, self)
        statusbar.setAlignment(Qt.AlignmentFlag.AlignLeft)

        hLayout = QHBoxLayout()
        hLayout.addWidget(statusbar)
        hLayout.addWidget(btn, alignment=Qt.AlignmentFlag.AlignRight)
        layout.addLayout(hLayout)


class HTMLTextEdit(QTextBrowser):
    def __init__(
            self,
            msg: str,
            min_width: int = 800,
            min_height: int = 600,
            *args, **kw):
        super().__init__(*args, **kw)
        self.setTextInteractionFlags(Qt.TextInteractionFlag.LinksAccessibleByMouse)
        self.setOpenExternalLinks(True)
        self.setMinimumWidth(800)
        # self.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)  # doesn't work

        # QTextEdit height controlled by verticalScrollBar()
        self.verticalScrollBar().setMinimumHeight(600)
        color = cf.config['color']['bg']

        self.document().setDefaultStyleSheet(f'\
                code {{color : {color["tan"]}; font-size: 14px; font-family: Calibri}}\
                li {{margin-bottom : 5px}}\
                a:link {{color: {color["medblue"]}}}')

        try:
            # use markdown-it-py to make a cleaner conversion to html for better styling
            md = MarkdownIt()
            tokens = md.parse(msg)
            html_text = md.render(msg)
            self.setHtml(html_text)
        except Exception as e:
            # just in case
            log.warning(f'Failed to parse markdown to html with MarkdownIt:\n\t{e}')
            self.setMarkdown(msg)


class DetailsView(QDialog):
    def __init__(self, parent=None, df=None):
        super().__init__(parent)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setWindowTitle('Details View')
        self.setMinimumSize(QSize(800, 1000))
        tbl = self.create_table(df=df)
        v_layout = QVBoxLayout(self)
        v_layout.addWidget(tbl)

        add_okay_cancel(dlg=self, layout=v_layout)

        f.set_self(vars())

    def create_table(self, df):
        tbl = QTableWidget()
        # tbl.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
        tbl.setFixedSize(QSize(800, 1000))
        tbl.setColumnWidth(0, 200)

        tbl.setRowCount(df.shape[0])
        tbl.setColumnCount(df.shape[1])
        tbl.setHorizontalHeaderLabels(list(df.columns))
        tbl.setVerticalHeaderLabels(list(df.index))
        tbl.horizontalHeader().setStretchLastSection(True)

        df_array = df.values
        for row in range(df.shape[0]):
            for col in range(df.shape[1]):
                val = df_array[row, col]
                val = str(val) if not val is None else ''
                tbl.setItem(row, col, QTableWidgetItem(val))

        tbl.resizeRowsToContents()

        tbl.cellChanged.connect(self.onCellChanged)
        return tbl

    @pyqtSlot(int, int)
    def onCellChanged(self, irow, icol):
        gbl.check_read_only()

        df, parent = self.df, self.parent
        val = self.tbl.item(irow, icol).text()
        row, col = df.index[irow], df.columns[icol]

        # update database
        dbtable = parent.get_dbtable(header=row)  # transposed table
        db_row = dbt.Row(df=df, col='Value', dbtable=dbtable, title=parent.title)

        if f.isnum(val):
            val = float(val)

        db_row.update_single(val=val, header=row)


class FailureReport(BaseDialog):
    """
    Dialog to select pictures, and set cause/correction text to create pdf failure report.
    """

    def __init__(
            self,
            parent: QWidget = None,
            p_start: Path = None,
            text: dict = None,
            unit: str = None,
            e: dbt.SQLAQuery = None):
        super().__init__(parent=parent, window_title='Create Failure Report')
        # self.resize(QSize(800, 1000))
        self.setSizeGripEnabled(True)
        v_layout = self.v_layout
        self.parent = parent

        text_fields = {}
        if text is None:
            text = {}  # default text for text fields

        if p_start is None:
            p_start = cf.desktop
        elif not p_start.exists():
            self.update_statusbar(f'Couldn\'t find event images path: {p_start}', warn=True)
            p_start = cf.desktop

        # create file dialog to select images
        file_dlg = FileDialogPreview(directory=p_start, standalone=False, parent=self)
        v_layout.addWidget(file_dlg)
        add_linesep(v_layout)

        # word/pdf radio buttons
        bg1 = QButtonGroup(self)
        btn_pdf = QRadioButton('PDF', self)
        check_pdf = gbl.get_setting('failure_report_btn_pdf', True)  # load from settings
        btn_pdf.setChecked(check_pdf)
        btn_word = QRadioButton('Word', self)
        btn_word.setChecked(not check_pdf)
        bg1.addButton(btn_pdf)
        bg1.addButton(btn_word)

        f.set_self(vars())
        names = ['complaint', 'cause', 'correction', 'details']
        self.add_textbox(names=names)
        add_linesep(v_layout)

        # oil samples

        oil_form = QFormLayout()
        oil_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        oil_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)

        oil_layout_comp = QHBoxLayout()
        oil_box = ff.ComboBox()
        oil_box.setFixedSize(QSize(300, oil_box.sizeHint().height()))
        oil_cb = ff.CheckBox(checked=False)
        oil_cb.stateChanged.connect(self.toggle_oil_components)
        oil_box.setEnabled(False)
        oil_layout_comp.addWidget(oil_cb)
        oil_layout_comp.addWidget(oil_box)
        # oil_layout_comp.addStretch(1)
        oil_form.addRow(QLabel('Component:'), oil_layout_comp)

        # oil date
        oil_layout_date = QHBoxLayout()
        d_lower = self.e.DateAdded + delta(days=-365)
        oil_date = ff.DateEdit(date=d_lower, enabled=False)
        oil_date_cb = ff.CheckBox(checked=False, enabled=False)
        oil_date_cb.stateChanged.connect(self.toggle_oil_date)
        oil_layout_date.addWidget(oil_date_cb)
        oil_layout_date.addWidget(oil_date)
        # oil_layout_date.addStretch(1)
        oil_form.addRow(QLabel('Date Lower:'), oil_layout_date)

        oil_h_layout = QHBoxLayout()
        oil_h_layout.addLayout(oil_form)
        oil_h_layout.addStretch(1)

        v_layout.addWidget(QLabel('Oil Samples'))
        v_layout.addLayout(oil_h_layout)
        # v_layout.addLayout(oil_layout)
        # v_layout.addLayout(oil_layout_date)
        add_linesep(v_layout)

        # PLM - NOTE lots of duplication here (I'm lazy)
        plm_form = QFormLayout()
        plm_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        plm_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.FieldsStayAtSizeHint)

        plm_cb = ff.CheckBox(checked=False)
        # plm_cb.setFixedSize(QSize(300, plm_cb.sizeHint().height()))
        plm_cb.stateChanged.connect(self.toggle_plm)
        plm_form.addRow(QLabel('PLM Report:'), plm_cb)
        plm_date_lower = ff.DateEdit(date=d_lower, enabled=False)
        plm_date_upper = ff.DateEdit(date=self.e.DateAdded, enabled=False)
        plm_form.addRow(QLabel('Date Lower:'), plm_date_lower)
        plm_form.addRow(QLabel('Date Upper:'), plm_date_upper)
        v_layout.addLayout(plm_form)
        add_linesep(v_layout)

        btn_layout = QHBoxLayout()
        btn_layout.addWidget(QLabel('Report type:'))
        btn_layout.addWidget(btn_pdf)
        btn_layout.addWidget(btn_word)
        btn_layout.addStretch(1)
        v_layout.addLayout(btn_layout)

        add_okay_cancel(dlg=self, layout=v_layout)
        f.set_self(vars())

        # TODO Faults?

    @classmethod
    def example(cls, uid: int = 163093319526):
        from guesttracker import eventfolders as efl
        e = dbt.Row.example(uid=uid)
        ef = efl.EventFolder.example(e=e)
        dlg = cls(p_start=ef.p_pics, unit=e.Unit, e=e)
        dlg.exec()
        return dlg

    def toggle_oil_components(self, state):
        """Toggle components when oil cb checked"""
        oil_box = self.oil_box

        if Qt.CheckState(state) == Qt.CheckState.Checked:
            oil_box.setEnabled(True)
            self.oil_date_cb.setEnabled(True)
            # self.oil_date.setEnabled(True)

            df_comp = db.get_df_oil_components(unit=self.unit)
            items = f.clean_series(df_comp.combined)
            oil_box.set_items(items)
            oil_box.select_all()
        else:
            oil_box.setEnabled(False)
            self.oil_date_cb.setEnabled(False)
            self.oil_date_cb.setChecked(False)
            # self.oil_date.setEnabled(False)

    def toggle_plm(self, state):
        """Toggle plm dates when plm cb checked"""
        box = self.plm_cb

        if Qt.CheckState(state) == Qt.CheckState.Checked:
            box.setEnabled(True)
            self.plm_date_lower.setEnabled(True)
            self.plm_date_upper.setEnabled(True)
        else:
            self.plm_date_lower.setEnabled(False)
            self.plm_date_upper.setEnabled(False)

    def toggle_oil_date(self, state):
        """Toggle components when oil cb checked"""
        oil_date = self.oil_date

        if Qt.CheckState(state) == Qt.CheckState.Checked:
            oil_date.setEnabled(True)
        else:
            oil_date.setEnabled(False)

    def add_textbox(self, names):
        def _add_textbox(name):
            layout = QVBoxLayout()
            layout.addWidget(QLabel(f'{name.title()}:'))

            textbox = QTextEdit()
            textbox.setText(self.text.get(name, ''))

            setattr(self, name, textbox)
            self.text_fields[name] = textbox
            layout.addWidget(textbox)
            self.v_layout.addLayout(layout)

        if not isinstance(names, list):
            names = [names]
        for name in names:
            _add_textbox(name)

    def accept(self):
        pics = self.file_dlg.selectedFiles()
        word_report = self.btn_word.isChecked()

        # save PDF/Word selection
        gbl.get_settings().setValue('failure_report_btn_pdf', not word_report)

        # convert dict of textbox objects to their plaintext (could also use html)
        for name, textbox in self.text_fields.items():
            self.text[name] = textbox.toPlainText()

        # save oil sample component/modifier
        oil_samples = False
        if self.oil_cb.isChecked():
            oil_samples = True
            val = self.oil_box.val
            lst = val.split(' - ')
            component = lst[0]

            # may or may not have modifier
            if len(lst) > 1:
                modifier = lst[1]
            else:
                modifier = None

            if self.oil_date_cb.isChecked():
                d_lower = self.oil_date.val
            else:
                d_lower = None

        plm_report = False
        if self.plm_cb.isChecked():
            plm_report = True

            # check if PLM needs to be updated first
            from guesttracker.data.internal import plm
            unit = self.unit
            maxdate = plm.max_date_plm(unit=unit)

            if (maxdate + delta(days=-5)).date() < self.e.DateAdded:
                msg = f'Max date in db: {maxdate:%Y-%m-%d}. ' \
                    + 'Importing haul cylce files from network drive, this may take a few minutes...'
                self.update_statusbar(msg=msg)

                plm.update_plm_single_unit(unit=unit, maxdate=maxdate)

        f.set_self(vars())
        super().accept()


class FileDialogPreview(QFileDialog):
    """
    Create QFileDialog with image preview
    """

    def __init__(
            self,
            parent: QWidget = None,
            caption: str = '',
            directory: Union[Path, str] = None,
            filter: str = None,
            standalone: bool = True,
            options=QFileDialog.Option.DontUseNativeDialog,
            **kw):
        super().__init__(parent, caption, str(directory), filter, options=options, **kw)
        box = QHBoxLayout()
        if not standalone:
            self.disable_buttons()

        self.setFixedSize(self.width() + 400, self.height() - 100)
        self.setFileMode(QFileDialog.FileMode.ExistingFiles)
        self.setViewMode(QFileDialog.ViewMode.Detail)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.Dialog)  # needed to use inside other dialog
        self.setSizeGripEnabled(False)

        mpPreview = QLabel('Preview', self)
        mpPreview.setFixedSize(400, 400)
        mpPreview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        mpPreview.setObjectName('labelPreview')
        box.addWidget(mpPreview)
        box.addStretch()  # not sure if necessary

        # Add extra column to FileDialog's gridLayout for image preview
        # row=0, column=3, rowSpan=4, colSpan=1
        self.layout().addLayout(box, 0, 3, 4, 1)

        self.currentChanged.connect(self.onChange)
        self.fileSelected.connect(self.onFileSelected)
        self.filesSelected.connect(self.onFilesSelected)

        # used to change picture on hover changed, to messy, dont need
        # for view in self.findChildren(QTreeView):
        #     if isinstance(view.data_model, QFileSystemModel):
        #         tree_view = view
        #         break

        # tree_view.setMouseTracking(True)
        # tree_view.entered.connect(self.onChange)

        self._fileSelected = None
        self._filesSelected = None
        f.set_self(vars())

        # close FailureReport dialog when esc pressed
        if not parent is None:
            self.rejected.connect(parent.close)

        self.select_files()

    def select_files(self, expr: str = r'^\d+$') -> None:
        """Select files where file name (excluding .ext) match regex pattern

        Parameters
        ----------
        expr : str, optional
            regex expression, by default r'^\d+$' (match only digits)
        """
        _p = Path(self.directory)

        sel_files = [p for p in _p.iterdir() if re.match(expr, p.stem)]

        # get list view
        file_view = self.findChild(QListView, 'listView')

        # get selection model
        sel_model = file_view.selectionModel()

        for p in sel_files:
            idx = sel_model.model().index(str(p))
            sel_model.select(idx, QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)

        self.file_view = file_view

    def disable_buttons(self):
        # remove okay/cancel buttons from dialog, when showing in another dialog
        btn_box = self.findChild(QDialogButtonBox)
        if btn_box:
            btn_box.hide()
            # self.layout().removeWidget(btn_box) # this stopped working for some reason

    def onChange(self, *args):
        if not args:
            return
        else:
            arg = args[0]

        if isinstance(arg, QModelIndex):
            index = arg
            if not index.column() == 0:
                index = index.siblingAtColumn(0)
            path = str(Path(self.directory) / index.data())
        else:
            path = arg

        pixmap = QPixmap(path)
        mpPreview = self.mpPreview

        if(pixmap.isNull()):
            mpPreview.setText('Preview')
        else:
            mpPreview.setPixmap(pixmap.scaled(mpPreview.width(), mpPreview.height(),
                                Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))

    def accept(self):
        # prevent window from being closed when part of a form
        if self.standalone:
            super().accept()

    def onFileSelected(self, file):
        self._fileSelected = file

    def onFilesSelected(self, files):
        self._filesSelected = files

    def getFileSelected(self):
        return self._fileSelected

    def getFilesSelected(self):
        return self._filesSelected


class Search(BaseDialog):
    index_changed = pyqtSignal(int)

    def __init__(self, parent: 'TableView' = None):
        super().__init__(parent, window_title='Search')
        # self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)

        self.index_changed.connect(self.select)
        items = []  # list of match items
        # parent should be view?
        self.view = parent
        self.model = self.view.data_model
        self.model.highlight_rows = False  # turn off row highlighting so we can see single selection

        label_matches = QLabel('Matches:')
        search_box = QLineEdit()
        self.meta_state = False
        search_box.textChanged.connect(self.text_changed)
        search_box.installEventFilter(self)
        self.v_layout.addWidget(search_box)
        self.v_layout.addWidget(label_matches)

        # cancel, prev, next
        prev = QPushButton('Prev')
        next_ = QPushButton('Next')
        prev.clicked.connect(self.find_prev)
        next_.clicked.connect(self.find_next)
        prev.setToolTip('Ctrl + Left Arrow')
        next_.setToolTip('Ctrl + Right Arrow | Enter')

        btnbox = QDialogButtonBox(QDialogButtonBox.StandardButton.Cancel)
        btnbox.addButton(prev, QDialogButtonBox.ButtonRole.ActionRole)
        btnbox.addButton(next_, QDialogButtonBox.ButtonRole.ActionRole)

        btnbox.rejected.connect(self.reject)
        self.rejected.connect(self.close)  # need to trigger close event to reset selection

        self.v_layout.addWidget(btnbox, alignment=Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignCenter)

        f.set_self(vars())

    def closeEvent(self, event):
        self.model.highlight_rows = True

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.KeyPress:
            mod = event.modifiers()
            key = event.key()

            # print(keyevent_to_string(event))
            if mod and (
                (cf.is_win and mod == Qt.KeyboardModifier.ControlModifier) or
                mod == Qt.KeyboardModifier.AltModifier or
                mod == Qt.KeyboardModifier.MetaModifier or
                mod == (Qt.KeyboardModifier.MetaModifier | Qt.KeyboardModifier.KeypadModifier) or
                    mod == (Qt.KeyboardModifier.AltModifier | Qt.KeyboardModifier.KeypadModifier)):

                if key == Qt.Key.Key_Right:
                    self.find_next()
                    return True
                elif key == Qt.Key.Key_Left:
                    self.find_prev()
                    return True

            elif key == Qt.Key.Key_Enter or key == Qt.Key.Key_Return:
                self.find_next()
                return True

        return super().eventFilter(obj, event)

    def select(self, i: int):
        """Call view to select, pass tuple of name index"""
        self.current_index = i
        if self.items:
            self.view.select_by_nameindex(self.items[i])
        else:
            i = -1  # no matches, select 0/0

        self.label_matches.setText(f'Selected: {i + 1}/{self.num_items}')

    def text_changed(self):
        search_box = self.search_box
        text = search_box.text()

        # get list of match items from model
        self.items = self.model.search(text)
        self.num_items = len(self.items)

        self.index_changed.emit(0)

    def find_next(self):
        i = self.current_index
        i += 1
        if i > self.num_items - 1:
            i = 0

        self.index_changed.emit(i)

    def find_prev(self):
        i = self.current_index
        i -= 1
        if i < 0:
            i = self.num_items - 1

        self.index_changed.emit(i)


class PLMReport(InputForm):
    def __init__(self, unit: str = None, d_upper: dt = None, d_lower: dt = None, **kw):
        super().__init__(window_title='PLM Report', use_saved_settings=False, **kw)
        # unit, start date, end date
        IPF = InputField

        if pd.isna(d_upper):
            d_upper = dt.now().date()

        if d_lower is None and not d_upper is None:
            d_lower = d_upper + relativedelta(years=-1)

        # Unit
        df = db.get_df_unit()
        lst = f.clean_series(df[df.MineSite == self.minesite].Unit)
        self.add_input(field=IPF(text='Unit', default=unit), items=lst)

        # Dates
        dates = {'Date Upper': d_upper, 'Date Lower': d_lower}
        for k, v in dates.items():
            self.add_input(
                field=IPF(
                    text=k,
                    default=v,
                    dtype='date'))


class BaseReportDialog(InputForm):
    """Report MineSite/Month period selector dialog for monthly reports"""

    def __init__(self, **kw):
        super().__init__(**kw)

        self.add_input(field=InputField(text='MineSite', default=self.minesite),
                       items=db.get_list_minesite(include_custom=False))

        df = qr.df_rolling_n_months(n=12)
        months = df.period.to_list()[::-1]
        self.add_input(field=InputField(text='Month', default=months[0]), items=months)

        f.set_self(vars())

    def accept(self):
        """Set day based on selected month"""
        period = self.fMonth.val
        self.d = self.df.loc[period, 'd_lower']

        super().accept()


class VerticalTabs(QTabWidget):
    """Vertical tab widget"""

    class TabBar(QTabBar):
        """Horizontal text for vertical tabs"""

        def tabSizeHint(self, index):
            s = QTabBar.tabSizeHint(self, index)
            s.transpose()

            # make tab label width slightly wider
            return QSize(s.width() + 20, s.height())

        def paintEvent(self, event):
            """
            NOTE - this seems to mess up custom qdarkstyle QTabBar::tab:left,
            when any custom css uses borders/margins etc
            """
            painter = QStylePainter(self)
            opt = QStyleOptionTab()

            for i in range(self.count()):
                self.initStyleOption(opt, i)
                painter.drawControl(QStyle.ControlElement.CE_TabBarTabShape, opt)
                painter.save()

                s = opt.rect.size()
                s.transpose()
                r = QRect(QPoint(), s)
                r.moveCenter(opt.rect.center())
                opt.rect = r

                c = self.tabRect(i).center()
                painter.translate(c)
                painter.rotate(90)
                painter.translate(-c)
                painter.drawControl(QStyle.ControlElement.CE_TabBarTabLabel, opt)
                painter.restore()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTabBar(self.TabBar(self))
        self.setTabPosition(QTabWidget.TabPosition.West)


class Preferences(BaseDialog):
    """Global user preferences dialog"""
    input_types = {
        bool: ff.CheckBox,
        list: ff.ComboBox,
        str: ff.LineEdit}

    def __init__(self, parent=None):
        super().__init__(parent, window_title='Preferences')

        self.resize(QSize(800, 600))
        self.s = gbl.get_settings()

        # apply settings changes and funcs on accept
        self.queued_changes = {}
        self.queued_funcs = {}

        # limit available tabs
        from guesttracker.gui.gui import TabWidget
        user = self.mw.u if not parent is None else None
        available_tabs = TabWidget.available_tabs(user=user)

        # TODO enforce values?
        settings_conf = dict(
            General=dict(
                username=dict(default=''),
                email=dict(default=''),
                read_only=dict(
                    default=False,
                    tooltip='Prevent app from updating any values in database.'),
                # minesite=dict(
                #     default='FortHills',
                #     items=db.get_list_minesite()),
                # custom_minesites=dict(
                #     cls=ff.MultiSelectList,
                #     items=db.get_list_minesite(include_custom=False),
                #     tooltip='Define list of multiple minesites to be returned in queries'
                #     + ' when current minesite = "CustomSites".'),
            ),
            Appearance=dict(
                font_size=dict(
                    default=cf.config_platform['font size'],
                    items=list(range(8, 17)),
                    tooltip='IMPORTANT: Must restart app for font size change to take effect.'
                ),
                visible_tabs=dict(
                    cls=ff.MultiSelectList,
                    items=available_tabs,
                    tooltip='Define list of visible tabs. Must restart app for changes to take effect.'),
            ),
            # Events=dict(
            #     close_wo_with_event=dict(
            #         default=False,
            #         label='Close Work Order with Event',
            #         tooltip='Set WO status to "Closed" when Event is closed and vice versa.'),
            #     wo_request_folder=dict(
            #         default='WO Request',
            #         label='WO Request Email Folder',
            #         tooltip='Specify the folder in your Outlook app to search for new WO request emails.'
            #         + ' (Case insensitive).'
            #     )
            # ),
            # TSI=dict(
            #     open_downloads_folder=dict(
            #         default=False,
            #         tooltip='Open downloads folder in addition to event folder on "View Folder" command.'),
            #     save_tsi_pdf=dict(
            #         default=False,
            #         label='Save TSI PDF',
            #         tooltip='Auto save PDF of TSI to event folder after created.'
            #         )
            # ),
            Advanced=dict(
                # dev_channel=dict(
                #     default=False,
                #     label='Alpha Update Channel',
                #     tooltip='Get alpha updates (potentially unstable).'),
                # is_admin=dict(
                #     default=False,
                #     label='Owner',
                #     tooltip='Set user to owner status to unlock specific tabs.'),
                show_query_time=dict(
                    default=False,
                    tooltip='Show detailed query/display time in statusbar. Useful for troubleshooting slow queries.'
                )
            ),
        )

        # cant update mw in testing
        if not parent is None:
            # settings_conf['General']['minesite']['funcs'] = self.mw.update_minesite_label
            settings_conf['Appearance']['font_size']['funcs'] = gbl.set_font_size

        tabs = VerticalTabs(self)

        for tab_name, vals in settings_conf.items():
            tab = QWidget()
            form_layout = FormLayout(tab)
            tabs.addTab(tab, tab_name)
            # form_layout.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

            for name, config in vals.items():
                self.add_setting(key=name, config=config, layout=form_layout)

        self.v_layout.addWidget(tabs)
        add_okay_cancel(self, self.v_layout)

    def add_setting(self, key: str, config: dict, layout: QFormLayout) -> None:
        """Add single inputbox, connect to settings key, add to tab layout

        Parameters
        ----------
        key : str
            string key to access global setting
        config : dict
            settings to init specific input box with
        layout : QFormLayout
            form layout
        """

        # pass in specific text if reqd
        label_text = config.get('label', key.replace('_', ' ').title())
        label = QLabel(f'{label_text}:')
        label.setFixedWidth(150)
        label.setWordWrap(True)

        # get input box type from type of default arg
        # set box current value
        default = config.get('default', None)
        items = config.get('items', None)
        cls = config.get('cls', None)
        val = gbl.get_setting(key, default)
        kw = dict(key=key, val=val)

        # combobox needs to set default items
        if not items is None:
            _type = list
            kw |= dict(items=items)
        else:
            _type = type(default)

        if cls is None:
            InputBox = self.input_types[_type]
        else:
            InputBox = cls

        box = InputBox(**kw)
        box.changed.connect(lambda x, box=box: self.queue_setting(box=box))

        # use hbox to add tooltip as QLabel
        h_box = QHBoxLayout()
        h_box.addWidget(box)

        # connect extra funcs on value changed
        funcs = config.get('funcs')
        if funcs:
            for func in f.as_list(funcs):
                box.changed.connect(lambda x: self.queue_func(key=key, func=func))

        # set tooltip
        tooltip = config.get('tooltip', None)
        if not tooltip is None:
            label.setToolTip(tooltip)

        info_label = QLabel(tooltip)
        info_label.setFixedWidth(300)
        info_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        info_label.setWordWrap(True)
        h_box.addWidget(info_label)

        layout.addRow(label, h_box)

    def queue_setting(self, box: ff.FormFields) -> None:
        """Update saved setting when box state changed

        Parameters
        ----------
        box : ff.FormFields
            box who's value to save to setting
        """
        self.queued_changes[box.key] = box.val

    def queue_func(self, key: str, func: Callable) -> None:
        """Queue func to call if settings accepted

        Parameters
        ----------
        key : str
            settings key to keep func calls unique
        func : Callable
        """
        self.queued_funcs[key] = func

    def accept(self):
        """Save settings and trigger funcs"""

        for key, val in self.queued_changes.items():
            try:
                self.s.setValue(key, val)
            except Exception as e:
                log.error(f'Failed to set value: ({key}, {val})')

        for key, func in self.queued_funcs.items():
            try:
                func()
            except Exception as e:
                log.error(f'Failed to call function: {func}')

        super().accept()


def msgbox(msg='', yesno=False, statusmsg=None, **kw):
    """Show messagebox, with optional yes/no prompt\n
    If app isn't running, prompt through python instead of dialog

    Parameters
    ----------
    msg : str, optional\n
    yesno : bool, optional\n
    statusmsg : [type], optional\n
        Show more detailed smaller message
    """

    if gbl.app_running():
        app = check_app()
        dlg = MsgBoxAdvanced(
            msg=msg,
            window_title=gbl.title,
            yesno=yesno,
            statusmsg=statusmsg,
            **kw)

        return dlg.exec()
    elif yesno:
        # if yesno and NOT frozen, prompt user through terminal
        return f._input(msg)
    else:
        print(msg)


def msg_simple(msg: str = '', icon: str = '', infotext: str = None):
    """Show message to user with dialog if app running, else print

    Parameters
    ----------
    msg : str, optional
    icon : str, optional
        Show icon eg 'warning', 'critical', default None
    infotext : str, optional
        Detailed text to show, by default None
    """
    if gbl.app_running():
        dlg = QMessageBox()
        dlg.setText(msg)
        dlg.setWindowTitle(gbl.title)

        icon = icon.lower()

        if icon == 'critical':
            dlg.setIcon(QMessageBox.Icon.Critical)
        elif icon == 'warning':
            dlg.setIcon(QMessageBox.Icon.Warning)

        if infotext:
            dlg.setInformativeText(infotext)

        return dlg.exec()
    else:
        print(msg)


def show_err_msg(text: str, tb_text: str = None) -> None:
    if gbl.app_running():
        dlg = ErrMsg(text=text, tb_text=tb_text)
        dlg.exec()
    else:
        print(text, tb_text)


def inputbox(msg='Enter value:', dtype='text', items=None, editable=False, title: str = None):
    if title is None:
        title = gbl.title

    app = check_app()
    dlg = QInputDialog()
    dlg.resize(gbl.minsize)
    dlg.setWindowTitle(title)
    dlg.setLabelText(msg)

    if dtype == 'text':
        dlg.setInputMode(QInputDialog.InputMode.TextInput)
    elif dtype == 'choice':
        dlg.setComboBoxItems(items)
        dlg.setFont(QFont('Courier New'))
        dlg.setComboBoxEditable(editable)

    elif dtype == 'int':
        dlg.setInputMode(QInputDialog.InputMode.IntInput)
        dlg.setIntMaximum(10)
        dlg.setIntMinimum(0)

    ok = dlg.exec()
    if dtype in ('text', 'choice'):
        val = dlg.textValue()
    elif dtype == 'int':
        val = dlg.intValue()

    return ok, val


def about():
    mw = gbl.get_mainwindow()
    u = mw.u
    m = {
        'Version': VERSION,
        'User Name': u.username,
        'Email': u.email,
        'User Group': u.usergroup,
        'Install Directory': str(cf.p_root)}

    msg = f'HBA Guest Tracker\n\n{f.pretty_dict(m)}'
    return msg_simple(msg=msg)


def check_dialog_path(p: Path) -> str:
    """Check if path exists, if not, warn user and return desktop

    Parameters
    ----------
    p : Path

    Returns
    -------
    str
        filepath as string
    """
    if not fl.check(p, warn=False):
        gbl.update_statusbar(f'Path: {p} does not exist. Check VPN connection.', warn=True)
        p = Path.home() / 'Desktop'

    return str(p)


def make_folder_dialog(p_start: Path = None):
    if p_start is None:
        p_start = cf.desktop

    return QFileDialog(
        directory=check_dialog_path(p_start),
        options=QFileDialog.Option.ShowDirsOnly | QFileDialog.Option.DontUseNativeDialog)


def select_multi_folders(p_start: Path) -> Union[List[Path], None]:
    """Allow user to select multiple folders
    - NOTE will also return prev level higher than final selected folders
    """
    app = check_app()

    # need this so can remove from selected folders
    dlg = QFileDialog(directory=check_dialog_path(p_start))
    dlg.setFileMode(QFileDialog.FileMode.Directory)
    dlg.setOption(QFileDialog.Option.DontUseNativeDialog, True)

    # make it possible to select multiple directories
    file_view = dlg.findChild(QListView, 'listView')
    if file_view:
        file_view.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)

    f_tree_view = dlg.findChild(QTreeView)
    if f_tree_view:
        f_tree_view.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)

    if dlg.exec():
        paths = dlg.selectedFiles()

        # includes previous dir by default
        if len(paths) > 1:
            paths = paths[1:]

        return [Path(s) for s in paths]
    else:
        return None


def select_single_folder(p_start: Path) -> Union[Path, None]:
    """Allow user to select folder

    Parameters
    ----------
    p_start : Path

    Returns
    -------
    Union[Path, None]
    """
    app = check_app()
    s = make_folder_dialog(p_start).getExistingDirectory()

    return Path(s) if s else None


def select_multi_files(p_start: Path, fltr: str = '') -> Union[list, None]:
    """Select multiple files (not folders) from directory

    Parameters
    ----------
    p_start : Path
        path to show user
    fltr : str
        file type filter eg "Images (*.png *.xpm *.jpg)" or "*.csv"

    Returns
    -------
    Union[list, None]
        list of files selected
    """
    app = check_app()

    lst = QFileDialog \
        .getOpenFileNames(
            directory=check_dialog_path(p_start),
            options=QFileDialog.Option.DontUseNativeDialog,
            filter=fltr)

    # lst is list of files selected + filetypes > ignore filetypes part
    return lst[0] if lst else None


def save_file(p_start=None, name=None, ext='xlsx'):
    # TODO save last folder location to QSettings?
    if p_start is None:
        p_start = Path.home() / 'Desktop'

    p = p_start / f'{name}.{ext}'

    app = check_app()
    s = QFileDialog.getSaveFileName(caption='Save File', directory=str(
        p), filter='*.xlsx, *.csv', options=QFileDialog.Option.DontUseNativeDialog)

    if s[0]:
        return Path(s[0])
    return None


def add_okay_cancel(dlg, layout):
    # add an okay/cancel btn box to bottom of QDialog's layout (eg self.v_layout)
    # parent = layout.parent()

    btnbox = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
    btnbox.accepted.connect(dlg.accept)
    btnbox.rejected.connect(dlg.reject)

    layout.addWidget(btnbox, alignment=Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignCenter)
    dlg.btnbox = btnbox


def create_linesep(parent=None):
    line_sep = QFrame(parent=parent)
    line_sep.setObjectName('line_sep')
    line_sep.setFrameShape(QFrame.Shape.HLine)
    line_sep.setFrameShadow(QFrame.Shadow.Raised)
    # line_sep.setStyleSheet('QFrame[frameShape="4"]
    # line_sep {color: red; padding: 20px; padding-top: 20px; padding-bottom: 20px}')
    return line_sep


def add_linesep(layout, i=None):
    # doesn't work well with form_layout
    type_ = 'Row' if isinstance(layout, QFormLayout) else 'Widget'

    add_func = f'add{type_}'
    insert_func = f'insert{type_}'

    line_sep = create_linesep()

    if i is None:
        getattr(layout, add_func)(line_sep)
    else:
        getattr(layout, insert_func)(i, line_sep)


def print_children(obj, depth=0, max_depth=3):
    tab = '\t'
    # if hasattr(obj, 'findChildren'):
    if depth > max_depth:
        return

    for o in obj.children():
        type_ = str(type(o)).split('.')[-1].replace("'>", '')
        print(f'{tab * (depth + 1)}name: {o.objectName()} | type: {type_}')

        print_children(obj=o, depth=depth + 1, max_depth=max_depth)


def show_item(name, parent=None, *args, **kw):
    # show message dialog by name eg gbl.show_item('InputUserName')
    app = check_app()
    dlg = getattr(sys.modules[__name__], name)(parent=parent, *args, **kw)
    # print(dlg.styleSheet())
    return dlg, dlg.exec()


def check_app() -> QApplication:
    """Just need to make sure app is set before showing dialogs"""
    return gbl.get_qt_app()


def unit_exists(unit):
    """Check if unit exists, outside of DB class, raise error message if False"""
    if not db.unit_exists(unit=unit):
        msg = f'Unit "{unit}" does not exist in database. Please add it to db from the [Unit Info] tab.'
        msg_simple(msg=msg, icon='warning')
        return False
    else:
        return True
