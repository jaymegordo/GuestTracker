
from datetime import datetime as dt
from datetime import timedelta as delta
from typing import TYPE_CHECKING, Dict, List, Union

import pandas as pd
from PyQt6.QtCore import QSize, Qt
from PyQt6.QtWidgets import (
    QAbstractItemView, QAbstractScrollArea, QHBoxLayout, QTableWidget,
    QTableWidgetItem)

from guesttracker import IntNone
from guesttracker import functions as f
from guesttracker.database import db
from guesttracker.gui import _global as gbl
from guesttracker.gui import formfields as ff
from guesttracker.gui.dialogs.dialogbase import BaseDialog, add_okay_cancel
from guesttracker.queries.fc import FCOpen
from guesttracker.queries.misc import ACMotorInspections
from guesttracker.queries.misc import Parts as _Parts
from guesttracker.queries.smr import UnitSMR

if TYPE_CHECKING:
    from guesttracker.queries import QueryBase


class DialogTableWidget(QTableWidget):
    """Simple table widget to display as a pop-out dialog"""

    def __init__(
            self,
            df: pd.DataFrame,
            query: Union['QueryBase', None] = None,
            editable: bool = False,
            name: str = 'table',
            col_widths: Union[Dict[str, int], None] = None,
            scroll_bar: bool = False,
            min_width: IntNone = None,
            **kw):
        super().__init__()
        self.setObjectName(name)
        self.formats = {}
        self.query = query

        self.setColumnCount(df.shape[1])
        self.setHorizontalHeaderLabels(list(df.columns))

        if col_widths:
            self.min_width = sum(col_widths.values()) if min_width is None else min_width
            for col, width in col_widths.items():
                self.setColumnWidth(df.columns.get_loc(col), width)

        if not editable:
            self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)

        self.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        if not scroll_bar:
            self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        self.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)

        self.df = df
        self.query = query
        self.ediiable = editable
        self.name = name
        self.scroll_bar = scroll_bar

    def sizeHint(self):
        """"Sketch, but have to override size width"""
        size = super().sizeHint()

        if hasattr(self, 'min_width'):
            width = self.min_width + 34
            if self.scroll_bar:
                width += 15  # width of scrollbar

            return QSize(width, size.height())
        else:
            return size

    def selected_rows(self) -> List[List[QTableWidgetItem]]:
        """Return list of selected rows"""
        sel = self.selectedItems()
        num_cols = self.columnCount()

        return [sel[i:i + num_cols] for i in range(0, len(sel), num_cols)]

    def deselect_row(self, row: int) -> None:
        self.blockSignals(True)
        for cell in self.selectedItems():
            if cell.row() == row:
                cell.setSelected(False)

        self.blockSignals(False)

    def set_formats(self, df: pd.DataFrame) -> None:
        date_cols = df.dtypes[df.dtypes == 'datetime64[ns]'].index.to_list()
        int_cols = df.dtypes[df.dtypes == 'Int64'].index.to_list()
        self.formats |= {col: '{:%Y-%m-%d}' for col in date_cols}
        self.formats |= {col: '{:,.0f}' for col in int_cols}

    def display_data(self, df: pd.DataFrame, resize_cols: bool = False) -> None:
        if df is None:
            return

        self.set_formats(df=df)
        self.setRowCount(df.shape[0])
        self.setVerticalHeaderLabels(df.index.astype(str).tolist())

        # set delegate for column alignment based on dtype
        from guesttracker.gui.delegates import TableWidgetDelegate
        delegate = TableWidgetDelegate(parent=self, df=df)
        self.setItemDelegate(delegate)

        df_display = f.df_to_strings(df=df, formats=self.formats)

        query = self.query
        bg, text = None, None
        if not query is None:
            stylemap = query.get_stylemap(df=df)
            if not stylemap is None:
                bg, text = stylemap[0], stylemap[1]

        for irow, row in enumerate(df.index):
            for icol, col in enumerate(df.columns):

                item = QTableWidgetItem(str(df_display.loc[row, col]))

                # set background and text colors
                if not (bg is None or text is None):
                    color_bg = bg[col].get(row, None)
                    color_text = text[col].get(row, None)

                    if color_bg:
                        item.setBackground(color_bg)
                    if color_text:
                        item.setForeground(color_text)

                self.setItem(irow, icol, item)

        if resize_cols:
            self.resizeColumnsToContents()

        self.resizeRowsToContents()
        self.adjustSize()
        self.df = df


class TableDialog(BaseDialog):
    """Base class dialog with table widget for quick/temp table display"""

    def __init__(
            self,
            df=None,
            cols: Union[dict, list] = None,
            name: str = 'table',
            parent: object = None,
            _show: bool = False,
            simple_table: bool = True,
            editable: bool = False,
            maximized: bool = False,
            query: 'QueryBase' = None,
            **kw):
        super().__init__(parent, **kw)

        # auto resize cols if no dict of col_widths
        if isinstance(cols, dict):
            col_widths = cols
            cols = cols.keys()
            self.resize_cols = False
        else:
            col_widths = None
            self.resize_cols = True

            if not df is None:
                cols = df.columns.tolist()

        # init table with default cols
        if simple_table:
            self.tbl = DialogTableWidget(
                df=pd.DataFrame(columns=cols),
                col_widths=col_widths,
                name=name,
                scroll_bar=True,
                **kw)
        else:
            # use full TableView for filters/line highlighting etc
            from guesttracker.gui import tables as tbls

            if query is None:
                raise RuntimeError('Must pass query obj if not using simple table')

            self.query = query
            self.tbl = tbls.TableView(parent=self, default_headers=cols, editable=editable)

        self.v_layout.addWidget(self.tbl)

        add_okay_cancel(dlg=self, layout=self.v_layout)

        f.set_self(vars())

        if _show:
            self.load_table(df=df)

        if maximized:
            self.showMaximized()

    def load_table(self, df):
        tbl = self.tbl
        tbl.display_data(df, resize_cols=self.resize_cols)
        tbl.setFocus()
        self.adjustSize()

    def add_buttons(self, *btns, func=None):
        """Add button widgets to top of table

        Parameters
        ----------
        *btns : ff.FormFields
            button widgets to add
        func : callable, optional
            function to trigger on value changed, default None
            - NOTE could make this a dict of name : func
        """
        btn_controls = QHBoxLayout()

        for item in btns:

            # add trigger function
            if not func is None:
                item.changed.connect(func)

            btn_controls.addWidget(item)

        btn_controls.addStretch(1)  # force buttons to right side
        self.v_layout.insertLayout(0, btn_controls)

        f.set_self(vars())


class ACInspectionsDialog(TableDialog):
    """Show SMR history per unit"""
    name = 'ACInspections'

    def __init__(self, parent=None, **kw):
        query = ACMotorInspections(theme='dark')
        df = query.get_df()
        super().__init__(parent=parent, df=df, query=query, simple_table=False,
                         window_title='AC Motor Inspections', maximized=True, **kw)

        # cols need to change when sorted
        # NOTE this isn't super dry, could define this somewhere else?
        self.tbl.mcols['sort_filter'] = ('Unit', 'fc_number_next')

        self.load_table(df=df)


class UnitSMRDialog(TableDialog):
    """Show SMR history per unit"""

    def __init__(self, parent=None, unit=None, **kw):
        cols = dict(Unit=60, DateSMR=90, SMR=90)

        super().__init__(parent=parent, cols=cols, simple_table=True, window_title='Unit SMR History', **kw)

        df_unit = db.get_df_unit()
        minesite = gbl.get_minesite()
        cb_unit = ff.ComboBox()
        items = f.clean_series(df_unit[df_unit.MineSite == minesite].Unit)
        cb_unit.set_items(items)

        d = dt.now()
        de_lower = ff.DateEdit(date=d + delta(days=-60))
        de_upper = ff.DateEdit(date=d)

        self.add_buttons(cb_unit, de_lower, de_upper, func=self.load_table)

        f.set_self(vars())

        if not unit is None:
            cb_unit.val = unit
            self.load_table()

    def load_table(self):
        """Reload table data when unit or dates change"""
        query = UnitSMR(
            unit=self.cb_unit.val,
            d_rng=(self.de_lower.val, self.de_upper.val))

        df = query.get_df() \
            .sort_values(by='DateSMR', ascending=False)

        super().load_table(df=df)


class UnitOpenFC(TableDialog):
    """Show table widget of unit's open FCs"""

    def __init__(self, unit, parent=None):

        cols = {
            'FC Number': 80,
            'Type': 40,
            'Subject': 300,
            'ReleaseDate': 90,
            'ExpiryDate': 90,
            'Age': 60,
            'Remaining': 70}

        super().__init__(parent=parent, cols=cols, name='fc_table', window_title='Open FCs')

        query = FCOpen()
        df_fc = db.get_df_fc()
        df_unit = db.get_df_unit()
        minesite = gbl.get_minesite()

        # btn_controls = QHBoxLayout()
        cb_minesite = ff.ComboBox(items=db.get_list_minesite(), default=minesite)
        cb_unit = ff.ComboBox()

        self.add_buttons(cb_minesite, cb_unit)

        f.set_self(vars())
        self.load_table(unit=unit)

        self.set_units_list()
        cb_unit.val = unit

        cb_minesite.currentIndexChanged.connect(self.set_units_list)
        cb_unit.currentIndexChanged.connect(self._load_table)

    def set_units_list(self):
        """Change units list when minesite changes"""
        cb_unit, cb_minesite, df = self.cb_unit, self.cb_minesite, self.df_unit
        items = f.clean_series(df[df.MineSite == cb_minesite.val].Unit)
        cb_unit.set_items(items)

    def _load_table(self):
        """Load table from cb_unit value"""
        self.load_table(unit=self.cb_unit.val)

    def load_table(self, unit):
        """Reload table data when unit changes"""
        tbl, df, query, layout = self.tbl, self.df_fc, self.query, self.v_layout

        df = df.pipe(query.df_open_fc_unit, unit=unit)
        super().load_table(df=df)


class Parts(TableDialog):
    def __init__(self, unit: str, parent=None):

        cols = {
            'PartNo': 100,
            'PartName': 400,
            'Model': 80}

        query = _Parts()

        super().__init__(
            parent=parent,
            cols=cols,
            name='parts',
            window_title='Parts',
            query=query,
            simple_table=False)

        df = db.get_df_parts()

        if not unit is None:
            model_base = db.get_unit_val(unit=unit, field='ModelBase')

            df = df.query('ModelBase == @model_base')

        # cb_part_no = ff.ComboBox(items=f.clean_series(df.PartNo))
        # cb_part_name = ff.ComboBox(items=f.clean_series(df.PartName))
        # cb_model = ff.ComboBox(items=f.clean_series(df.Model))

        # self.add_buttons(cb_part_no, cb_part_name, cb_model)

        f.set_self(vars())
        self.load_table(df=df)
