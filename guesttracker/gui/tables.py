import copy
import re
import time
from collections import defaultdict as dd
from functools import partial
from pathlib import Path
from typing import *

import numpy as np
import pandas as pd
from PyQt6.QtCore import (
    QItemSelection, QItemSelectionModel, QModelIndex, QSize, Qt, QTimer,
    pyqtSignal)
from PyQt6.QtGui import QColor, QFontMetrics, QKeySequence
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QHBoxLayout, QHeaderView, QLabel,
    QLineEdit, QListWidget, QListWidgetItem, QMenu, QPushButton, QStyle,
    QTableView, QTableWidget, QVBoxLayout, QWidget, QWidgetAction)
# from selenium.webdriver.remote.webdriver import WebDriver
from sqlalchemy.orm.query import Query as SQLAQuery

from guesttracker import IntNone
from guesttracker import config as cf
from guesttracker import dbtransaction as dbt
from guesttracker import dt
from guesttracker import errors as er
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker import queries as qr
from guesttracker import styles as st
from guesttracker.database import db
from guesttracker.gui import _global as gbl
from guesttracker.gui import formfields as ff
from guesttracker.gui.datamodel import TableDataModel
from guesttracker.gui.delegates import (
    CellDelegate, ComboDelegate, DateDelegate, DateTimeDelegate,
    HighlightCellDelegate, TimeDelegate)
from guesttracker.gui.dialogs import addrows as adr
from guesttracker.gui.dialogs import dialogbase as dlgs
from guesttracker.gui.dialogs import refreshtables as rtbls
from guesttracker.gui.dialogs.addrows import (
    AddEmail, AddEvent, AddPart, AddUnit)
from guesttracker.gui.dialogs.tables import ACInspectionsDialog, UnitSMRDialog
from guesttracker.gui.multithread import Worker
from guesttracker.queries.hba import HBAQueryBase
from guesttracker.utils import dbconfig as dbc
from guesttracker.utils import dbmodel as dbm
from guesttracker.utils import email as em
from guesttracker.utils import fileops as fl
from jgutils import pandas_utils as pu

if TYPE_CHECKING:
    from pandas.io.formats.style import Styler

    from guesttracker.gui.gui import MainWindow, TabWidget

log = getlog(__name__)


class TableView(QTableView):
    dataFrameChanged = pyqtSignal()
    cellClicked = pyqtSignal(int, int)

    delegates = dict(
        date=DateDelegate,
        datetime=DateTimeDelegate,
        time=TimeDelegate,
        highlight_dates=HighlightCellDelegate)

    colors = cf.config['color']
    bg = colors['bg']
    yellow, darkyellow, lightblue = bg['yellowrow'], bg['darkyellow'], '#148CD2'
    location, colr = 'background-color', 'inherit'
    stylesheet = f' \
            QTableView::item:selected {{color: {colr}; {location}: {yellow};}} \
            QTableView::item:selected:active {{color: {colr}; {location}: {yellow};}} \
            QTableView:item:selected:focus {{color: {colr}; border: 1px solid red; }} \
            QTableView::item:selected:hover {{color: {colr}; {location}: {darkyellow};}}'

    prevent_column_resize = False

    def __init__(
            self,
            parent: Union['TableWidget', None] = None,
            default_headers: Union[List[str], None] = None,
            editable=True,
            header_margin: IntNone = None,
            warn_rows: int = 2000,
            *args,
            **kwargs):
        super().__init__(parent, *args, **kwargs)

        if not parent is None:
            self.mainwindow = parent.mainwindow
            self.settings = parent.settings
            self.name = parent.name.lower()
        else:
            self.mainwindow = gbl.get_mainwindow()

        self.mcols = dd(tuple)
        self.is_init = False  # TableWidget sets to True after refresh
        self.header_key = f'headers/{self.name}'
        col_widths = {'Title': 150, 'Part Number': 150, 'Failure Cause': 300}
        self.highlight_funcs, col_func_triggers, self.formats = dd(type(None)), dd(list), {}
        highlight_funcs_complex = dd(type(None))

        highlight_vals = {
            'true': 'goodgreen',
            'false': 'bad'}

        # start with query formats, will be overridden if needed
        query = parent.query
        self.formats |= query.formats

        # set up initial empty model
        self.parent = parent  # model needs this to access parent table_widget
        self.table_widget = parent if isinstance(parent, TableWidget) else None
        _data_model = TableDataModel(parent=self)
        self.setModel(_data_model)
        rows_initialized = True

        # Signals/Slots
        _data_model.modelReset.connect(self.dataFrameChanged)
        _data_model.dataChanged.connect(self.dataFrameChanged)
        self.dataFrameChanged.connect(self._enable_widgeted_cells)  # NOTE or this

        header = HeaderView(self, margin=header_margin)
        self.setHorizontalHeader(header)

        self.setItemDelegate(CellDelegate(parent=self))
        self.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.setWordWrap(True)
        # self.setSortingEnabled(True)  # dont need this. calls model().sort() every time data set

        self.setStyleSheet(self.stylesheet)

        sel = self.selectionModel()
        sel.currentChanged.connect(self.data_model.row_changed)

        # DONT USE - this messes up row selection
        # self.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.parent = parent
        self.read_only_all = False  # disable editing of all columns
        f.set_self(vars())
        self.set_default_headers(default_headers=default_headers)

        if not editable:
            self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)

        self.setVisible(True)

    @property
    def u(self):
        return self.parent.u

    @property
    def data_model(self) -> TableDataModel:
        return self.model()

    @property
    def e(self):
        return self.model_from_activerow()

    def get_e(self, **kw):
        """Same as e, but allow passing warn=False"""
        return self.model_from_activerow(**kw)

    @property
    def e_db(self):
        return self.model_from_activerow_db()

    @property
    def i(self):
        """Return active row index
        - Used to create QModelIndex to update row"""
        return self.active_row_index()

    @property
    def row(self) -> dbt.Row:
        return self.row_from_activerow()

    def display_data(self, df: pd.DataFrame, init_cols: bool = True, **kw) -> float:
        """Display dataframe in table
        - called with refreshing data AND when setting default headers on init
        - NOTE maybe don't set column widths on first call? (add kw)

        Parameters
        ----------
        df : pd.DataFrame

        Returns
        -------
        float
            time taken to display data in table
        """
        self.rows_initialized = False

        if df.shape[0] > self.warn_rows:
            msg = f'Warning: the data you requested has a large number of rows: {df.shape[0]}\n\n\
                Would you like to continue loading data to the table?'
            if not dlgs.msgbox(msg=msg, yesno=True):
                return 0

        start = time.time()
        self.data_model.set_df(df=df, center_cols=self.get_center_cols(df=df))

        self.hide_columns()
        if init_cols:
            self.init_columns()

        self.rows_initialized = True
        self.resizeRowsToContents()

        return time.time() - start

    def set_default_headers(self, default_headers: List[str] = None):
        """Initialize table by setting empty df with headers only

        Parameters
        ----------
        default_headers : List[str], optional
            default headers to use, by default None
            - Most tables set their own, temp tables like ACInspections need to pass in
        """
        if default_headers is None:
            cols = f.get_default_headers(title=self.parent.title)
        else:
            cols = default_headers

        df = pd.DataFrame(columns=cols) \
            .pipe(pu.parse_datecols) \
            .pipe(f.set_default_dtypes, m=self.parent.query.default_dtypes)

        self.display_data(df=df, init_cols=False)

    def set_datecol_delegates(self) -> None:
        """Set datetime column delegates and column format"""
        model = self.data_model

        for k in self.delegates.keys():
            if model.mcols[k]:
                delegate = self.delegates[k](self)

                for i in model.mcols[k]:
                    col = model.get_col_name(i)
                    self.setItemDelegateForColumn(i, delegate)

                    if not k == 'highlight_dates':
                        self.formats[col] = f'{{:{delegate.display_format}}}'

    def set_datecol_widths(self) -> None:
        """Set datetime column default widths"""
        for k, Delegate in self.delegates.items():
            if not k == 'highlight_dates':
                for i in self.data_model.mcols[k]:
                    self.setColumnWidth(i, Delegate.width)

    def hide_columns(self) -> None:
        """Hide specific columns (eg UID)"""
        for col in self.mcols['hide']:
            self.hideColumn(self.data_model.get_col_idx(col))

    def init_columns(self) -> None:
        """Called every time data refreshed
        - sets special datetime delegates
        - checks if has header_state saved already
            - if not, sets default column widths
        - DOES NOT save header state
        """
        self.set_datecol_delegates()

        if not self.prevent_column_resize and self.restore_header_state():
            return

        self.resizeColumnsToContents()
        self.set_datecol_widths()

        # set with default column widths
        model = self.data_model

        for c, width in self.col_widths.items():
            if c in model.df.columns:
                self.setColumnWidth(model.get_col_idx(c), width)

    def restore_header_state(self) -> bool:
        """Restore header state if exists saved in settings

        Returns
        -------
        bool
            True if has state saved else False
        """
        # some large text columns need max width
        # should some columns always resize to contents?
        header_state = self.settings.value(self.header_key, None)
        if not header_state is None:
            self.horizontalHeader().restoreState(header_state)
            log.info(f'restored header state: {self.header_key}')
            return True

        return False

    def save_header_state(self) -> None:
        """Save header column widths/hidden state
        - Will NOT save sate until table is_init (after first refresh)
        - called when:
            - active tab changes
            - app closed
            - table data refreshed
        """
        if not self.is_init:
            return

        self.settings.setValue(self.header_key, self.horizontalHeader().saveState())

    def reset_header_state(self) -> None:
        """Allow user to reset column widths/states to default
        - user will need to call refresh
        """
        self.settings.remove(self.header_key)
        self.is_init = False
        self.update_statusbar(f'Column layouts reset to default: {self.name}', success=True)

    def show_search(self):
        """Show search dialog"""
        dlg = dlgs.Search(self)
        dlg.show()

    def double_click_enter(self, QModelIndex):
        """NOTE this is old, not used"""
        print('double_click_enter')
        QModelIndex.model().change_color(Qt.GlobalColor.red, True)

        self.alarm = QTimer()
        self.alarm.setSingleShot(True)
        self.alarm.timeout.connect(self.color_timeout)
        self.alarm.start(200)

    def color_timeout(self):
        self.data_model.change_color(Qt.GlobalColor.magenta, False)

    def add_highlight_funcs(self, cols, func, cmplx=False):
        """Add same highlight func to multiple cols"""
        if not isinstance(cols, list):
            cols = [cols]
        for col in cols:
            if not cmplx:
                self.highlight_funcs[col] = func
            else:
                self.highlight_funcs_complex[col] = func

    def add_col_funcs(self, cols: Union[str, List[str]], func: Callable) -> None:
        """Add col trigger func to single or multiple cols
        - called by TableDataModel on cell changed like:
            - >>> func(index=index, val_new=val, val_prev=val_prev)

        Parameters
        ----------
        cols : Union[str, List[str]]
            columns to add function triggers to
        func : Callable
            function to call when cell changed
        """

        for col in f.as_list(cols):
            self.col_func_triggers[col].append(func)

    def highlight_alternating(self, df, row, col, role, **kw):
        # use count of unique values mod 2 to highlight alternating groups of values
        # TODO only works if column is sorted by unit
        # TODO make this work with irow/icol .. email table fails
        irow, icol = df.index.get_loc(row), df.columns.get_loc(col)
        alt_row_num = len(df.iloc[:irow + 1, icol].unique()) % 2

        if alt_row_num == 0 and role == Qt.ItemDataRole.BackgroundRole:
            return QColor(self.colors['bg']['maroon'])

    def highlight_by_val(self, val, role, **kw):
        # map cell value > color name > color code
        color_name = self.highlight_vals.get(str(val).lower(), None)
        if not color_name is None:
            color_code = self.colors['bg'].get(color_name, None)

            if not color_code is None:
                if role == Qt.ItemDataRole.BackgroundRole:
                    return QColor(color_code)
                elif role == Qt.ItemDataRole.ForegroundRole:
                    if 'light' in color_name:  # TODO maybe move into own func
                        return QColor(Qt.GlobalColor.black)
                    else:
                        color_code = self.colors['text'].get(color_name, None)
                        if not color_code is None:
                            return QColor(color_code)

    def highlight_blanks(self, val, role, **kw):
        if val is pd.NA or val is np.NaN:
            val = None

        if val in ('', None):
            if role == Qt.ItemDataRole.BackgroundRole:
                return QColor(self.colors['bg']['bad'])
            elif role == Qt.ItemDataRole.ForegroundRole:
                return QColor(self.colors['text']['bad'])

    def highlight_pics(self, val, role, **kw):
        color = 'goodgreen' if f.isnum(val) and val > 0 else 'bad'

        if role == Qt.ItemDataRole.BackgroundRole:
            color_code = self.colors['bg'][color]
        elif role == Qt.ItemDataRole.ForegroundRole:
            color_code = self.colors['text'][color]

        return QColor(color_code)

    def highlight_color_scale(self, val, **kw):
        # highlight values using max/min within range of multiple columns
        # Not used

        if self.col_maxmin is None:
            df = self.data_model.df
            df = df[self.maxmin_cols]
            self.col_maxmin = tuple(df.max().max(), df.min().min())

        return

    def get_style(
            self,
            df: pd.DataFrame = None,
            outlook: bool = False,
            exclude_cols: List[str] = None) -> 'Styler':
        """Get styler with color from current TableView's dataframe

        Parameters
        ----------
        df : DataFrame, optional
        outlook : bool, optional
            Different styles used if outlook, by default False
        exclude_cols : list, optional

        Returns
        -------
        pd.Styler
        """
        model = self.data_model
        if df is None:
            df = model.df.copy() \
                .drop(columns=list(self.mcols['hide']))

        # only pass a subset to get_background_colors if exclude_cols are passed
        kw = dict(subset=[c for c in df.columns if not c in exclude_cols]) if not exclude_cols is None else {}

        # HACK replace date formats '{:%Y-%m-%d}' with func which skips pd.NaT, styler cant handle
        m_replace = {
            '{:%Y-%m-%d}': st.format_date,
            '{:%Y-%m-%d     %H:%M}': st.format_datetime}
        formats = {k: v if not v in m_replace else m_replace[v] for k, v in self.formats.items()}

        s = []
        s.append(dict(
            selector='table',
            props=[('border', '1px solid black')]))

        return st.default_style(df=df, outlook=outlook) \
            .apply(model.get_background_colors_from_df, axis=None, **kw) \
            .pipe(st.format_dict, fmt=formats) \
            .pipe(st.add_table_style, s=s)

    def resizeRowsToContents(self):
        # sender = self.sender()
        # model cant sort initially before the column widths are set
        if self.rows_initialized:
            super().resizeRowsToContents()

    def keyPressEvent(self, event):
        # F2 to edit cell
        # print(event.key(), event.key() == Qt.Key.Key_Enter)
        if event.key() in (16777265, 16777220, Qt.Key.Key_Enter) \
                and (self.state() != QAbstractItemView.State.EditingState):
            self.edit(self.currentIndex())
        elif event.matches(QKeySequence.StandardKey.Copy):
            self.copy()
        elif event.key() == Qt.Key.Key_D and event.modifiers() == Qt.KeyboardModifier.ControlModifier:
            self.fill_down()
        elif event.key() == Qt.Key.Key_Escape:
            self.sel.clear()  # clear selected row highlight
        else:
            super().keyPressEvent(event)

    def set_combo_delegate(
            self,
            col: str,
            items: list = None,
            dependant_col: str = None,
            allow_blank: bool = True):
        """Assign combo delegate to column.

        Parameters
        ----------
        col : str
        items : list, optional
            List of items for ComboBox
        dependant_col : str, optional
            Column to check before setting items (used for issue_category/sub_category)
        allow_blank : bool
            Allow blank values, will append '' to list
        """
        model = self.data_model
        combo_delegate = ComboDelegate(
            parent=self,
            items=items,
            dependant_col=dependant_col,
            allow_blank=allow_blank)

        c = model.get_col_idx(col=col)
        self.setItemDelegateForColumn(c, combo_delegate)

    def set_value(self, col_name: str, val: Any, uid: float, irow: int = None):
        """Update single value in database and current table, if field exists
        - NOTE only works for EL tables, need to get table keys other than UID (too lazy rn)

        Parameters
        ----------
        col_name : str
        val : Any
        uid : float
            row uid (EL tables only)
        irow : int, optional
            index of row to update, by default active_row
        """

        if irow is None:
            irow = self.i

        # index = None if column doesn't exist in current table
        index = self.create_index_activerow(col_name=col_name)

        if not index is None:
            self.data_model.setData(index=index, val=val)
        else:
            # not in table, just update db
            dbt.Row(dbtable=self.parent.get_dbtable(), keys=dict(UID=uid)) \
                .update(vals={col_name: val})

    def _header_menu(self, pos):
        """Create popup menu used for header"""
        # TODO values > df.col.value_counts()
        model = self.data_model
        menu = FilterMenu(self)

        # get logical index by mouse position
        header = self.header
        icol = header.logicalIndexAt(pos)

        if icol == -1:
            return  # out of bounds

        # Filter Menu Action
        menu.addAction(DynamicFilterMenuAction(self, menu, icol))
        menu.addAction(FilterListMenuWidget(self, menu, icol))
        menu.addAction(self._icon('DialogResetButton'),
                       'Clear Filter',
                       model.reset_filter)

        # Sort Ascending/Decending Menu Action
        menu.addAction(self._icon('TitleBarShadeButton'),
                       'Sort Ascending',
                       partial(model.sort, icol=icol, order=Qt.SortOrder.AscendingOrder))
        menu.addAction(self._icon('TitleBarUnshadeButton'),
                       'Sort Descending',
                       partial(model.sort, icol=icol, order=Qt.SortOrder.DescendingOrder))
        menu.addSeparator()

        # Hide
        menu.addAction(f'Hide Column: {model.headerData(icol, Qt.Orientation.Horizontal)}',
                       partial(self.hideColumn, icol))

        # set columns movable
        menu.addAction('Move Column Left', partial(self._move_col, icol, -1))
        menu.addAction('Move Column Right', partial(self._move_col, icol, 1))

        # Show column to left and right
        # description needs to get the LOGICAL index of the VISIBLE index next to it

        for i in (-1, 1):
            cur_vis = header.visualIndex(icol)  # visual index of current logical
            col = header.logicalIndex(cur_vis + i)  # logical idx of neighbor (hidden is still visible)

            if not col == -1 and header.isSectionHidden(col):
                menu.addAction(f'Unhide Column: {model.headerData(col, Qt.Orientation.Horizontal)}',
                               partial(self.showColumn, col))

        menu.exec(self.mapToGlobal(pos))

    def _move_col(self, icol: int, direction: int) -> None:

        # Swaps the section at visual index first with the section at visual index second.
        icol = self.header.visualIndex(icol)
        self.header.swapSections(icol, icol + direction)

    def iter_selected_idxs(
            self,
            irow_only: bool = False,
            icol_only: bool = False) -> Union[QModelIndex, int]:
        """Convenience func to iter current selected rows/cols

        - irow: int = index.row()
        - icol: int = index.column()

        Yields
        -------
        Union[QModelIndex, int]
            index or irow/icol
        """
        for index in self.selectedIndexes():
            if irow_only:
                yield index.row()
            elif icol_only:
                yield index.column()
            else:
                yield index

    def iter_selected_models(
            self,
            db: bool = False) -> Union[dbm.Base, Tuple[dbm.Base, SQLAQuery]]:
        """Convenience func to iter current selected row models (eg self.e)
        - local table data only, not from database

        Yields
        -------
        Union[dbm.Base, Tuple[dbm.Base, SQLAQuery]]
            - e OR (e, e_db)
            - e = local row data
            - e_db = database row data
        """
        data_model = self.data_model
        irows = [index.row() for index in self.selectedIndexes()]
        es = [data_model.create_model(i=i) for i in irows]  # data from local table only

        # get all SQL db row objs at once
        e_dbs = np.zeros(len(irows))  # just to have right shape for zipping
        if db:
            e_dbs = dbt.get_rowset_db(
                irows=irows,
                df=data_model.df,
                dbtable=self.parent.get_dbtable())

        for e, e_db in zip(es, e_dbs):
            if not db:
                yield e
            else:
                yield e, e_db

    def create_index_activerow(self, col_name: str = None, irow: int = None) -> QModelIndex:
        """Create QModelIndex from currently selected row

        Parameters
        ----------
        col_name : str, optional
            If col_name given, create index at that column\n
        irow : int, optional\n

        Returns
        -------
        QModelIndex
        """
        model = self.data_model
        if irow is None:
            irow = self.active_row_index()

        if col_name is None:
            icol = self.selectionModel().currentIndex().column()
        else:
            icol = model.get_col_idx(col_name)

        if None in (irow, icol):
            return None  # not in table, need to make sure to handle this when calling

        return model.createIndex(irow, icol)

    def get_irow_uid(self, uid):
        """Get irow from uid
        - TODO this is only for EventLog tables with UID, should probably adapt this for any key"""
        df = self.data_model.df
        return self.data_model.get_val_index(val=uid, col_name='UID')

    def active_row_index(self, warn=True, **kw) -> int:
        """Return index of currently selected row, or None"""
        irow = self.selectionModel().currentIndex().row()
        if irow > -1:
            return irow
        else:
            if warn:
                raise er.NoRowSelectedError()

            return None

    def row_from_activerow(self) -> dbt.Row:
        i = self.active_row_index()
        return dbt.Row(data_model=self.data_model, i=i)

    def model_from_activerow(self, **kw) -> dbm.Base:
        """only returns values in current table view, not all database"""
        i = self.active_row_index(**kw)
        return self.data_model.create_model(i=i)

    def model_from_activerow_db(self):
        # create model from db (to access all fields, eg MineSite)
        # NOTE this relies on MineSite being a field in Event Log > can also use db.get_df_unit() for model/minesite
        row = self.row_from_activerow()
        return row.create_model_from_db()  # TODO this won't work with mixed tables eg FCSummary

    def df_from_activerows(self, i: int = None) -> pd.DataFrame:
        """Get concat dataframe of all selected rows"""
        if i is None:
            idxs = list(self.iter_selected_idxs(irow_only=True))
            if idxs is None:
                return
        else:
            idxs = [i]

        return self.data_model.df.iloc[idxs]

    def nameindex_from_activerow(self):
        index = self.selectionModel().currentIndex()

        if index.isValid():
            return self.data_model.data(index=index, role=TableDataModel.NameIndexRole)

    def select_by_nameindex(self, name_index: tuple):
        """Reselect items by named index after model is sorted/filtered"""
        model = self.data_model
        # convert name_index to i_index
        index = model.data(name_index=name_index, role=TableDataModel.qtIndexRole)
        if index is None:
            index = model.createIndex(0, 0)

        self.select_by_index(index)

    def select_by_int(self, irow: int = 0, icol: int = 1):
        """Select table row by int number"""
        model = self.data_model
        max_row = model.rowCount() - 1
        max_col = model.columnCount() - 1

        if irow > max_row:
            irow = max_row
        if icol > max_col:
            icol = max_col

        index = model.createIndex(irow, icol)
        self.select_by_index(index=index)

    def select_by_index(self, index: QModelIndex):
        """Select table row by index"""
        sel = QItemSelection(index, index)
        self.setUpdatesEnabled(False)
        # | QItemSelectionModel.SelectionFlag.Rows)
        self.selectionModel().select(sel, QItemSelectionModel.SelectionFlag.ClearAndSelect)
        self.scrollTo(index)
        # make new index 'active'
        self.selectionModel().setCurrentIndex(index, QItemSelectionModel.SelectionFlag.Current)
        self.setUpdatesEnabled(True)

    def get_df_selection(self) -> pd.DataFrame:
        """Get dataframe of selected cells

        Returns
        -------
        pd.DataFrame
        """
        indexes = self.selectionModel().selectedIndexes()
        idx_min, idx_max = indexes[0], indexes[-1]

        return self.data_model.df.iloc[
            idx_min.row(): idx_max.row() + 1,
            idx_min.column(): idx_max.column() + 1]

    def copy(self) -> None:
        """Copy single value or selected cell(s) to clipboard"""
        sel = self.selectionModel()
        indexes = sel.selectedIndexes()  # list of selected index items

        if len(indexes) == 1:
            # just copy text from single cell
            index = indexes[0]
            s = str(index.data())
            msg = f.truncate(val=s, max_len=20)
            QApplication.clipboard().setText(s)

        elif len(indexes) > 1:
            # Capture selection into a DataFrame with max/min of selected indicies
            df = self.get_df_selection()
            # idx_min, idx_max = indexes[0], indexes[-1]
            # df = self.data_model.df.iloc[
            #     idx_min.row(): idx_max.row() + 1,
            #     idx_min.column(): idx_max.column() + 1]

            df.replace({'\n': ''}, regex=True).to_clipboard(index=False, excel=True)
            msg = f'rows: {df.shape[0]}, cols: {df.shape[1]}'

        self.update_statusbar(f'Cell data copied - {msg}')

    def fill_down(self):
        """Fill values from previous cell, or copy first value to all items in selection"""
        if self.read_only_all:
            raise er.ReadOnlyError()

        index = self.create_index_activerow()
        if index.row() == 0:
            return  # cant fill down at first row

        model = self.data_model
        indexes = self.selectedIndexes()  # list of selected index items

        if len(indexes) == 1:
            # single item selected, copy from above

            # warn user read-only column
            icol = index.column()
            if not icol in model.mcols['fill_enabled']:
                col = model.headerData(icol)
                msg = f'Warning: values in non fill-enabled column [{col}] not updated.'
                self.update_statusbar(msg)
                return

            index_copy = index.siblingAtRow(index.row() - 1)

            val = index_copy.data(role=model.RawDataRole)
            model.setData(index=index, val=val)
        else:
            # more than one cell selected
            # bin indexes into columns
            m_cols = dd(list)
            for idx in indexes:
                m_cols[idx.column()].append(idx)

            # warn user before overwriting too many values
            if len(indexes) > 6:
                num_cols = len(m_cols.keys())
                num_vals = len(indexes) - num_cols

                msg = f'WARNING: You are about to overwrite [{num_vals}] values in [{num_cols}] column(s). Are you SURE you would like to proceed?'  # noqa

                if not dlgs.msgbox(msg=msg, yesno=True):
                    return

            # model has disabled cols as ints, view as header strings
            m_disabled = {icol: model.headerData(icol) for icol in m_cols if not icol in model.mcols['fill_enabled']}

            # remove any locked cols from update and warn statusbar msg
            m_cols = {icol: lst for icol, lst in m_cols.items() if not icol in m_disabled}

            if m_disabled:
                msg = f'Warning: Values in non fill-enabled column(s) [{", ".join(m_disabled.values())}] not updated.'
                self.update_statusbar(msg)

            # Do the update
            model.lock_queue()
            for col_list in m_cols.values():
                # get value from first index in selection
                val = col_list[0].data(role=model.RawDataRole)

                # update everything other than the first
                for update_idx in col_list[1:]:
                    model.setData(index=update_idx, val=val, queue=True)

            model.flush_queue(unlock=True)

    def _icon(self, icon_name):
        # Convinence function to get standard icons from Qt
        if not icon_name.startswith('SP_'):
            icon_name = f'SP_{icon_name}'

        icon = getattr(QStyle.StandardPixmap, icon_name, None)

        if icon is None:
            raise Exception(f'Unknown icon {icon_name}')

        return self.style().standardIcon(icon)

    def _on_click(self, index):
        if index.isValid():
            self.cellClicked.emit(index.row(), index.column())

    def _enable_widgeted_cells(self):
        """- NOTE not set up yet"""

        return
        # Update all cells with WidgetedCell to have persistent editors
        model = self.data_model
        if model is None:
            return
        for r in range(model.rowCount()):
            for c in range(model.columnCount()):
                idx = model.index(r, c)
                # if isinstance(d, WidgetedCell):
                #     self.openPersistentEditor(idx)
                d = model.data(idx, TableDataModel.RawDataRole)

    def get_center_cols(self, *a, **kw):
        return self.mcols['center']

    def remove_row(self, i=None):
        """default, just remove row from table view"""
        if i is None:
            i = self.active_row_index()
        self.data_model.removeRows(i=i)

    def update_statusbar(self, msg, *args, **kw):
        if not self.mainwindow is None:
            self.mainwindow.update_statusbar(msg=msg, *args, **kw)

    def filter_column(self, col_name, val):
        """Toggle specific filter on column"""
        model = self.data_model
        name_index = self.nameindex_from_activerow()  # TODO probably connect this to a signal to use other places

        if not hasattr(self, 'filter_state') or not self.filter_state:
            model.filter_by_items(col=col_name, items=[str(val)])
            self.filter_state = True
        else:
            model.reset_filter()
            self.filter_state = False

        if not name_index is None:
            self.select_by_nameindex(name_index=name_index)

    def jump_top_bottom(self):
        """Jump to top or bottom of currently active table"""
        num_rows = self.data_model.rowCount()
        cur_row = self.active_row_index(warn=False)
        cur_col = self.selectionModel().currentIndex().column()
        if not cur_col > -1:
            cur_col = 1
        max_row = num_rows - 1

        if cur_row is None:
            self.select_by_int()
        else:
            midpoint = num_rows // 2
            # if closer to bottom, but not bottom, jump bottom
            if (cur_row == 0 or num_rows - cur_row < midpoint) and not cur_row == max_row:
                self.select_by_int(irow=max_row, icol=cur_col)  # jump bottom
            else:
                self.select_by_int(irow=0, icol=cur_col)  # jump top


class TableWidget(QWidget):
    """Controls TableView & buttons/actions within tab"""

    def __init__(
            self,
            parent: Union['TabWidget', None] = None,
            refresh_on_init: bool = True,
            name: Union[str, None] = None,
            **kw):
        super().__init__(parent)

        if name is None:
            name = self.__class__.__name__
        self.name = name
        self.persistent_filters = []  # el/wo usergroup filters

        if not parent is None:
            self.mainwindow = parent.mainwindow  # type: MainWindow
            self.settings = self.mainwindow.settings
        else:
            self.mainwindow = gbl.get_mainwindow()  # type: MainWindow
            self.settings = gbl.get_settings()

        self.title = cf.config['TableName']['Class'][name]

        self.context_actions = dd(
            list,
            refresh=['refresh', 'refresh_all_open', 'reload_last_query'],
            details=['details_view'])

        v_layout = QVBoxLayout(self)
        btnbox = QHBoxLayout()
        btnbox.setAlignment(Qt.AlignmentFlag.AlignLeft)
        v_layout.addLayout(btnbox)

        # get default refresh dialog from refreshtables by name
        self.refresh_dialog = getattr(rtbls, name, rtbls.HBABase)  # type: Type[rtbls.RefreshTable]

        self.query = getattr(qr, name, HBAQueryBase)(parent=self, theme='dark', name=self.name)
        self.dbtable = self.query.update_table  # type: dbm.Base
        db_col_map = {}

        # try getting inner-classed tableview, if not use default
        _view = getattr(self, 'View', TableView)(parent=self)
        v_layout.addWidget(_view)

        f.set_self(vars())

        self.add_action(name='Refresh', btn=True, func=self.show_refresh, tooltip='Show Refresh menu')

    @property
    def mw(self) -> 'MainWindow':
        return self.mainwindow

    @property
    def minesite(self) -> str:
        return self.mainwindow.minesite if not self.mainwindow is None else 'FortHills'  # default for testing

    @property
    def u(self):
        """Return mainwindow user, else default"""
        if not self.mainwindow is None:
            return self.mainwindow.u
        else:
            from guesttracker.users import User
            return User.default()

    @property
    def view(self) -> TableView:
        return self._view

    @property
    def e(self):
        return self.view.e

    @property
    def e_db(self):
        return self.view.e_db

    @property
    def i(self):
        return self.view.i

    @property
    def row(self) -> dbt.Row:
        return self.view.row

    def add_action(
            self,
            name: str,
            func: Callable,
            btn: bool = False,
            ctx: Union[str, None] = None,
            tooltip: Union[str, None] = None,
            **kw) -> None:

        name_action = name.replace(' ', '_').lower()

        if self.mw is None:
            return  # not GUI

        act = self.mw.add_action(name=name, func=func, tooltip=tooltip, parent=self, **kw)

        if btn:
            self.add_button(act=act, tooltip=tooltip)

        if ctx:
            self.context_actions[ctx].append(name_action)

    def add_button(self, name=None, func=None, act=None, tooltip=None):
        if not act is None:
            name = act.text()
            func = act.triggered

        btn = QPushButton(name, self)
        btn.setMinimumWidth(60)
        btn.setToolTip(tooltip)
        btn.clicked.connect(func)
        self.btnbox.addWidget(btn)

    def show_refresh(self, *args, **kw):
        """Show RefreshTable dialog and restore prev settings"""
        dlg = self.refresh_dialog(parent=self, name=self.name)
        dlg.exec()

    def show_details(self):
        """Show details view dialog"""
        row = self.view.row_from_activerow()

        model = row.create_model_from_db()
        df = dbt.df_from_row(model=model)

        # load to details view
        dlg = dlgs.DetailsView(parent=self, df=df)
        dlg.exec()

    def show_smr_history(self):
        """Show SMR history dialog for selected unit"""
        unit = self.view.create_index_activerow('Unit').data()

        dlg = UnitSMRDialog(unit=unit, parent=self)
        dlg.exec()

    def refresh_lastweek(self, base=True):
        """NOTE default means 'didn't come from refresh menu'"""
        if not self.query.set_lastweek():
            self.mainwindow.warn_not_implemented()
            return

        self.refresh(base=base, save_query=False)

    def refresh_lastmonth(self, base=True):
        # self.sender() = PyQt6.QtGui.QAction > could use this to decide on filters
        if not self.query.set_lastmonth():
            self.mainwindow.warn_not_implemented()
            return

        # kinda sketch, but base = default - allopen
        self.refresh(base=base, save_query=False)

    def refresh_allopen(self, default=False):
        query = self.query
        if hasattr(query, 'set_allopen'):
            query.set_allopen()

        self.refresh(default=default, save_query=False)

    @er.errlog(display=True)
    def refresh(self, **kw):
        """Load dataframe to table view
        - RefreshTable dialog will have modified query's fltr"""
        self.update_statusbar('Refreshing table, please wait...')
        self.mainwindow.app.processEvents()

        # Add persistent filter items to query
        for field in self.persistent_filters:
            dlgs.add_items_to_filter(field=field, fltr=self.query.fltr)

        df = self.query.get_df(**kw)

        if not df is None and not len(df) == 0:
            view = self.view

            # don't want to save state for first run after col layout reset
            if view.is_init:
                view.save_header_state()

            data_display_time = view.display_data(df=df)
            view.is_init = True

            if self.mainwindow.get_setting('show_query_time'):
                # update statusbar with total query time taken
                msg = 'Query time: {:.1f}s, Display time: {:.1f}s, Total: {:.1f}s'.format(
                    self.query.data_query_time,
                    data_display_time,
                    self.query.data_query_time + data_display_time)

                self.update_statusbar(msg)
            else:
                self.mainwindow.revert_status()
        else:
            self.update_statusbar(msg='No rows returned in query.', warn=True)

    def get_dbtable(self, header: str = None) -> Type[dbm.Base]:
        """return dbtable (definition) for specific header"""
        m = self.db_col_map
        dbtable = self.dbtable if header is None or not header in m else getattr(dbm, m[header])
        return dbtable

    def email_table(
            self,
            subject: str = '',
            body: str = '',
            email_list: List[str] = None,
            df: pd.DataFrame = None,
            prompts: bool = True,
            prompt_attach: bool = True,
            prompt_include: bool = True,
            selection: bool = False):
        """Email any table in outlook.
        - TODO make this an input dialog so settings can be remembered?

        Parameters
        ----------
        subject : str, optional
            Email subject, by default ''
        body : str, optional
            Body text, will have html table inserted, by default ''
        email_list : List[str], optional
            List of email addresses, by default None
        df : pd.DataFrame, optional
            df to email, by default None
        prompts : bool, optional
            convers both attach and incude
        prompt_attach : bool, optional
            prompt to attach excel file, by default True
        prompt_include : bool, optional
            prompt to include table in email body
        """
        if not prompts:
            prompt_attach = False
            prompt_include = False

        if selection:
            df = self.view.get_df_selection()
            prompt_include = False  # dont prompt if only emailing selection

        style = self.view.get_style(df=df, outlook=True)  # this will have all colors in current GUI table

        msg_ = 'Include table in email body?'
        style_body = ''

        if not prompt_include or (prompt_include and dlgs.msgbox(msg=msg_, yesno=True)):
            # style.data = self.view.data_model.get_df_display(df=df)  # set string format df
            style_body = style \
                .hide(axis='index').render()

        body = f'{body}<br><br>{style_body}'  # add in table to body msg

        # create new email
        msg = em.Message(subject=subject, body=body, to_recip=email_list, show_=False)

        if prompt_attach:
            msg_ = 'Would you like to attach an excel file of the data?'
            if dlgs.msgbox(msg=msg_, yesno=True):
                p = self.save_df(style=style, name=self.name, ext='xlsx')
                msg.add_attachment(p)
                p.unlink()

        msg.show()

    def email_row(
            self,
            title: str,
            email_list: List[str] = None,
            body_text: str = '',
            exclude_cols: List[str] = None,
            lst_attach: List[Path] = None,
            df: pd.DataFrame = None) -> None:
        """Create email with df from selected row(s)

        Parameters
        ----------
        title : str
            email title
        email_list : List[str], optional
            list of emails, by default None
        body_text : str, optional
            email body text, by default ''
        exclude_cols : List[str], optional
            exclude columns from dataframe, by default None
        lst_attach : List[Path], optional
            list of files to attach, by default None
        df : pd.DatFrame, optional
            use passed in df instead of current row (to add extra info)
        """

        if df is None:
            df = self.view.df_from_activerows() \
                .drop(columns=exclude_cols)

        if df is None:
            return

        formats = {'int64': '{:,}', 'datetime64[ns]': st.format_date}
        style = st.default_style(df=df, outlook=True) \
            .pipe(st.apply_formats, formats=formats) \
            .pipe(st.set_borders)

        body = f'{f.greeting()}{body_text}<br><br>{style.hide(axis="index").to_html()}'

        # show new email
        msg = em.Message(subject=title, body=body, to_recip=email_list, show_=False)
        msg.add_attachments(lst_attach=lst_attach)
        msg.show()

    def save_df(self, style=None, df=None, p=None, name='temp', ext='xlsx'):
        if p is None:
            p = Path.home() / f'Desktop/{name}.{ext}'

        if ext == 'xlsx':
            style.to_excel(p, index=False, freeze_panes=(1, 0))
        elif ext == 'csv':
            df.to_csv(p, index=False)

        return p

    def export_excel(self):
        self.export_df(ext='xlsx')

    def export_csv(self):
        self.export_df(ext='csv')

    def export_df(self, ext='xlsx'):
        """Export current table as excel/csv file, prompt user for location"""

        p = dlgs.save_file(name=self.title, ext=ext)
        if p is None:
            return
        p_try = p

        kw = dict(name=self.name, p=p, ext=ext)

        if ext == 'xlsx':
            kw['style'] = self.view.get_style(df=None, outlook=True)
        elif ext == 'csv':
            kw['df'] = self.view.data_model.df

        p = self.save_df(**kw)
        if not p is None:
            msg = f'File created:\n\n{p}\n\nOpen now?'
            if dlgs.msgbox(msg=msg, yesno=True):
                fl.open_folder(p=p, check_drive=False)
        else:
            msg = f'Error: File not created: {p_try}'
            self.update_statusbar(msg)

    def remove_row(self):
        """Default, just remove from table view (doesn't do anything really)"""
        self.view.remove_row()

    def _remove_row(self, name: str, warn_keys: List[str]) -> None:
        """Warn user before removing row from db

        Parameters
        ----------
        name : str
            item name to display in messages
        warn_keys : List[str]
            keys to show warning for (must be in current table)
        """
        view, e, row = self.view, self.e, self.row

        m = {k: getattr(e, k, None) for k in warn_keys}
        m_pretty = f.pretty_dict(m)

        msg = f'Are you sure you would like to permanently delete the {name.lower()} record:\n\n{m_pretty}'

        if dlgs.msgbox(msg=msg, yesno=True):
            if row.update(delete=True):
                view.remove_row(i=row.i)
                self.update_statusbar(f'{name.title()} removed from database - \n\n{m_pretty}', success=True)
            else:
                self.update_statusbar(f'Error: {name.title()} not deleted from database.')

    def update_statusbar(self, msg, *args, **kw):
        if not self.mainwindow is None:
            self.mainwindow.update_statusbar(msg=msg, *args, **kw)

    def check_cummins(self) -> bool:
        """Return True if not cummins user"""
        if not self.u.is_cummins:
            return True
        else:
            self.warn_not_implemented(cummins=True)
            return False

    def warn_not_implemented(self, cummins=False):
        if not cummins:
            msg = 'Sorry, this feature not yet implemented.'
        else:
            msg = 'Sorry, this feature not enabled for cummins.'

        self.update_statusbar(msg=msg)

    def save_persistent_filter_settings(self):
        """Save UserGroup filter settings
        - NOTE this will need to be restructured if we add more filters"""
        if not self.persistent_filters:
            return
        s = self.settings

        for field in self.persistent_filters:
            items = [field.box, field.cb]
            for obj in items:
                val = obj.val

                if not val is None:
                    s.setValue(f'tbl_{self.name}_{obj.objectName()}', val)

    def restore_persistent_filter_settings(self):
        """Restore UserGroup filter settings"""
        if not self.persistent_filters:
            return
        s = self.settings

        for field in self.persistent_filters:
            items = [field.box, field.cb]
            for obj in items:
                name, val = obj.objectName(), None

                val = s.value(f'tbl_{self.name}_{name}')
                if not val is None:
                    obj.val = val

    def open_tsi(self, status: str = 'open', index: QModelIndex = None) -> None:
        """Create TSI from event

        Parameters
        ----------
        status : str, optional
            new tsi status, default 'open'
        index : QModelIndex, optional
            pass in from combobox func
        """
        if not self.title in ('Event Log', 'Work Orders', 'Component CO', 'TSI'):
            msg = 'Please chose a row from the Event Log or Work Orders tab.'
            dlgs.msg_simple(msg=msg, icon='warning')
            return

        e, row = self.e, self.row
        if row is None:
            return

        title = self.title
        if status == 'open' and not title == 'TSI':
            # update raw db vals
            author = self.mw.username
            row.update(vals=dict(StatusTSI='Open', TSIAuthor=author))
            self.update_statusbar(msg=f'TSI opened for: {e.Unit} - {e.Title}, Author: {author}', success=True)

        elif status == 'closed' and title == 'TSI':
            # update by TableView, only set date submitted from TSI tab
            d = dt.now().date()
            model = self.view.data_model

            update_index = index.siblingAtColumn(model.get_col_idx('Date Submitted'))
            if not update_index.data():
                model.setData(index=update_index, val=d, triggers=False, update_db=True)
                self.update_statusbar(msg=f'TSI submission date set: {d}', success=True)

    def update_tsi_combobox(self, index: QModelIndex, val_new: str, **kw) -> None:
        """Open/close TSI by combobox, eg from EventLog tab

        Parameters
        ----------
        index : QModelIndex
        val_new : str
            New TSI status value set
        """
        val_new = str(val_new).lower()
        if not val_new in ('open', 'closed'):
            return

        self.open_tsi(status=val_new, index=index)


class HBATableWidget(TableWidget):
    def __init__(self, name: str, parent=None):
        super().__init__(parent=parent, name=name)
        self.add_action(
            name='Add New',
            func=self.show_addrow,
            btn=True,
            ctx='add',
            tooltip='Add new event')

    class View(TableView):
        def __init__(self, parent: TableWidget):
            super().__init__(parent=parent)
            self.mcols['hide'] = ('uid',)

    def show_addrow(self):
        cls = getattr(adr, self.name, adr.HBAAddRow)
        dlg = cls(parent=self, name=self.name)
        dlg.exec()

    def remove_row(self):
        """Remove selected part from database
        """
        warn_keys = dbc.table_data[self.name].get('warn_delete_fields', ['name'])
        self._remove_row(name=self.name, warn_keys=warn_keys)


class Units(HBATableWidget):
    class View(HBATableWidget.View):
        def __init__(self, parent: HBATableWidget):
            super().__init__(parent=parent)

            lists = cf.config['Lists']
            self.set_combo_delegate(col='Active', items=lists['TrueFalse'], allow_blank=False)


class EventLogBase(TableWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.add_action(
            name='Add New',
            func=self.show_addrow,
            btn=True,
            ctx='add',
            tooltip='Add new event')
        self.add_action(
            name='Link FC',
            func=self.link_fc,
            ctx='fc',
            tooltip='Link selected event to FC')

        self.context_actions['refresh'].extend(['refresh_all_open', 'refresh_last_week', 'refresh_last_month'])
        self.context_actions['view'] = ['view_folder']
        self.context_actions['smr'] = ['show_smr_history', 'update_smr']
        self.context_actions['tsi'] = ['open_tsi']

    class View(TableView):
        def __init__(self, parent: TableWidget):
            super().__init__(parent=parent)
            self.add_col_funcs(['Work Order', 'Title', 'Date Added'], self.update_eventfolder_path)
            self.add_col_funcs('Status', self.parent.close_event)
            self.highlight_funcs['Status'] = self.highlight_by_val
            self.highlight_funcs['Pics'] = self.highlight_pics
            self.mcols['hide'] = ('UID',)
            self.mcols['longtext'] = ('Description', 'Failure Cause', 'Comments', 'Details', 'Notes')
            self.mcols['disabled'] = ('Model', 'Serial')
            self.mcols['no_space'] = ('Part Number',)
            self.mcols['fill_enabled'] = ('Passover', 'Status', 'Removal Reason',
                                          'Issue Category', 'Sub Category', 'Failure Category')
            self.highlight_vals |= {
                'closed': 'goodgreen',
                'open': 'bad',
                'action required': 'bad',
                'complete': 'goodgreen',
                'work in progress': 'lightorange',
                'waiting customer': 'lightorange',
                'cancelled': 'lightorange',
                'monitor': 'lightyellow',
                'planned': 'lightyellow',
                'waiting costing': 'lightyellow',
                'pre-open': 'lightblue',
                'waiting parts (up)': 'lightyellow',
                'missing info': 'lightyellow',
                'waiting review': 'lightorange',
                'in progress': 'lightorange',
                'waiting parts (down)': 'bad',
                'x': 'good'}

            self.formats |= {
                'Unit SMR': '{:,.0f}',
                'Comp SMR': '{:,.0f}',
                'Part SMR': '{:,.0f}',
                'SMR': '{:,.0f}'}

            self.col_widths |= {
                'Unit SMR': 60,
                'SMR': 60,
                'Comp SMR': 60,
                'Bench SMR': 60,
                'Part SMR': 60,
                'Life Achieved': 60}

            items = cf.config['Lists'][f'{self.parent.name}Status']
            self.set_combo_delegate(col='Status', items=items, allow_blank=False)

        # def update_eventfolder_path(self, index: QModelIndex, val_prev, **kw):
        #     """Update event folder path when Title/Work Order/Date Added changed

        #     Parameters
        #     ----------
        #     index : QModelIndex\n
        #     val_prev :
        #         Value before changed
        #     """
        #     if self.u.is_cummins:
        #         return

        #     e = self.e
        #     minesite = db.get_unit_val(e.Unit, 'MineSite')

        #     # get header from index
        #     header = index.model().headerData(i=index.column()).replace(' ', '').lower()
        #     if header == 'codate':
        #         header = 'dateadded'  # little hack for component CO table

        #     ef = efl.EventFolder \
        #         .from_model(e=e, data_model=self.data_model, irow=index.row(), table_widget=self.parent) \
        #         .update_eventfolder_path(vals={header: val_prev})

    def show_addrow(self):
        dlg = AddEvent(parent=self)
        dlg.exec()

    def show_component(self, **kw):
        dlg = dlgs.ComponentCO(parent=self)
        dlg.exec()

    def close_event(self, index: QModelIndex, **kw):
        """Set DateCompleted to current date when status change to 'Closed' or 'Completed' (EL + WO only)\n
        Notes
        ---
        - would be more ideal to queue change at start of setData and update all vals in bulk
        - could auto link those changes through setData eg auto_update_sibling_table"""
        title = self.title
        if not title in ('Event Log', 'Work Orders'):
            return

        view, e, row = self.view, self.e, self.row

        if index.data() in ('Closed', 'Complete'):
            # update both StatusEvent and StatusWO in db
            d = dt.now().date()
            m = {'Event Log': dict(StatusEvent='Complete'), 'Work Orders': dict(StatusWO='Closed')}
            vals = dict(DateCompleted=d)

            # only update StatusEvent or StatusWO based on current table
            vals |= m.pop(title)

            # also close other tab if enabled
            if gbl.get_setting('close_wo_with_event', False):
                other_table = list(m.keys())[0]
                vals |= m.get(other_table, {})

            # set dateclosed in table view but don't update
            model = index.model()
            update_index = index.siblingAtColumn(model.get_col_idx('Date Complete'))

            # Update if DateComplete is null
            if not update_index.data():
                model.setData(index=update_index, val=d, triggers=False, update_db=False)

            row.update(vals=vals)
            self.update_statusbar(msg=f'Event closed: {e.Unit} - {d.strftime("%Y-%m-%d")} - {e.Title} ')

        elif index.data() == 'Cancelled':
            # check if event has linked FC and unlink
            e_fc = self.get_e_fc(uid=e.UID)
            self.unlink_fc(e_fc=e_fc)

    # def view_folder(self):
    #     """Open event folder of currently active row in File Explorer/Finder"""
    #     if not self.check_cummins():
    #         return

    #     view, i, e = self.view, self.i, self.e_db

    #     try:
    #         minesite = db.get_unit_val(e.Unit, 'MineSite')
    #     except KeyError:
    #         self.update_statusbar(
    #             f'Warning: Could not get minesite for unit: "{e.Unit}". Does it exist in the database?')
    #         return

    #     # Fix ugly titles if needed
    #     title = e.Title
    #     title_good = f.nice_title(title=title)

    #     if not title == title_good:
    #         view.set_value(col_name='Title', val=title_good, uid=e.UID, irow=i)
    #         self.update_statusbar(f'Title fixed: {title_good}', success=True)

    #     efl.EventFolder.from_model(e=e, irow=i, data_model=view.data_model).show()

    # def view_dls_folder(self) -> None:
    #     """View current unit's dls folder
    #     """
    #     efl.UnitFolder.from_model(e=self.e).show(dls=True)

    def get_e_fc(self, uid: float):
        """Try to select FC from FactoryCampaing table by UID"""
        return dbt.select_row_by_secondary(dbtable=dbm.FactoryCampaign, col='UID', val=uid)

    # def link_fc(self):
    #     """Show available FCs dialog, link selected event to FC"""
    #     if not self.check_cummins():
    #         return

    #     # get selected row
    #     view, i, e = self.view, self.i, self.e_db
    #     model = view.model

    #     # show fc dialog
    #     ok, fc_number, title = fc.select_fc(unit=e.Unit)

    #     if not ok:
    #         return

    #     fc.link_fc_db(unit=e.Unit, uid=e.UID, fc_number=fc_number)

    #     # update current title
    #     view.set_value(col_name='Title', val=title, uid=e.UID, irow=i)

    #     self.update_statusbar(f'Event linked to FC: {e.Unit}, {title}', success=True)

    def unlink_fc(self, e_fc):
        """Unlink FC from event by setting e_fc UID to None"""
        if e_fc is None:
            return

        e_fc.UID = None
        e_fc.DateCompleteSMS = None
        if db.safe_commit():
            self.update_statusbar(f'{e_fc.Unit} - FC {e_fc.FCNumber} unlinked from event.')

    def remove_row(self):
        """Remove selected event from table and delete from db"""
        view, e, row = self.view, copy.deepcopy(self.e), self.row

        m = dict(Unit=e.Unit, DateAdded=e.DateAdded, Title=e.Title)
        msg = f'Are you sure you would like to permanently delete the event:\n\n{f.pretty_dict(m)}'

        if dlgs.msgbox(msg=msg, yesno=True):

            # Check if Event is linked to FC, ask to unlink
            e_fc = self.get_e_fc(uid=e.UID)
            if not e_fc is None:
                msg = f'This event is linked to FC {e_fc.FCNumber}, would you like to unlink the FC?'
                if dlgs.msgbox(msg=msg, yesno=True):
                    self.unlink_fc(e_fc=e_fc)
                else:
                    self.update_statusbar('Warning: Can\'t delete event with linked FC.')
                    return

            if row.update(vals=dict(deleted=True)):
                view.remove_row(i=row.i)
                self.update_statusbar(f'Event removed from database: {e.Unit} - {e.Title}')

                # ask to delete event folder
                # if self.u.usergroup == 'SMS':
                #     ef = efl.EventFolder.from_model(e)
                #     if ef.exists:
                #         msg = f'Found event folder containing ({ef.num_files}) files/folders, would you like to delete? This cannot be undone.'  # noqa
                #         if dlgs.msgbox(msg=msg, yesno=True):
                #             if ef.remove_folder():
                #                 self.update_statusbar('Event folder successfully removed.')
            else:
                self.update_statusbar('Error: Event not deleted from database.')

    def get_wo_from_email(self) -> None:
        """Find WO for selected row in email inbox, write back to table"""

        if not self.check_cummins():
            return
        e = self.e_db

        from guesttracker.utils.outlook import Outlook

        ol = Outlook()
        wo = ol.get_wo_from_email(unit=e.Unit, title=e.Title)
        self.handle_wo_result(wo=wo, uid=e.UID)

    def handle_wo_result(self, wo: str, uid: int = None):
        """Get WO from worker thread, write back to table
        """

        if not wo is None:
            # write wo back to table/db
            # NOTE not DRY, could put this into a func
            view = self.view
            msg = f'WO number found in outlook: {wo}'
            self.update_statusbar(msg=msg, success=True)
            irow = view.get_irow_uid(uid=uid)

            if not irow is None:
                index = view.create_index_activerow(irow=irow, col_name='Work Order')
                view.data_model.setData(index=index, val=wo)
            else:
                dbt.Row(dbtable=self.get_dbtable(), keys=dict(UID=uid)) \
                    .update(vals=dict(WorkOrder=wo))
        else:
            msg = 'No WO found in outlook for selected event.'
            self.update_statusbar(msg=msg, warn=True)

    def update_smr(self):
        row, e = self.row, self.e_db

        cur_smr = e.SMR
        e_smr = db.session.query(dbm.UnitSMR).get(dict(Unit=e.Unit, DateSMR=e.DateAdded))

        if e_smr is None:
            msg = f'No SMR value found for Unit {e.Unit} on {e.DateAdded:%Y-%m-%d}'
            dlgs.msg_simple(msg=msg, icon='warning')
            return

        if not pd.isnull(cur_smr):
            msg = f'Found existing SMR: {cur_smr:,.0f}\n\nOverwrite?'
            if not dlgs.msgbox(msg=msg, yesno=True):
                return

        col_name = 'SMR' if self.title == 'Work Orders' else 'Unit SMR'
        index = self.view.create_index_activerow(col_name=col_name)
        self.view.data_model.setData(index=index, val=e_smr.SMR)
        self.update_statusbar(f'SMR updated: {e_smr.SMR}')

    def jump_event(self):
        """Jump to selected event in EventLog or WorkOrders table"""
        e, mw = self.e, self.mainwindow

        m = {'Event Log': 'Work Orders', 'Work Orders': 'Event Log'}
        other_title = m.get(self.title, None)
        if other_title is None:
            return

        table_widget = mw.tabs.get_widget(title=other_title)
        model = table_widget.view.data_model
        irow = model.get_val_index(val=e.UID, col_name='UID')

        if not irow is None:
            mw.setUpdatesEnabled(False)
            mw.tabs.activate_tab(title=other_title)
            table_widget.view.select_by_int(irow=irow, icol=1)
            mw.setUpdatesEnabled(True)
        else:
            self.update_statusbar(
                f'Couldn\'t find matching row in [{other_title}] table. Make sure row exists in table.')

    def add_usergroup_filter(self):
        """Add QCombobox and checkbox to right side of btnbox bar"""
        def _toggle(state):
            # toggle input field enabled/disabled based on checkbox
            # TODO needs more DRY
            source = self.sender()
            box = source.box

            if Qt.CheckState(state) == Qt.CheckState.Checked:
                box.setEnabled(True)
                box.select_all()
            else:
                box.setEnabled(False)

            self.save_persistent_filter_settings()

        items = db.domain_map.keys()
        text = 'User Group'
        box = ff.ComboBox(items=items, enabled=False, name=text)
        cb = ff.CheckBox(name=text)
        cb.box = box
        cb.stateChanged.connect(_toggle)
        label = QLabel('User Group:')
        label.setToolTip('This is a global filter to limit all records returned to only users in selected User Group.')

        box_layout = QHBoxLayout()
        box_layout.setAlignment(Qt.AlignmentFlag.AlignRight)
        box_layout.addWidget(label)
        box_layout.addWidget(cb)
        box_layout.addWidget(box)
        self.btnbox.addStretch(1)
        self.btnbox.addLayout(box_layout)

        # create inputfield for later filtering
        field = dlgs.InputField(
            text=text,
            default=db.domain_map_inv.get(self.u.domain, 'SMS'),
            table='UserSettings')

        # set so can call later in save/restore settings
        field.box = box
        field.cb = cb
        self.persistent_filters.append(field)
        self.restore_persistent_filter_settings()


class EventLog(EventLogBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.add_action(
            name='Jump WorkOrders',
            func=self.jump_event,
            btn=True,
            ctx='view',
            tooltip='Jump to selected event in [Work Orders] table.')
        self.add_action(
            name='Filter Passover',
            func=lambda x: self.view.filter_column('Passover', 'x'),
            btn=True,
            ctx='passover',
            tooltip='Filter table to rows marked "x" in Passover column.')
        self.add_action(name='Email Passover', func=self.email_passover, btn=True, ctx='passover',
                        tooltip='Create new email with all rows marked "x" for passover.')

        self.context_actions['smr'] = ['show_smr_history']  # clear update_smr from menu

        self.add_usergroup_filter()

    class View(EventLogBase.View):
        def __init__(self, parent):
            super().__init__(parent=parent)
            self.col_widths |= {
                'Passover': 50,
                'Description': 800,
                'Status': 100,
                'Failure Cause': 150}

            self.mcols['time'] = ('Time Called',)
            self.mcols['highlight_dates'] = ('Description',)
            self.mcols['tooltip'] = {
                'Description': 'HINT: Use "ctrl + shift + i" to insert current date',
            }

            self.highlight_funcs['Passover'] = self.highlight_by_val
            self.highlight_funcs['TSI'] = self.highlight_by_val
            self.set_combo_delegate(col='Passover', items=['x'])

            self.set_combo_delegate(col='Issue Category', items=db.get_issues())
            self.set_combo_delegate(col='Sub Category', dependant_col='Issue Category')

            items = sorted(cf.config['Lists']['FailureCategory'])
            self.set_combo_delegate(col='Failure Category', items=items)

            items = cf.config['Lists']['TSIStatus']
            self.set_combo_delegate(col='TSI', items=items, allow_blank=True)

            self.add_col_funcs('TSI', self.parent.update_tsi_combobox)

        def get_style(self, df=None, outlook=False):
            return super().get_style(df=df, outlook=outlook) \
                .pipe(st.set_column_widths, vals=dict(Status=80, Description=400, Title=100), outlook=outlook)

    def email_passover(self):
        """Email current passover rows as table in outlook."""
        shift = 'DS' if 8 <= dt.now().hour <= 20 else 'NS'
        d = dt.now().date().strftime('%Y-%m-%d')
        shift = f'{d} ({shift})'

        df = self.view.data_model.df

        cols = ['MineSite', 'Status', 'Unit', 'Title', 'Description', 'Date Added']
        df = df[df.Passover.str.lower() == 'x'] \
            .sort_values(by=['MineSite', 'Unit', 'Date Added'])[cols]

        df.Description = df.Description.apply(self.remove_old_dates)

        usergroup = self.u.usergroup

        dfu_cols = ['Customer', 'Model', 'Unit']
        cols = ['Status'] + dfu_cols + cols[3:]

        # send separate emails for each minesite
        for minesite, df_mine in df.groupby('MineSite'):
            df_mine = df_mine.pipe(pu.safe_drop, cols=['MineSite'])

            dfu = db.get_df_unit()[dfu_cols] \
                .rename_axis('index')

            df_mine = df_mine \
                .reset_index(drop=False) \
                .merge(right=dfu, how='left', on='Unit') \
                .set_index('index')[cols]

            subject = f'{usergroup} Passover {minesite} - {shift}'
            body = f'{f.greeting()}Please see updates from {shift}:<br>'

            email_list = qr.EmailListShort(col_name='Passover', minesite=minesite, usergroup=usergroup).emails

            self.email_table(subject=subject, body=body, email_list=email_list, df=df_mine, prompts=False)

    def remove_old_dates(self, s):
        """Split description on newlines, remove old dates if too long, color dates red"""
        if not isinstance(s, str):
            return s
        if s.strip() == '':
            return s

        lst = s.splitlines()
        cur_len, max_len = 0, 400

        for i, item in enumerate(lst[::-1]):
            cur_len += len(item)
            if cur_len >= max_len:
                break

        lst = lst[max(len(lst) - i - 1, 0):]

        # color dates red
        date_reg_exp = re.compile(r'(\d{4}[-]\d{2}[-]\d{2})')
        replace = r'<span style="color: red;">\1</span>'
        lst = [re.sub(date_reg_exp, replace, item) for item in lst]

        return '\n'.join(lst)


class WorkOrders(EventLogBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.add_action(
            name='Jump EventLog',
            func=self.jump_event,
            btn=True,
            ctx='view',
            tooltip='Jump to selected event in [Event Log] table.')

        self.add_action(
            name='Get WO from Outlook',
            func=self.get_wo_from_email,
            btn=False,
            ctx='wo',
            tooltip='Create new WO Request email in outlook.')

        self.add_action(name='Email WO Open', func=self.email_wo_request, btn=True, ctx='wo',
                        tooltip='Create new WO Request email in outlook.')
        self.add_action(name='Email WO Close', func=self.email_wo_close, btn=True, ctx='wo',
                        tooltip='Create new WO Close email in outlook.')

        self.add_usergroup_filter()

    class View(EventLogBase.View):
        def __init__(self, parent):
            super().__init__(parent=parent)
            self.col_widths |= {
                'Status': 60,
                'Wrnty': 60,
                'Work Order': 90,
                'Customer WO': 80,
                'Customer PO': 90,
                'Order Parts': 200,
                'Comp CO': 50,
                'Comments': 800,
                'Seg': 30,
                'Pics': 40}

            self.add_col_funcs('Comp CO', self.set_component)
            self.highlight_funcs['Comp CO'] = self.highlight_by_val
            self.mcols['highlight_dates'] = ('Comments',)

            lists = cf.config['Lists']
            wrnty_list = 'WarrantyType' if not self.u.is_cummins else 'WarrantyTypeCummins'
            self.set_combo_delegate(col='Wrnty', items=lists[wrnty_list])
            self.set_combo_delegate(col='Comp CO', items=lists['TrueFalse'], allow_blank=False)

        def set_component(self, val_new, **kw):
            if val_new is True:
                self.parent.show_component()

    def email_wo(self, title: str, body_text: str, exclude_cols: List[str]) -> None:
        """Email a WorkOrder (Open|Close) for the currently selected row

        Parameters
        ----------
        title : str
            email title
        body_text : str
        exclude_cols : List[str]
            df cols to exclude from row
        """
        e = self.e
        title = f'{title} - {e.Unit} - {e.Title}'

        m = {item: item for item in ['PRP', 'RAMP', 'Service', 'Parts']}
        m |= {'No': 'Service'}
        name = m.get(e.WarrantyYN, 'WO Request')

        lst = qr.EmailListShort(col_name=name, minesite=self.minesite, usergroup=self.u.usergroup).emails

        df = self.view.df_from_activerows() \
            .drop(columns=exclude_cols) \
            .reset_index(drop=True)

        # add new cols and reorder
        add_cols = ['Customer']
        i = df.columns.get_loc('Customer PO') + 1
        cols = df.columns.tolist()
        new_cols = cols[:i] + add_cols + cols[i:]

        # add MineSite to row
        df = df.pipe(f.left_merge, db.get_unit_val(unit=df.Unit.iloc[0], field=add_cols))[new_cols]

        self.email_row(title=title, body_text=body_text, exclude_cols=exclude_cols, email_list=lst, df=df)

    def email_wo_request(self):
        """Email a WorkOrder request for the currently selected row"""
        e = self.e

        m = dict(yes='warranty', no='non-warranty')
        wrnty_type = m.get(e.WarrantyYN.lower(), e.WarrantyYN)

        self.email_wo(
            title='Open WO Request',
            body_text=f'Please open a {wrnty_type} work order for:',
            exclude_cols=['UID', 'Status', 'Work Order', 'Seg', 'Date Complete', 'Pics'])

    def email_wo_close(self):
        """Send email to close event"""

        # set status to closed, will trigger 'close event'
        index = self.view.create_index_activerow(col_name='Status')
        self.view.data_model.setData(index=index, val='Closed')

        self.email_wo(
            title='Close WO Request',
            body_text='Please close the following work order:',
            exclude_cols=['UID', 'Pics'])


class ComponentCO(EventLogBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.context_actions['smr'] = ['update_smr']

    class View(EventLogBase.View):
        def __init__(self, parent):
            super().__init__(parent=parent)

            self.mcols['disabled'] = ('MineSite', 'Model', 'Unit', 'Component', 'Side', 'Bench SMR', 'Life Achieved')
            self.mcols['tooltip'] = {
                'Install SMR': 'Set hours if new component was installed with > 0 hours.',
                'Life Achieved': 'Difference between Benchmark SMR and SMR at changeout.',
                'Status': 'Has component CO record been validated as correc.'
            }
            self.col_widths |= dict(Notes=400)

            cols = ['Unit SMR', 'Comp SMR', 'SN Removed', 'SN Installed', 'Removal Reason']
            self.add_highlight_funcs(cols=cols, func=self.highlight_blanks)
            self.set_combo_delegate(col='Reman', items=['True', 'False'], allow_blank=False)
            self.set_combo_delegate(col='Removal Reason', items=cf.config['Lists']['RemovalReason'])


class ComponentSMR(TableWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent, refresh_on_init=False)
        label = QLabel(
            'Warning: this table is currently only valid for FortHills and BaseMine. (Other sites to be updated)')
        self.btnbox.addWidget(label)

    class View(TableView):
        def __init__(self, parent):
            super().__init__(parent=parent)

            # disable all cols
            self.mcols['disabled'] = f.get_default_headers(title=self.parent.title)

            smr_cols = ['Bench SMR', 'Curr Unit SMR', 'SMR Last CO', 'Curr Comp SMR', 'Life Remaining']
            self.formats |= {col: '{:,.0f}' for col in smr_cols}
            self.mcols['tooltip'] = {
                'Curr Unit SMR': 'Current SMR hours on unit at time of refresh (now).',
                'SMR Last CO': 'SMR hours on unit at time of last changeout.',
                'Curr Comp SMR': 'Curr Unit SMR - SMR Last CO + Prev Install SMR (Component CO table).',
                'Pred. CO Date': 'Predicted CO date based on assumed 20hrs/day usage.',
                'Life Remaining': 'Life of component remaining (days).',
                'Last SN Installed': 'Current component serial number (from last CO, Component CO table).'}


class TSI(EventLogBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        self.add_action(name='Zip DLS', func=self.zip_recent_dls, btn=True, ctx='tsi',
                        tooltip='Find and zip most recent download folder in the unit\'s main "Downloads" folder')
        self.add_action(name='Create Failure Report', func=self.create_failure_report, btn=True, ctx='report',
                        tooltip='Create new Failure report PDF.')
        self.add_action(name='Email Failure Report', func=self.email_report, btn=True, ctx='report',
                        tooltip='Create new email from selected row, select docs to attach.')
        self.add_action(name='TSI Homepage', func=self.open_tsi_homepage, btn=True, ctx='tsi',
                        tooltip='Open chrome browser to TSI homepage.')
        self.add_action(name='Fill TSI Webpage', func=self.fill_tsi_webpage, btn=True, ctx='tsi',
                        tooltip='Open TSI portal, create new TSI from selected event.')
        self.add_action(name='Refresh Open (User)', func=self.refresh_allopen_user, btn=True,
                        ctx='refresh', tooltip='Refresh all open TSIs for current user.')

        # don't need "open_tsi" ctx in TSI tab
        self.context_actions.pop('tsi')

    def remove_row(self):
        view, e, row = self.view, self.e, self.row

        m = dict(Unit=e.Unit, DateAdded=e.DateAdded, Title=e.Title)

        msg = f'Are you sure you would like to remove the TSI for:\n\n{f.pretty_dict(m)}\n\n \
            (This will only set the TSI Status to Null, not delete the event).'
        if dlgs.msgbox(msg=msg, yesno=True):
            row.update(vals=dict(StatusTSI=None))
            view.remove_row(i=row.i)
            self.mainwindow.update_statusbar(msg=f'TSI removed: {e.Unit} - {e.Title}')

    def view_folder(self) -> None:
        """View EventFolder then DLS folder from TSI tab
        - TODO add this to global settings
        """
        if gbl.get_setting('open_downloads_folder', False):
            self.view_dls_folder()

        super().view_folder()

    class View(EventLogBase.View):
        def __init__(self, parent):
            super().__init__(parent=parent)
            # self.mcols['disabled'] = ('WO',)
            self.mcols['fill_enabled'] += ('KA Phenomenon', 'KA Comp Group')
            self.col_widths |= {'Details': 400, 'TSI No': 120, 'KA Comp Group': 280}

            lists = cf.config['Lists']
            self.set_combo_delegate(col='KA Phenomenon', items=lists['KAPhenomenon'])
            self.set_combo_delegate(col='KA Comp Group', items=lists['KAComponentGroup'])

            self.add_col_funcs('Status', self.parent.update_tsi_combobox)

    def refresh_allopen_user(self):
        username = self.mainwindow.username
        query = self.query
        query.set_allopen()
        query.fltr.add(vals=dict(TSIAuthor=username))
        self.refresh()

    # @property
    # def driver(self) -> Union[WebDriver, None]:
    #     """Save driver for use between TSI webpage calls"""
    #     if not self.mw is None:
    #         # use global mw driver if exists (not testing)
    #         return self.mw.driver
    #     else:
    #         return self._driver if hasattr(self, '_driver') else None

    # @driver.setter
    # def driver(self, driver: WebDriver):
    #     if not self.mw is None:
    #         self.mw.driver = driver
    #     else:
    #         self._driver = driver

    def open_tsi_homepage(self):
        """Just login and show the homepage so user can go from there, check TSIs etc"""
        if not self.check_cummins():
            return

        from guesttracker.utils import web
        tsi = web.TSIWebPage(table_widget=self, _driver=self.driver)

        if not tsi.is_init:
            return

        tsi.tsi_home()
        self.driver = tsi.driver

    def fill_tsi_webpage(self) -> None:
        if not self.check_cummins():
            return

        tsis = []

        # loop all selected rows
        # collect all TSI dialog options/downloads/files etc per row selected
        for e, e_db in self.view.iter_selected_models(db=True):
            tsi = self._init_tsi(e=e, e_db=e_db)
            if not tsi is None:
                tsis.append(tsi)

        num_tsis = len(tsis)
        msg = f'Submit [{num_tsis}] TSIs?'

        for tsi in tsis:
            log.info(f'TSI: {tsi.name}')
            msg += f'\n\t{tsi.name}'

        if not tsis:
            self.update_statusbar('No TSIs to submit.')
            return

        if not dlgs.msgbox(msg=msg, yesno=True):
            return

        Worker(func=self.process_multi_tsis, mw=self.mw, tsis=tsis, return_to_home=not num_tsis == 1) \
            .add_signals(signals=('result', dict(func=self.handle_tsi_result))) \
            .start()

        self.update_statusbar(f'Creating [{len(tsis)}] new TSI(s) in worker thread. GUI free to use.')

    # def process_multi_tsis(self, tsis: List[TSIWebPage], return_to_home: bool = True) -> List[TSIWebPage]:
    #     submitted = []
    #     for tsi in tsis:
    #         try:
    #             tsi.driver = self.driver
    #             tsi.open_tsi(return_to_home=return_to_home, submit_tsi=True)
    #             self.driver = tsi.driver
    #         except:
    #             log.error(f'Failed to create TSI: {tsi.name}')
    #             raise
    #             tsi.open_url(tsi.pages['tsi_home'])

    #         submitted.append(tsi)

    #     return submitted

    # def _init_tsi(self, e: dbm.Base, e_db: dbt.Row) -> TSIWebPage:
    #     """Fill TSI webpage with values from TSI tab
    #     """
    #     d = e.DateAdded.strftime('%m/%d/%Y')

    #     field_vals = {
    #         'Unit': e_db.Unit,
    #         'Failure Date': d,
    #         'Repair Date': d,
    #         'Failure SMR': e_db.SMR,
    #         'Hours On Parts': e_db.ComponentSMR,
    #         'Serial': e_db.SNRemoved,
    #         'Part Number': e_db.PartNumber,
    #         'Part Name': e_db.TSIPartName,
    #         'New Part Serial': e_db.SNInstalled,
    #         'Work Order': e_db.WorkOrder,
    #         'Complaint': e_db.Title,
    #         'Cause': e_db.FailureCause,
    #         'Notes': e_db.Description}

    #     search_vals = {
    #         'Phenomenon': e_db.KAPhenomenon,
    #         'Component Group': e_db.KAComponentGroup,
    #     }

    #     name = f'{e_db.Unit} - {e_db.Title}'
    #     msg = f'{name}\n\n' \
    #         + 'Would you like to save the TSI after it is created?' \
    #         + '\n(Can\'t attach documents unless TSI saved first)'
    #     save_tsi = True if dlgs.msgbox(msg=msg, yesno=True) else False

    #     docs, ef = None, None
    #     if save_tsi:
    #         docs = None
    #         msg = 'Select documents to attach?'

    #         # if dlgs.msgbox(msg=msg, yesno=True):
    #         ef = efl.EventFolder.from_model(e=e_db)
    #         ef.check()
    #         docs = self.attach_docs(ef=ef)

    #         if not docs:
    #             msg = 'No documents selected, abort TSI?'
    #             if dlgs.msgbox(msg=msg, yesno=True):
    #                 return

    #     return TSIWebPage(
    #         table_widget=self,
    #         name=name,
    #         save_tsi=save_tsi,
    #         field_vals=field_vals,
    #         search_vals=search_vals,
    #         serial=e.Serial,
    #         model=e.Model,
    #         uid=e_db.UID,
    #         docs=docs,
    #         ef=ef)

    def attach_docs(self, ef=None):
        """Prompt user to select tsi docs and downloads to attach to tsiwebpage"""
        lst_files = []

        # show FileDialog and select docs to attach to TSI
        lst = dlgs.select_multi_files(p_start=ef._p_event)
        if not lst is None:
            lst_files.extend(lst)

        # Select download zip
        p_dls = ef.p_unit / 'Downloads'
        p_dls_year = p_dls / f'{ef.year}'
        p_start = p_dls_year if p_dls_year.exists() else p_dls
        lst = dlgs.select_multi_files(p_start=p_start)
        if not lst is None:
            lst_files.extend(lst)

        return lst_files

    # def handle_tsi_result(self, tsis: Union[TSIWebPage, List[TSIWebPage]] = None) -> None:
    #     # get TSIWebpage obj back from worker thread, save TSI Number back to table
    #     if tsis is None:
    #         self.update_statusbar(msg='ERROR: Failed to create TSIs.')
    #         return

    #     view = self.view
    #     msg = f'[{len(tsis)}] new TSI(s) created:\n\n'
    #     m_msg = {}

    #     for tsi in f.as_list(tsis):

    #         tsi_number, uid, num_files = tsi.tsi_number, tsi.uid, tsi.uploaded_docs

    #         # fill tsi number back to table or db
    #         # Get correct row number by UID > user may have reloaded table or changed tabs

    #         if not tsi_number is None:
    #             irow = view.get_irow_uid(uid=uid)

    #             if not irow is None:
    #                 index = view.create_index_activerow(irow=irow, col_name='TSI No')
    #                 view.data_model.setData(index=index, val=tsi_number)
    #             else:
    #                 dbt.Row(dbtable=self.get_dbtable(), keys=dict(UID=uid)) \
    #                     .update(vals=dict(TSINumber=tsi_number))

    #         m_msg[tsi.name] = dict(tsi_no=tsi_number, files=num_files)

    #     msg += f.pretty_dict(m_msg, prnt=False)

    #     dlgs.msgbox(msg, min_width=400)

    # def create_failure_report(self):
    #     if not self.check_cummins():
    #         return

    #     fl.drive_exists()
    #     e, row, view = self.e_db, self.row, self.view

    #     # get event folder
    #     ef = efl.EventFolder.from_model(e=e, irow=row.i, data_model=view.data_model)

    #     # get pics, body text from dialog
    #     cause = e.FailureCause if not e.FailureCause.strip() == '' else 'Uncertain.'
    #     complaint = e.Title

    #     # NOTE this could be more general for other types of failures
    #     correction = 'Crack(s) repaired.' if 'crack' in complaint.lower() else 'Component replaced with new.'

    #     if not e.TSIDetails is None:
    #         complaint = f'{complaint}\n\n{e.TSIDetails}'

    #     text = dict(
    #         complaint=complaint,
    #         cause=cause,
    #         correction=correction,
    #         details=e.Description)

    #     dlg = dlgs.FailureReport(parent=self, p_start=ef.p_pics, text=text, unit=e.Unit, e=e)
    #     if not dlg.exec():
    #         return

    #     # create report obj and save as pdf/docx in event folder
    #     from guesttracker.reports import FailureReport, PLMUnitReport
    #     from guesttracker.utils.word import FailureReportWord
    #     kw = {}

    #     if dlg.word_report:
    #         Report = FailureReportWord
    #         func = 'create_word'

    #     else:
    #         Report = FailureReport
    #         func = 'create_pdf'

    #     if dlg.oil_samples:
    #         kw['query_oil'] = qr.OilSamplesReport(
    #             unit=dlg.unit,
    #             component=dlg.component,
    #             modifier=dlg.modifier,
    #             d_lower=dlg.d_lower,
    #             d_upper=e.DateAdded)

    #     if dlg.plm_report:
    #         kw['rep_plm'] = PLMUnitReport(
    #             unit=dlg.unit,
    #             d_upper=dlg.plm_date_upper.dateTime().toPyDateTime(),
    #             d_lower=dlg.plm_date_lower.dateTime().toPyDateTime(),
    #             include_overloads=True)

    #     rep = Report.from_model(e=e, ef=ef, pictures=dlg.pics, body=dlg.text, **kw)

    #     # need to msgbox for check overwrite in main thread
    #     if not rep.check_overwrite(p_base=ef._p_event):
    #         self.update_statusbar(msg='User declined to overwrite existing report.')
    #         return

    #     def handle_report_result(rep=None):
    #         if rep is None:
    #             return
    #         msg = 'Failure report created, open now?'
    #         if dlgs.msgbox(msg=msg, yesno=True):
    #             rep.open_()

    #     Worker(func=getattr(rep, func), mw=self.mw) \
    #         .add_signals(signals=('result', dict(func=handle_report_result))) \
    #         .start()

    #     self.update_statusbar('Creating failure report in worker thread.')

    # def zip_recent_dls(self):
    #     if not self.check_cummins():
    #         return
    #     fl.drive_exists()
    #     e = self.e
    #     unit = e.Unit

    #     p_dls = dls.zip_recent_dls_unit(unit=unit, _zip=False)
    #     if not p_dls:
    #         return

    #     def _handle_zip_result(p_zip):
    #         self.update_statusbar(f'Folder successfully zipped: {p_zip.name}')

    #     Worker(func=fl.zip_folder_threadsafe, mw=self.mainwindow, p_src=p_dls) \
    #         .add_signals(signals=('result', dict(func=_handle_zip_result))) \
    #         .start()
    #     self.update_statusbar(f'Zipping folder in worker thread: {p_dls.name}')

    # def email_report(self):
    #     """Email selected row, attach failure report doc if exists
    #     """
    #     if not self.check_cummins():
    #         return

    #     view = self.view
    #     lst_attach = []
    #     email_list = []
    #     minesites = []

    #     for _, e in self.view.iter_selected_models(db=True):

    #         minesite = db.get_unit_val(e.Unit, 'MineSite')
    #         ef = efl.EventFolder.from_model(e=e)
    #         p = ef.p_event / f'{ef.title_short}.pdf'

    #         # only add new emails for new minesites
    #         if not minesite in minesites:
    #             email_list += qr.el.EmailListShort(
    #                 col_name='TSI',
    #                 minesite=minesite,
    #                 usergroup=self.u.usergroup).emails

    #         minesites.append(minesite)

    #         if p.exists():
    #             lst_attach.append(p)
    #         else:
    #             msg = f'Couldn\'t find report:\n\n{p.name}\n\nSelect file to attach?'
    #             if dlgs.msgbox(msg=msg, yesno=True):
    #                 lst_attach += dlgs.select_multi_files(p_start=ef._p_event)
    #             else:
    #                 self.update_statusbar(f'Couldn\'t find report to attach: {p}', warn=True)
    #                 # lst_attach = None

    #     self.email_row(
    #         title=f'Failure Summary - [{len(lst_attach)}]',
    #         exclude_cols=['UID', 'Status', 'Details', 'Author', 'Pics'],
    #         email_list=email_list,
    #         body_text='The following TSI(s) have been submitted:',
    #         lst_attach=lst_attach)


class UnitInfo(TableWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        tbl_b = 'EquipType'
        self.db_col_map = {
            'Equip Type': tbl_b}

        # cols not in table may not exist, have to check first
        # NOTE not dry, could put these both in func
        self.view.mcols['check_exist'] = tuple(self.db_col_map.keys())

        self.add_action(name='Add New', func=self.show_addrow, btn=True, ctx='add')

        self.context_actions['smr'] = ['show_smr_history']
        self.context_actions['view'] = ['view_folder']

    class View(TableView):
        def __init__(self, parent):
            super().__init__(parent=parent, warn_rows=5_000)
            self.mcols['disabled'] = ('SMR Measure Date', 'Current SMR', 'Warranty Remaining', 'GE Warranty')
            self.col_widths |= {
                'Notes': 400,
                'Warranty Remaining': 40,
                'GE Warranty': 40}
            self.formats |= {'Current SMR': '{:,.0f}'}
            self.mcols['sort_filter'] = ('Model',)

            items = cf.config['Lists']['EquipClass']
            self.set_combo_delegate(col='Equip Type', items=items, allow_blank=False)

    # def view_folder(self):
    #     """Open event folder of currently active row in File Explorer/Finder"""
    #     if not self.check_cummins():
    #         return
    #     efl.UnitFolder(unit=self.e.Unit).show()

    def show_addrow(self):
        dlg = AddUnit(parent=self)
        dlg.exec()

    def remove_row(self):
        """Remove selected unit from table and delete from db"""
        view, e, row = self.view, self.e, self.row

        m = dict(Unit=e.Unit, Model=e.Model, MineSite=e.MineSite)

        msg = f'Are you sure you would like to permanently delete the unit:\n\n{f.pretty_dict(m)}'
        if dlgs.msgbox(msg=msg, yesno=True):
            row.update(delete=True)
            view.remove_row(i=row.i)
            self.mainwindow.update_statusbar(msg=f'Unit removed from database: {e.Unit}')


class FCBase(TableWidget):
    def __init__(self, parent=None, **kw):
        super().__init__(parent=parent, **kw)

        self.add_action(
            name='Import FCs',
            func=self.import_fc,
            btn=True)
        self.add_action(
            name='View FC Folder',
            func=self.view_fc_folder,
            btn=True,
            tooltip=f'Open FC folder at:\n{cf.config["FilePaths"]["Factory Campaigns"]}')
        self.add_action(
            name='Create FC',
            func=self.create_fc_manual,
            shortcut='Ctrl+Shift+N',
            btn=True,
            tooltip='Create custom FC for range of units.')

        self.context_actions['view'] = ['view_folder']

    class View(TableView):
        def __init__(self, parent):
            super().__init__(parent=parent)

    # def import_fc(self):
    #     lst_csv = dlgs.select_multi_files(
    #         p_start=Path.home() / 'Downloads',
    #         fltr='*.csv')

    #     if lst_csv is None:
    #         return

    #     Worker(func=fc.import_fc, mw=self.mainwindow, lst_csv=lst_csv, upload=True, worker_thread=True) \
    #         .add_signals(signals=('result', dict(func=fc.ask_delete_files))) \
    #         .start()
    #     self.update_statusbar('FC import started in worker thread.')

    def show_addrow(self):
        self.create_fc_manual()

    # def create_fc_manual(self):
    #     dlg = dlgs.CustomFC(parent=self)
    #     if not dlg.exec():
    #         return

    #     units = dlg.units
    #     fc_number = dlg.fFCNumber.val

    #     m = dict(
    #         units=units,
    #         fc_number=fc_number,
    #         _type=dlg.fType.val,
    #         subject=dlg.fSubject.val,
    #         release_date=dlg.fReleaseDate.val,
    #         expiry_date=dlg.fExpiryDate.val)

    #     msg = f'Successfully created manual FC "{fc_number}" for [{len(units)}] unit(s).'
    #     Worker(func=fc.create_fc_manual, mw=self.mw, worker_thread=True, **m) \
    #         .add_signals(signals=('result', dict(func=lambda *args: self.update_statusbar(msg=msg)))) \
    #         .start()
    #     self.update_statusbar('Manual FC import started in worker thread.')

    def get_fc_folder(self):
        fl.drive_exists()
        e = self.e

        p = cf.p_drive / cf.config['FilePaths']['Factory Campaigns'] / e.FCNumber

        if not p.exists():
            msg = f'FC folder: \n\n{p} \n\ndoes not exist, create now?'
            if dlgs.msgbox(msg=msg, yesno=True):
                p.mkdir(parents=True)
            else:
                return

        return p

    def view_fc_folder(self):
        p = self.get_fc_folder()
        if p is None:
            return
        fl.open_folder(p=p)

    def view_folder(self):
        self.view_fc_folder()


class FCSummary(FCBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.add_action(
            name='Email New FC',
            func=self.email_new_fc,
            btn=True,
            ctx='fc',
            tooltip='Create email to send to customer, attaches FC docs.')
        self.add_action(
            name='Close FC',
            func=self.close_fc,
            btn=True,
            ctx='fc',
            tooltip='Set selected FC to "ManualClosed=True" and hide FC from this list.')
        self.add_action(
            name='Re-Open FC',
            func=self.reopen_fc,
            btn=True,
            ctx='fc',
            tooltip='Re-open closed FC.')
        self.add_action(
            name='Show FC Details',
            func=self.show_fc_details,
            ctx='view',
            tooltip='Show full list of FCs in FC Details tab.')
        self.add_action(
            name='Show AC Inspections',
            func=self.show_ac_motor_inspections,
            btn=True,
            tooltip='Show table of AC motor inspection status')

        # map table col to update table in db if not default
        tbl_b = 'FCSummaryMineSite'
        self.db_col_map = {
            'Action Reqd': tbl_b,
            'Parts Avail': tbl_b,
            'Comments': tbl_b,
            'ManualClosed': tbl_b}  # bit sketch, not actual header in table, just in db

        self.view.mcols['check_exist'] = tuple(self.db_col_map.keys())  # rows for cols may not exist yet

    class View(FCBase.View):
        prevent_column_resize = True

        def __init__(self, parent):
            super().__init__(parent=parent)

            self.mcols['fill_enabled'] = ('Action Reqd',)
            self.mcols['highlight_dates'] = ('Comments',)
            self.mcols['longtext'] = ('Comments',)

            self.col_widths |= {
                'Subject': 250,
                'Comments': 600,
                'Action Reqd': 60,
                'Type': 40,
                'Part Number': 100,
                'Parts Avail': 40,
                'Total Complete': 60,
                '% Complete': 45}

            self.highlight_vals |= {
                'm': 'maroon',
                'sms rel': 'lightyellow'}
            self.highlight_funcs['Type'] = self.highlight_by_val
            self.highlight_funcs['Action Reqd'] = self.highlight_by_val

            # TODO: add dropdown menu for Type, Action Reqd, Parts Avail

        def get_center_cols(self, df):
            # FCSummary needs dynamic center + vertical cols
            # this is called every time table is refreshed - NOTE change to 'update_cols'
            cols = list(df.columns[13:]) if df.shape[1] >= 13 else []
            mcols = self.mcols
            mcols['vertical'] = cols
            mcols['disabled'] = ['MineSite', 'FC Number', 'Total Complete', '% Complete'] + cols
            self.col_widths |= {c: 25 for c in cols}  # NOTE not ideal, would like to reimplement sizeHint
            return cols

    def email_new_fc(self):
        # get df of current row
        df = self.view.df_from_activerows()
        if df is None:
            return
        df = df.iloc[:, :10]

        formats = {'int64': '{:,}', 'datetime64[ns]': st.format_date}
        style = st.default_style(df=df, outlook=True) \
            .pipe(st.apply_formats, formats=formats)

        fcnumber = df['FC Number'].iloc[0]
        subject = df.Subject.iloc[0]
        title = f'New FC - {fcnumber} - {subject}'

        body = f'{f.greeting()}New FC Released:<br><br>{style.hide(axis="index").to_html()}'

        # get email list from db
        email_list = qr.EmailListShort(col_name='FC Summary', minesite=self.minesite, usergroup=self.u.usergroup).emails

        # show new email
        msg = em.Message(subject=title, body=body, to_recip=email_list, show_=False)

        # attach files in fc folder
        p = self.get_fc_folder()
        if not p is None:
            msg.add_attachments(lst_attach=[p for p in p.glob('*.pdf')])

        msg.show()

    def _change_fc_state(self, status: str = 'open') -> None:
        """Open or close selected FC

        Parameters
        ----------
        status : str, optional
            open | close, default 'open'

        Raises
        ------
        ValueError
            if incorrect status given
        """
        e = self.e

        statuses = dict(
            open=['opened', False],
            close=['closed', True])

        if not status in statuses.keys():
            raise ValueError(f'Status must be in {list(statuses.keys())}, not "{status}"')

        status_action, manualclosed = statuses[status]

        msg = f'Would you like {status} FC "{e.FCNumber}" for MineSite "{e.MineSite}"?'
        if not dlgs.msgbox(msg=msg, yesno=True):
            return

        row = dbt.Row(
            data_model=self.view.data_model,
            dbtable=self.get_dbtable(header='ManualClosed'),
            i=self.i)

        row.update(vals=dict(ManualClosed=manualclosed), check_exists=True)
        self.view.remove_row()

        msg = f'FC: "{e.FCNumber}" {status_action} for MineSite: "{e.MineSite}"'
        self.update_statusbar(msg=msg, success=True)

    def close_fc(self):
        """Close single selected FC"""
        self._change_fc_state('close')

    def reopen_fc(self) -> None:
        """Reopen single closed FC"""
        self._change_fc_state('open')

    def show_fc_details(self):
        """Show selected row FC Number in FC Details tab"""
        view = self.view
        fc_number = view.create_index_activerow('FC Number').data()

        title = 'FC Details'
        tabs = self.mainwindow.tabs
        table_widget = tabs.get_widget(title)

        args = [
            dict(vals=dict(MineSite=self.minesite), table='UnitID'),
            dict(vals=dict(FCNumber=fc_number))]
        query = table_widget.query
        query.add_fltr_args(args)

        table_widget.refresh()

        tabs.activate_tab(title)

    def show_ac_motor_inspections(self):
        dlg = ACInspectionsDialog(parent=self)
        dlg.exec()


class FCDetails(FCBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent, refresh_on_init=False)

        tbl_b = 'EventLog'
        self.db_col_map = {
            'Pics': tbl_b}

    class View(FCBase.View):
        def __init__(self, parent):
            super().__init__(parent=parent)

            self.highlight_funcs['Pics'] = self.highlight_pics
            self.highlight_funcs['Complete'] = self.highlight_by_val
            self.highlight_funcs['Sched'] = self.highlight_by_val
            self.mcols['disabled'] = ('MineSite', 'Model', 'Unit', 'FC Number',
                                      'Complete', 'Closed', 'Type', 'Subject', 'Pics')
            self.mcols['fill_enabled'] = ('Date Complete', 'Ignore', 'Sched')
            self.mcols['hide'] = ('UID',)
            self.col_widths.update({
                'Complete': 60,
                'Sched': 60,
                'Closed': 60,
                'Type': 60,
                'Subject': 400,
                'Notes': 400})

            tf = cf.config['Lists']['TrueFalse']
            self.set_combo_delegate(col='Ignore', items=tf, allow_blank=False)
            self.set_combo_delegate(col='Sched', items=tf, allow_blank=False)

    # def view_folder(self):
    #     view, i, e = self.view, self.i, self.e

    #     df = view.df_from_activerows()
    #     unit, uid = df.Unit.values[0], df.UID.values[0]

    #     if pd.isnull(uid):
    #         msg = 'FC not yet linked to an event, cannot view event folder.'
    #         dlgs.msg_simple(msg=msg, icon='warning')
    #         return

    #     # create EventLog row/e with UID
    #     row = dbt.Row(dbtable=dbm.EventLog, keys=dict(UID=uid))
    #     e2 = row.create_model_from_db()

    #     efl.EventFolder.from_model(e=e2, irow=i, data_model=view.data_model).show()


class EmailList(TableWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        self.add_action(name='Add New', func=self.show_addrow, btn=True, ctx='add')

    def show_addrow(self):
        dlg = AddEmail(parent=self)
        dlg.exec()

    def remove_row(self):
        """Remove selected email from database
        """
        self._remove_row(name='email', warn_keys=['MineSite', 'Email'])

    def delete_email(self):
        # TODO delete email
        return


class Availability(TableWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

        self.add_action(name='Create Report', func=self.create_report, btn=True)
        self.add_action(name='Email Assignments', func=self.email_assignments, shortcut='Ctrl+Shift+E', btn=True)
        self.add_action(
            name='Filter Assigned',
            func=lambda x: self.view.filter_column('Assigned', '0'),
            shortcut='Ctrl+Shift+A',
            btn=True,
            ctx='filter')

        self.add_action(name='Save Assignments', func=self.save_assignments, btn=True)
        self.add_action(name='Assign Suncor', func=self.assign_suncor, shortcut='Ctrl+Shift+Z')
        self.add_action(name='Show Unit EL', func=self.filter_unit_eventlog,
                        shortcut='Ctrl+Shift+F', btn=True, ctx='filter')
        self.add_action(name='Find Unit SAP', func=self.find_unit_sap, shortcut='Ctrl+Shift+S', ctx='filter')

        self.context_actions['refresh'].extend(['refresh_all_open', 'refresh_last_week', 'refresh_last_month'])

    class View(TableView):
        def __init__(self, parent):
            super().__init__(parent=parent)

            self.mcols['disabled'] = ('Unit', 'ShiftDate', 'StartDate', 'EndDate')  # read_only
            self.mcols['fill_enabled'] = ('Total', 'SMS', 'Suncor', 'Category Assigned', 'DownReason', 'Comments')
            self.mcols['datetime'] = ('StartDate', 'EndDate')
            self.mcols['dynamic'] = ('Total', 'SMS', 'Suncor')
            self.mcols['sort_filter'] = ('Unit',)
            self.col_widths |= dict(Comment=600)
            self.add_highlight_funcs(cols=['Category Assigned', 'Assigned'], func=self.highlight_by_val)
            # self.add_highlight_funcs(cols=['StartDate', 'EndDate'], func=self.highlight_ahs_duplicates, cmplx=True)
            self.add_col_funcs(cols=['SMS', 'Suncor'], func=self.update_duration)

            self.formats |= {
                'Total': '{:,.2f}',
                'SMS': '{:,.2f}',
                'Suncor': '{:,.2f}'}

            # TODO move this to an after_init, first time tab selected
            p = cf.p_res / 'csv/avail_resp.csv'
            df = pd.read_csv(p)
            self.set_combo_delegate(col='Category Assigned', items=f.clean_series(s=df['Category Assigned']))

            self.highlight_vals |= {
                's1 service': 'lightyellow',
                's4 service': 'lightblue',
                's5 service': 'lightgreen',
                '0': 'greyrow',
                'collecting info': 'lightyellow'}

            # if a value is changed in any of self.mcols['dynamic'],
            # model needs to re-request stylemap from query for that specific col

        def get_style(self, df=None, outlook=False):
            """This supercedes the default tableview get_style
            - used for email > should probably move/merge this with query"""
            theme = 'light'
            return super().get_style(df=df, outlook=outlook, exclude_cols=['Unit']) \
                .pipe(st.set_column_widths, vals=dict(StartDate=60, EndDate=60, Comment=400)) \
                .pipe(self.parent.query.background_gradient, theme=theme, do=outlook) \
                .apply(st.highlight_alternating, subset=['Unit'], theme=theme, color='navyblue')

        def update_duration(self, index, **kw):
            """Set SMS/Suncor duration if other changes"""
            model = index.model()
            col_name = model.headerData(i=index.column())

            duration = model.df.iloc[index.row(), model.get_col_idx('Total')]
            val = index.data(role=TableDataModel.RawDataRole)

            m = dict(SMS='Suncor', Suncor='SMS')
            update_col = model.get_col_idx(m.get(col_name))

            update_index = index.siblingAtColumn(update_col)
            update_val = duration - val
            model.setData(index=update_index, val=update_val, triggers=False, queue=True)
            model.flush_queue()  # this wont be triggered if queue locked > good

    def email_assignments(self, filter_assignments: bool = True) -> None:
        model = self.view.data_model
        df = model.df.copy()
        if filter_assignments:
            df = df[df.Assigned == 0]

        df = df.iloc[:, :-1]

        s = df.ShiftDate
        fmt = '%Y-%m-%d'
        maxdate, mindate = s.max(), s.min()
        if not maxdate == mindate:
            dates = '{} - {}'.format(mindate.strftime(fmt), maxdate.strftime(fmt))
        else:
            dates = maxdate.strftime(fmt)

        subject = f'Downtime Assignment | {dates}'

        body = f'{f.greeting()}See below for current downtime assignments. Please correct and respond with any updates as necessary.'  # noqa

        email_list = qr.EmailListShort(col_name='AvailDaily', minesite='FortHills', usergroup='SMS').emails

        super().email_table(subject=subject, body=body, email_list=email_list, df=df, prompts=False)

    def save_assignments(self):
        model = self.view.data_model
        df = model.df
        df = df[df.Assigned == 0]

        msg = f'Would you like to update [{len(df)}] records in the database?'
        if not dlgs.msgbox(msg=msg, yesno=True):
            return

        cols = ['Total', 'SMS', 'Suncor', 'Category Assigned', 'Comment']
        txn = dbt.DBTransaction(data_model=model) \
            .add_df(df=df, update_cols=cols) \
            .update_all()

    def assign_suncor(self):
        """Auto assign all vals in selected range to suncor
        - Ctrl+Shift+Z"""

        view = self.view
        model = view.data_model
        model.lock_queue()

        for sel_idx in view.selectedIndexes():
            index = sel_idx.siblingAtColumn(model.get_col_idx('Suncor'))

            duration = model.df.iloc[index.row(), model.get_col_idx('Total')]
            model.setData(index=index, val=duration, queue=True)

        model.flush_queue(unlock=True)

    def filter_unit_eventlog(self):
        """Filter eventlog to currently selected unit and jump to table"""
        view = self.view
        unit = view.create_index_activerow('Unit').data()

        title = 'Event Log'
        tabs = self.mainwindow.tabs
        table_widget = tabs.get_widget(title)

        if table_widget.view.data_model.rowCount() == 0:
            table_widget.refresh_lastmonth()

        table_widget.view.data_model.filter_by_items(col='Unit', items=[unit])
        tabs.activate_tab(title)

    def find_unit_sap(self):
        """Wrapper for fl.find_unit_sap from unit in table"""
        unit = self.view.create_index_activerow('Unit').data().replace('F', 'F0')
        self.mainwindow.app.processEvents()
        fl.find_unit_sap(unit=unit)

    def get_report_base(self, period_type: str, year: str):
        p = cf.p_drive / cf.config['FilePaths']['Availability']
        return p / f'{self.minesite}/{period_type.title()}ly/{year}'

    def get_report_name(self, period_type, name):
        return f'Suncor Reconciliation Report - {self.minesite} - {period_type.title()}ly - {name}'

    def get_report_path(self, p_base, name):
        return p_base / f'{name}.pdf'

    def create_report(self):
        """Show menu to select period"""
        from guesttracker.gui.dialogs.refreshtables import AvailReport
        from guesttracker.reports import AvailabilityReport

        dlg = AvailReport(parent=self)
        # self.dlg = dlg
        if not dlg.exec():
            return

        p_base = self.get_report_base(dlg.period_type, year=dlg.d_rng[0].year)

        if not fl.drive_exists(warn=False):
            msg = 'Not connected to drive, create report at desktop?'
            if not dlgs.msgbox(msg=msg, yesno=True):
                return
            else:
                p_base = Path.home() / 'Desktop'

        rep = AvailabilityReport(name=dlg.name, period_type=dlg.period_type)

        Worker(func=rep.create_pdf, mw=self.mainwindow, p_base=p_base) \
            .add_signals(signals=('result', dict(func=self.handle_report_result))) \
            .start()
        self.update_statusbar('Creating Availability report...')

    def handle_report_result(self, rep=None):
        if rep is None:
            return
        rep.open_()

        msg = f'Report:\n\n"{rep.title}"\n\nsuccessfully created. Email now?'
        self.update_statusbar(msg, success=True)

        if dlgs.msgbox(msg=msg, yesno=True):
            rep.email()


class UserSettings(TableWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

    class View(TableView):
        def __init__(self, parent):
            super().__init__(parent=parent)

            self.mcols['datetime'] = ('LastLogin',)


class OilSamples(TableWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

    class View(TableView):
        def __init__(self, parent):
            super().__init__(parent=parent, header_margin=-10)
            self.read_only_all = True


class Parts(TableWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent, refresh_on_init=False)
        self.add_action(
            name='Add New',
            func=self.show_addrow,
            btn=True,
            ctx='add',
            tooltip='Add new part')

    class View(TableView):
        def __init__(self, *args, **kw):
            super().__init__(*args, **kw, warn_rows=10_000)
            self.mcols['no_space'] = ('Part Number',)
            self.col_widths |= {'Part Number': 200, 'Part Name': 400, 'Alt Part Name': 400}

    def show_addrow(self) -> None:
        dlg = AddPart(parent=self)
        dlg.exec()

    def remove_row(self):
        """Remove selected part from database
        """
        self._remove_row(name='part', warn_keys=['PartNo', 'PartName', 'Model'])

# FILTER MENU


class FilterMenu(QMenu):
    def __init__(self, parent=None):
        super().__init__(parent=parent)


class DynamicFilterLineEdit(QLineEdit):
    # Filter textbox for a DataFrameTable

    def __init__(self, *args, **kwargs):
        _always_dynamic = kwargs.pop('always_dynamic', False)
        super().__init__(*args, **kwargs)

        col_to_filter, _df_orig, _host = None, None, None
        f.set_self(vars())

    def bind_dataframewidget(self, host, icol):
        # Bind tihs DynamicFilterLineEdit to a DataFrameTable's column

        self.host = host
        self.col_to_filter = icol
        self.textChanged.connect(self._update_filter)

    @property
    def host(self) -> TableView:
        if self._host is None:
            raise RuntimeError('Must call bind_dataframewidget() '
                               'before use.')
        else:
            return self._host

    @host.setter
    def host(self, value):
        if not isinstance(value, TableView):
            raise ValueError(f'Must bind to a TableDataModel, not {value}')
        else:
            self._host = value

        if not self._always_dynamic:
            self.editingFinished.connect(self._host._data_model.endDynamicFilter)

    def focusInEvent(self, QFocusEvent):
        self._host._data_model.beginDynamicFilter()
        super().focusInEvent(QFocusEvent)

    def _update_filter(self, text):
        # Called everytime we type in the filter box
        icol = self.col_to_filter

        self.host.data_model.filter(icol, text)


class DynamicFilterMenuAction(QWidgetAction):
    """Filter textbox in column-header right-click menu"""

    def __init__(self, parent: TableView, menu, icol):
        super().__init__(parent)

        parent_menu = menu

        # Build Widgets
        widget = QWidget()
        layout = QHBoxLayout()
        label = QLabel('Filter')
        text_box = DynamicFilterLineEdit()
        text_box.bind_dataframewidget(self.parent(), icol)
        text_box.returnPressed.connect(self._close_menu)

        layout.addWidget(label)
        layout.addWidget(text_box)
        widget.setLayout(layout)

        self.setDefaultWidget(widget)
        f.set_self(vars())

    def _close_menu(self):
        """Gracefully handle menu"""
        self.parent_menu.close()


class FilterListMenuWidget(QWidgetAction):
    """Filter textbox in column-right click menu"""

    def __init__(self, parent: TableView, menu, icol):
        super().__init__(parent)

        # Build Widgets
        widget = QWidget()
        layout = QVBoxLayout()
        lst_widget = QListWidget()
        lst_widget.setFixedHeight(200)

        layout.addWidget(lst_widget)
        widget.setLayout(layout)

        self.setDefaultWidget(widget)

        # Signals/slots
        lst_widget.itemChanged.connect(self.on_list_itemChanged)
        self.parent().dataFrameChanged.connect(self._populate_list)

        f.set_self(vars())
        self._populate_list(inital=True)

    def _populate_list(self, inital=False):
        self.lst_widget.clear()
        model = self.parent.data_model

        df = model._df_orig
        col = df.columns[self.icol]

        full_col = f.clean_series(s=df[col], convert_str=True)  # All Entries possible in this column
        disp_col = f.clean_series(s=model.df[col], convert_str=True)  # Entries currently displayed

        def _build_item(item, state=None):
            i = QListWidgetItem(f'{item}')
            i.setFlags(i.flags() | Qt.ItemFlag.ItemIsUserCheckable)

            if state is None:
                if item in disp_col:
                    state = Qt.CheckState.Checked
                else:
                    state = Qt.CheckState.Unchecked

            i.setCheckState(state)
            self.lst_widget.addItem(i)
            return i

        # Add a (Select All)
        if full_col == disp_col:
            select_all_state = Qt.CheckState.Checked
        else:
            select_all_state = Qt.CheckState.Unchecked

        self._action_select_all = _build_item('(Select All)', state=select_all_state)

        # Add filter items
        if inital:
            build_list = full_col
        else:
            build_list = disp_col

        for i in build_list:
            _build_item(i)

        # Add a (Blanks)
        # TODO

    def on_list_itemChanged(self, item):
        """Figure out what "select all" check-box state should be"""
        lst_widget = self.lst_widget
        count = lst_widget.count()

        lst_widget.blockSignals(True)
        if item is self._action_select_all:
            # Handle "select all" item click
            if item.checkState() == Qt.CheckState.Checked:
                state = Qt.CheckState.Checked
            else:
                state = Qt.CheckState.Unchecked

            # Select/deselect all items
            for i in range(count):
                if i is self._action_select_all:
                    continue
                i = lst_widget.item(i)
                i.setCheckState(state)
        else:
            # Non "select all" item; figure out what "select all" should be
            if item.checkState() == Qt.CheckState.Unchecked:
                self._action_select_all.setCheckState(Qt.CheckState.Unchecked)
            else:
                # "select all" only checked if all other items are checked
                for i in range(count):
                    i = lst_widget.item(i)
                    if i is self._action_select_all:
                        continue
                    if i.checkState() == Qt.CheckState.Unchecked:
                        self._action_select_all.setCheckState(Qt.CheckState.Unchecked)
                        break
                else:
                    self._action_select_all.setCheckState(Qt.CheckState.Checked)

        lst_widget.blockSignals(False)

        # Filter dataframe according to list
        items = []
        for i in range(count):
            i = lst_widget.item(i)
            if i is self._action_select_all:
                continue
            if i.checkState() == Qt.CheckState.Checked:
                items.append(str(i.text()))

        parent = self.parent
        parent.blockSignals(True)
        parent.data_model.filter_by_items(items, icol=self.icol)
        parent.blockSignals(False)
        parent._enable_widgeted_cells()


# HEADER
class HeaderView(QHeaderView):
    """Custom header, allows vertical header labels"""

    def __init__(self, parent=None, margin: int = None):
        super().__init__(Qt.Orientation.Horizontal, parent)

        _font = gbl.get_qt_app().font()
        _metrics = QFontMetrics(_font)
        _descent = _metrics.descent()
        vertical_margin = 10

        # create header menu bindings
        self.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter | Qt.TextFlag.TextWordWrap)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(parent._header_menu)
        self.setSectionsMovable(True)
        # self.setSortIndicatorShown(False)
        # self.setFixedHeight(30) # NOTE maybe not

        # not sure why but need negative padding for header cells...
        if not margin is None:
            self.setStyleSheet(
                f'QHeaderView::section::horizontal {{ padding-left: {margin}px; padding-right: {margin}px; }}')

        f.set_self(vars())

    def paintSection(self, painter, rect, index):
        col = self._get_data(index)

        if col in self.parent.mcols['vertical']:
            painter.rotate(-90)
            painter.setFont(self._font)
            painter.drawText(- rect.height() + self.vertical_margin,
                             rect.left() + int((rect.width() + self._descent) / 2), col)
        else:
            super().paintSection(painter, rect, index)

    def sizeHint(self):
        if self.parent.mcols['vertical']:
            return QSize(0, self._max_text_width() + 2 * self.vertical_margin)
        else:
            return QSize(super().sizeHint().width(), 40)

    def _max_text_width(self):
        # return max text width of vertical cols, used for header height
        return max([self._metrics.horizontalAdvance(self._get_data(i))
                    for i in self.model().get_col_idxs(self.parent.mcols['vertical'])])

    def _get_data(self, index):
        return self.model().headerData(index, self.orientation())
