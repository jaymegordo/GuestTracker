import re
from typing import TYPE_CHECKING

import pandas as pd
from PyQt6.QtCore import QSize, Qt, QTime, pyqtSignal
from PyQt6.QtGui import (
    QAbstractTextDocumentLayout, QBrush, QColor, QCursor, QPalette,
    QTextCursor, QTextDocument)
from PyQt6.QtWidgets import (
    QApplication, QDateEdit, QDateTimeEdit, QStyle, QStyledItemDelegate,
    QStyleOptionComboBox, QStyleOptionFocusRect, QStyleOptionViewItem,
    QTextEdit, QTimeEdit)

from smseventlog import dt
from smseventlog import errors as er
from smseventlog import functions as f
from smseventlog import getlog
from smseventlog.database import db
from smseventlog.gui import _global as gbl
from smseventlog.gui.datamodel import TableDataModel
from smseventlog.gui.formfields import ComboBoxTable

if TYPE_CHECKING:
    from PyQt6.QtCore import QModelIndex
    from PyQt6.QtGui import QPainter

log = getlog(__name__)

global m_align
m_align = {
    'object': Qt.AlignmentFlag.AlignLeft,
    'float64': Qt.AlignmentFlag.AlignRight,
    'int64': Qt.AlignmentFlag.AlignRight,
    'Int64': Qt.AlignmentFlag.AlignRight,
    'bool': Qt.AlignmentFlag.AlignCenter,
    'datetime64[ns]': Qt.AlignmentFlag.AlignCenter}


class TableWidgetDelegate(QStyledItemDelegate):
    """Alignment delegate for TableWidget cells
    - Aligning this way takes way too long for full table"""

    def __init__(self, parent=None, df=None):
        super().__init__(parent=parent)
        self.parent = parent
        self.df = df
        self.index = None

    def _initStyleOption(self, option, index):
        # bypass to parent
        super().initStyleOption(option, index)

    def initStyleOption(self, option, index):
        """set column alignment based on data type"""
        # model = self.parent.data_model
        df = self.df
        icol = index.column()
        # dtype = model.get_dtype(icol=icol)
        dtype = str(df.dtypes[icol])

        # align all cols except 'longtext' VCenter
        alignment = m_align.get(dtype, Qt.AlignmentFlag.AlignLeft)
        # col_name = model.get_col_name(icol=icol)

        # if not col_name in self.parent.mcols['longtext']:
        alignment |= Qt.AlignmentFlag.AlignVCenter

        option.displayAlignment = alignment
        self._initStyleOption(option, index)


class TextEditor(QTextEdit):
    returnPressed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.setTabChangesFocus(True)
        self.setAcceptRichText(False)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self.custom_context_menu)

        act = gbl.get_mainwindow().add_action(
            parent=self,
            name='insert_current_date',
            func=self.insert_current_date,
            shortcut='Ctrl+Shift+I')

        act.setShortcutContext(Qt.ShortcutContext.WidgetShortcut)

    def custom_context_menu(self):
        menu = self.createStandardContextMenu()
        menu.addSeparator()
        menu.addAction(self.act_insert_current_date)
        menu.exec(QCursor.pos())

    def insert_current_date(self) -> None:
        """Insert current date to text edit field"""
        d = dt.now().strftime('%Y-%m-%d')

        # add current date with " - " after
        self.append(f'{d} - ')

    def keyPressEvent(self, event):
        modifiers = QApplication.keyboardModifiers()

        if (modifiers != Qt.KeyboardModifier.ShiftModifier and
                event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter)):
            self.returnPressed.emit()
            return

        super().keyPressEvent(event)


class CellDelegate(QStyledItemDelegate):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.parent = parent
        self.index = None

    def _initStyleOption(self, option, index):
        # bypass to parent
        super().initStyleOption(option, index)

    def sizeHint(self, option, index):
        size = super().sizeHint(option, index)
        return QSize(size.width() + 2, size.height() + 2)

    def paint_(self, qPainter, option, qModelIndex):
        # not used, for example only rn
        option = QStyleOptionViewItem(option)
        option.index = qModelIndex
        value = qModelIndex.data()
        option.text = str(value)

        parent = self.parent
        style = parent.style()

        if (option.state & QStyle.StateFlag.State_HasFocus):
            # --- The table cell with focus
            # Draw the background
            style.drawPrimitive(style.PE_PanelItemViewItem, option, qPainter, parent)

            # Draw the text
            subRect = style.subElementRect(style.SE_ItemViewItemText, option, parent)
            alignment = qModelIndex.data(Qt.ItemDataRole.TextAlignmentRole)
            if not alignment:
                alignment = int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            if (option.state & QStyle.StateFlag.State_Enabled):
                itemEnabled = True
            else:
                itemEnabled = False

            textRect = style.itemTextRect(option.fontMetrics, subRect, alignment, itemEnabled, value)
            style.drawItemText(qPainter, textRect, alignment, option.palette, option.state, value)

            # Draw the focus rectangle
            focusOption = QStyleOptionFocusRect()
            focusOption.rect = option.rect
            style.drawPrimitive(style.PE_FrameFocusRect, focusOption, qPainter, parent)
        else:
            # --- All other table cells
            style.drawControl(style.CE_ItemViewItem, option, qPainter, parent)

    def createEditor(self, parent, option, index):

        gbl.check_read_only()

        self.index = index
        editor = TextEditor(parent=parent)

        # TODO: only do this if cell height is smaller than...?
        editor.setMinimumHeight(self.parent.rowHeight(index.row()) + 10)
        editor.returnPressed.connect(self.commitAndCloseEditor)
        self.editor = editor
        return editor

    def setEditorData(self, editor, index):
        val = index.data(role=Qt.ItemDataRole.EditRole)

        if isinstance(editor, QTextEdit):
            editor.setText(str(val))

            # move cursor to end for long items, else highlight everything for quicker editing
            if len(str(val)) > 20:
                editor.moveCursor(QTextCursor.MoveOperation.End)
            else:
                editor.selectAll()

    def setModelData(self, editor, model, index):
        model.setData(index=index, val=editor.toPlainText(), role=Qt.ItemDataRole.EditRole)

    def close_editor(self):
        self.closeEditor.emit(self.editor, QStyledItemDelegate.EndEditHint.NoHint)

    def commitAndCloseEditor(self):
        editor = self.editor
        self.commitData.emit(editor)
        self.closeEditor.emit(editor, QStyledItemDelegate.EndEditHint.NoHint)
        self.parent.resizeRowToContents(self.index.row())


class HighlightCellDelegate(CellDelegate):
    """Allow highlighting partial text different colors"""
    border_width = 2
    date_expr = r'(\d{4}-\d{2}-\d{2})'
    color = '#F8696B'
    color_hl_bg = '#ffff64'  # highlighted row background color
    color_hl_bg_mouseover = '#cccc4e'
    color_mouseover_bg = '#32414B'
    color_mouseover_text = '#148CD2'
    color_mouseover_text_date = color

    def __init__(self, parent=None):
        super().__init__(parent)
        self.doc = QTextDocument(self)
        app = gbl.get_qt_app()

    def drawFocus(self, painter, option, rect, widget=None):
        # NOTE not used
        if (option.state & QStyle.StateFlag.State_HasFocus) == 0 or not rect.isValid():
            return

        o = QStyleOptionFocusRect()
        o.state = option.state
        o.direction = option.direction
        o.rect = option.rect
        o.fontMetrics = option.fontMetrics
        o.palette = option.palette

        o.state |= QStyle.StateFlag.State_KeyboardFocusChange
        o.state |= QStyle.StateFlag.State_Item
        cg = QPalette.ColorGroup.Active if (
            option.state & QStyle.StateFlag.State_Enabled) else QPalette.ColorGroup.Disabled

        o.backgroundColor = option.palette.color(cg, QPalette.ColorRole.Highlight if (
            option.state & QStyle.StateFlag.State_Selected) else QPalette.ColorRole.Window)

        style = widget.style() if widget else QApplication.style()
        style.drawPrimitive(QStyle.PrimitiveElement.PE_FrameFocusRect, o, painter, widget)

    def paint(self, painter: 'QPainter', option: QStyleOptionViewItem, index: 'QModelIndex'):
        """Highlight dates in red
        - TODO clean this up a bit, put stuff at class level
        - TODO could do this for all cell delegates so focus rect is same
            - and no text shifting happens
        """
        date_color = self.color
        painter.save()
        parent = self.parent
        options = QStyleOptionViewItem(option)
        self.initStyleOption(options, index)

        # otherwise text draws twice
        options.text = ''
        style = QApplication.style() if options.widget is None else options.widget.style()
        ctx = QAbstractTextDocumentLayout.PaintContext()

        if index.row() == self.parent.data_model.current_row:
            # yellow selected row
            ctx.palette.setColor(QPalette.ColorRole.Text, QColor('black'))

            color = self.color_hl_bg_mouseover if option.state & QStyle.StateFlag.State_MouseOver else self.color_hl_bg

            options.palette.setColor(QPalette.ColorRole.Highlight, QColor(color))

        elif option.state & QStyle.StateFlag.State_MouseOver and not option.state & QStyle.StateFlag.State_HasFocus:
            # mouse hover, but not active row
            ctx.palette.setColor(QPalette.ColorRole.Text, QColor(self.color_mouseover_text))
            options.backgroundBrush = QBrush(QColor(self.color_mouseover_bg))
            date_color = self.color_mouseover_text_date

        else:
            # not selected or hovered, just normal cell in column
            ctx.palette.setColor(
                QPalette.ColorRole.Text,
                option.palette.color(QPalette.ColorGroup.Active, QPalette.ColorRole.Text))

        # need to set background color before drawing row
        style.drawControl(QStyle.ControlElement.CE_ItemViewItem, options, painter)

        if option.state & QStyle.StateFlag.State_HasFocus:
            pen = painter.pen()
            pen.setColor(QColor('red'))
            pen.setWidth(self.border_width)
            painter.setPen(pen)
            rect = option.rect.adjusted(1, 1, -1, -1)
            # rect = option.rect
            painter.drawRect(rect)

        textRect = style.subElementRect(QStyle.SubElement.SE_ItemViewItemText, options)

        # shift text up 4 pixels so don't have to shift with red focus box
        # this matches super().paint alignment
        if index.column() != 0:
            textRect.adjust(-1, -4, 0, 0)

        # this sets the html doc text in the correct place, but modifies location of painter, call close to last
        painter.translate(textRect.topLeft())
        painter.setClipRect(textRect.translated(-textRect.topLeft()))

        # regex replace font color for dates, set to html
        text = index.data()
        res = re.sub(self.date_expr, rf'<font color="{date_color}">\1</font>', text) \
            .replace('\n', '<br>')

        self.doc.setHtml(res)

        # define max width textDoc can occupy, eg word wrap
        self.doc.setTextWidth(options.rect.width())
        self.doc.documentLayout().draw(painter, ctx)

        painter.restore()


class ComboDelegate(CellDelegate):
    def __init__(self, parent, items=None, dependant_col=None, allow_blank=True):
        super().__init__(parent)
        # parent is TableView
        model = parent.data_model
        _cell_widget_states = {}

        f.set_self(vars())

        self.set_items(items=items)

    def set_items(self, items):
        self.items = items
        if items is None:
            return

        if self.allow_blank:
            self.items.append('')

        self.items_lower = [item.lower() for item in self.items]

    def initStyleOption(self, option, index):
        option.displayAlignment = Qt.AlignmentFlag.AlignCenter
        self._initStyleOption(option, index)

    def createEditor(self, parent, option, index):

        gbl.check_read_only()

        self.index = index

        # get items based on dependant col
        if not self.dependant_col is None:
            index_category = index.siblingAtColumn(self.model.get_col_idx('Issue Category'))
            category = index_category.data()
            self.set_items(items=db.get_sub_issue(issue=category))

        editor = ComboBoxTable(parent=parent, delegate=self, items=self.items)
        # TODO not sure how this will look on windows!!
        editor.setMinimumWidth(editor.minimumSizeHint().width() - 50)
        # editor.setMaximumWidth(self.sizeHint(option, index).width() + 40)

        self.editor = editor
        return editor

    def paint_(self, painter, option, index):
        # NOTE not used yet, maybe draw an arrow on the cell?
        val = index.data(Qt.ItemDataRole.DisplayRole)
        style = QApplication.instance().style()

        opt = QStyleOptionComboBox()
        opt.text = str(val)
        opt.rect = option.rect
        style.drawComplexControl(QStyle.ComplexControl.CC_ComboBox, opt, painter)
        super().paint(painter, option, index)

    def setEditorData(self, editor, index):
        """Set data when item already exists in list"""
        val = index.data(Qt.ItemDataRole.DisplayRole).lower()
        try:
            if val == '':
                editor.setCurrentIndex(0)
            elif val in self.items_lower:
                num = self.items_lower.index(val)
                editor.setCurrentIndex(num)
            editor.lineEdit().selectAll()

        except:
            er.log_error(log=log)

    def setModelData(self, editor, model, index):
        """Convert any matching string to good value even if case mismatched"""
        val = editor.val
        val_lower = val.lower()

        if val_lower in self.items_lower:
            val = self.items[self.items_lower.index(val_lower)]
            model.setData(index=index, val=val, role=Qt.ItemDataRole.EditRole)
        else:
            msg = f'Error setting value: "{val}" not in list.'
            self.parent.update_statusbar(msg=msg)

    def updateEditorGeometry(self, editor, option, index):
        editor.setGeometry(option.rect)


class DateDelegateBase(CellDelegate):
    def __init__(self, parent=None):
        super().__init__(parent=parent)

    def createEditor(self, parent, option, index):

        gbl.check_read_only()

        self.index = index
        editor = self.date_editor(parent)
        editor.setDisplayFormat(self.editor_format)
        editor.setCalendarPopup(True)
        editor.setMinimumWidth(self.parent.columnWidth(index.column()) + 10)  # add 10px (editor cuts off date)

        calendar = editor.calendarWidget()
        if not calendar is None:
            calendar.clicked.connect(self.commitAndCloseEditor)
        self.editor = editor

        return editor

    def setEditorData(self, editor, index):
        val = index.data(role=TableDataModel.RawDataRole)

        if pd.isnull(val):
            val = self.cur_date
        elif isinstance(self, TimeDelegate):
            val = self.get_time(d=val)

        getattr(editor, self.set_editor)(val)

    def setModelData(self, editor, model, index):
        editor_date = getattr(editor, self.date_type)()
        if isinstance(self, TimeDelegate):
            # get date from DateAdded
            index_dateadded = index.siblingAtColumn(model.get_col_idx('Date Added'))
            d1 = model.data(index=index_dateadded, role=TableDataModel.RawDataRole)
            if d1 is None:
                d1 = dt.now()

            t = QTime(editor_date).toPyTime()
            d = dt(d1.year, d1.month, d1.day, t.hour, t.minute)
        else:
            # d = QDateTime(editor_date).toPyDateTime()
            d = f.convert_date(editor_date.toPyDate())

        model.setData(index, d)

# TODO use formfields.DateEdit/DateTimeEdit


class DateTimeDelegate(DateDelegateBase):
    editor_format = 'yyyy-MM-dd hh:mm'
    display_format = '%Y-%m-%d     %H:%M'
    date_editor = QDateTimeEdit
    date_type = 'dateTime'
    set_editor = 'setDateTime'
    width = 144

    @property
    def cur_date(self):
        return dt.now()


class DateDelegate(DateDelegateBase):
    editor_format = 'yyyy-MM-dd'
    display_format = '%Y-%m-%d'
    date_editor = QDateEdit
    date_type = 'date'
    set_editor = 'setDate'
    width = 90

    @property
    def cur_date(self):
        return dt.now().date()


class TimeDelegate(DateDelegateBase):
    editor_format = 'hh:mm'
    display_format = '%H:%M'
    date_editor = QTimeEdit
    date_type = 'time'
    set_editor = 'setTime'
    width = 90

    def get_time(self, d):
        t = QTime()
        t.setHMS(d.hour, d.minute, 0)
        return t

    @property
    def cur_date(self):
        return self.get_time(d=dt.now())
