import inspect
from distutils.util import strtobool
from typing import *

from PyQt6.QtCore import QEvent, QItemSelectionModel, Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDateEdit, QDateTimeEdit, QLineEdit, QListWidget,
    QRadioButton, QSizePolicy, QSlider, QSpinBox, QTextEdit, QTimeEdit)

from smseventlog import dt
from smseventlog import functions as f
from smseventlog import getlog

if TYPE_CHECKING:
    from smseventlog.gui.dialogs.base import InputField

log = getlog(__name__)

# map obj: (getter, setter)
global obj_vals
obj_vals = {
    QComboBox: ('currentText', 'setCurrentText', 'currentIndexChanged'),
    QListWidget: ('get_selected_items', 'set_selected_items', 'itemSelectionChanged'),
    QTextEdit: ('toPlainText', 'setText', 'textChanged'),
    QLineEdit: ('text', 'setText', 'textChanged'),
    QDateEdit: ('dateTime.toPyDateTime', 'setDate', 'dateChanged'),
    QTimeEdit: ('time.toString', 'setTime', 'timeChanged'),  # NOTE not used
    QDateTimeEdit: ('dateTime.toPyDateTime', 'setDateTime', 'dateTimeChanged'),
    QCheckBox: ('isChecked', 'setChecked', 'stateChanged'),
    QSpinBox: ('value', 'setValue', 'valueChanged'),
    QSlider: ('value', 'setValue', 'valueChanged'),
    QRadioButton: ('isChecked', 'setChecked', 'toggled'),
}


class FormFields(object):
    """Base input box class to simplify getting/setting all form field values"""
    changed = pyqtSignal(object)  # connect all "changed" signals to common signal

    def __init__(
            self,
            name: str = None,
            key: str = None,
            val: Any = None,
            max_width: int = None,
            *args, **kw):
        super().__init__(*args, **kw)
        self.propagate = True  # prevent emmiting changed signal when false

        # loop base classes till find a match in obj_vals
        for cls in inspect.getmro(self.__class__):
            type_ = obj_vals.get(cls, None)
            if not type_ is None:
                parentclass = cls  # eg PyQt6.QtWidgets.QTextEdit, (class object not str)
                break

        getter, setter = type_[0], type_[1]  # currentText, setCurrentText
        _settings_key = key
        f.set_self(vars(), exclude=('key', 'val'))

        # set value from constructor
        if not val is None:
            self.val = val

        # changed signal
        getattr(self, type_[2]).connect(self._state_changed)

        if not name is None:
            self.set_name(name=name)

        if not max_width is None:
            self.setMaximumWidth(max_width)

    def _state_changed(self, state=None, *args):
        """Need to use this func to allow None for some states"""
        if self.propagate:
            self.changed.emit(state)

    @property
    def key(self) -> str:
        """key to access box's setting in global app settings"""
        return self._settings_key

    @property
    def val(self):
        """Get value based on input box type's specific method"""
        v = f.getattr_chained(self, self.getter)  # chained because DateEdit needs multi method() calls
        if isinstance(v, str):
            return v.strip()
        else:
            return v

    @val.setter
    def val(self, value):
        self._set_val(value)

    @property
    def field(self) -> 'InputField':
        return self._field

    @field.setter
    def field(self, field: 'InputField') -> None:
        self._field = field

    def _set_val(self, value):
        getattr(self, self.setter)(value)

    def set_name(self, name):
        # give widget a unique objectName to save/restore state
        self.name = name
        if hasattr(self, 'setObjectName'):
            self.setObjectName(f'{name}_{self.parentclass.__name__}'.replace(' ', '').lower())

    def select_all(self):
        try:
            self.setFocus()
            self.selectAll()
        except:
            log.warning(f'{self.__class__.__name__}: couldnt select all text')
            pass  # NOTE not sure which methods select text in all types of boxes yet


class ComboBox(QComboBox, FormFields):
    def __init__(self, items=None, editable=True, default=None, *args, **kw):

        # don't init ComboBox with 'val', use 'default to set after init
        if 'val' in kw:
            default = kw.pop('val')

        super().__init__(*args, **kw)
        self.setMaxVisibleItems(20)
        self.setEditable(editable)
        self.setDuplicatesEnabled(False)
        self.set_items(items)

        if default:
            self.val = default

    @FormFields.val.setter
    def val(self, value):
        # prevent adding previous default items not in this list, allow wildcards
        value = str(value)
        val_lower = value.lower()

        if '*' in value:
            self._set_val(value)  # just use setText

        elif val_lower in self.items_lower:
            idx = self.items_lower.index(val_lower)
            self.setCurrentIndex(idx)

    def select_all(self):
        self.setFocus()
        # self.lineEdit().setCursorPosition(0)  # can't select all and set cursor pos
        self.lineEdit().selectAll()

    def set_items(self, items: Union[Iterable[str], None]):
        """Clear all items and add new

        Parameters
        ----------
        items : Union[Iterable[str], None]
            items to add to combobox
        """
        if items is None:
            items = []
        else:
            items = [str(item) for item in items]

        self.items = items
        self.items_original = items
        self.items_lower = [str(item).lower() for item in self.items]

        self.clear()
        self.addItems(items)

    def reset(self):
        self.set_items(items=self.items_original)


class ComboBoxTable(ComboBox):
    """
    Special combo box for use as cell editor in TableView

    To implement a persistent state for the widgetd cell, must provide
    `getWidgetedCellState` and `setWidgetedCellState` methods.  This is how
    the WidgetedCell framework can create and destory widget as needed.
    """
    escapePressed = pyqtSignal()
    returnPressed = pyqtSignal()

    def __init__(self, parent=None, delegate=None, **kw):
        super().__init__(parent=parent, **kw)
        # need parent so TableView knows where to draw editor

        self.delegate = delegate
        if not delegate is None:
            self.escapePressed.connect(self.delegate.close_editor)
            # self.returnPressed.connect(self.delegate.commitAndCloseEditor)

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.KeyPress:
            if event.key() == Qt.Key.Key_Escape:
                self.escapePressed.emit()
        return super().eventFilter(obj, event)

    def commit_list_data(self):
        # need to manually set the editors's index/value when list item is pressed, then commit
        self.setCurrentIndex(self.view().currentIndex().row())
        self.delegate.commitAndCloseEditor()

    def showPopup(self):
        # need event filter to catch combobox view's ESC event and close editor completely
        super().showPopup()
        view = self.view()
        view.installEventFilter(self)
        view.pressed.connect(self.commit_list_data)

    def getWidgetedCellState(self):
        return self.currentIndex()

    def setWidgetedCellState(self, state):
        self.setCurrentIndex(state)


class TextEdit(QTextEdit, FormFields):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.setTabChangesFocus(True)


class LineEdit(QLineEdit, FormFields):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)


class DateEdit(QDateEdit, FormFields):
    def __init__(self, date=None, calendar=True, *args, **kw):
        super().__init__(*args, **kw)
        editor_format = 'yyyy-MM-dd'
        display_format = '%Y-%m-%d'  # not sure if used
        self.setCalendarPopup(calendar)
        self.setDisplayFormat(editor_format)

        if date is None:
            date = dt.now().date()

        if isinstance(date, dt):
            date = date.date()

        self.setDate(date)


class TimeEdit(QTimeEdit, FormFields):
    """NOTE not used yet"""

    def __init__(self, time=None, calendar=True, *args, **kw):
        super().__init__(*args, **kw)
        editor_format = 'HH:mm'
        display_format = '%HH:MM'  # not sure if used
        self.setDisplayFormat(editor_format)

        if time is None:
            time = dt.now().time()

        self.setTime(time)


class DateTimeEdit(QDateTimeEdit, FormFields):
    def __init__(self, datetime=None, calendar=True, *args, **kw):
        super().__init__(*args, **kw)
        editor_format = 'yyyy-MM-dd HH:mm'
        display_format = '%Y-%m-%d %HH:MM'  # not sure if used
        self.setDisplayFormat(editor_format)

        if datetime is None:
            datetime = dt.now()

        self.setDateTime(datetime)


class CheckBox(QCheckBox, FormFields):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        # self.setChecked(checked) # maybe dont need this
        # self.setEnabled(enabled)

    @FormFields.val.setter
    def val(self, value):
        # windows saves bools as 'true'/'false'
        if not value in (True, False):
            try:
                value = strtobool(value)
            except:
                value = False

        self._set_val(value)

    @property
    def box(self) -> 'FormFields':
        """A CheckBox's formfield
        - eg LineEdit, ComboBox etc
        """
        return self._box

    @box.setter
    def box(self, box: 'FormFields') -> None:
        self._box = box


class SpinBox(QSpinBox, FormFields):
    def __init__(self, range=None, *args, **kw):
        super().__init__(*args, **kw)
        if not range is None:
            self.setRange(*range)

    @FormFields.val.getter
    def val(self):
        # Always return None instead of 0
        # NOTE this may need to change but is good for now
        v = super().val
        return v if not v == 0 else None


class MultiSelectList(QListWidget, FormFields):
    """Class to handle multiple selection of items in a list"""

    def __init__(self, items: Union[List[str], None] = None, **kw):

        # don't init MultiSelectList with 'val', use 'default to set after init
        if 'val' in kw:
            default = kw.pop('val')
        else:
            default = None

        super().__init__(**kw)
        self.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        self.set_items(items)

        # keeps list from expanding to fill form layout in Preferences dialog (horizontal, vertical)
        self.setSizePolicy(QSizePolicy.Policy.MinimumExpanding, QSizePolicy.Policy.Minimum)

        if default:
            self.val = default

    def set_items(self, items: Union[Iterable[str], None]):
        """Clear all items and add new

        Parameters
        ----------
        items : Union[Iterable[str], None]
            items to add to combobox
        """
        if items is None:
            items = []
        else:
            items = [str(item) for item in items]

        self.items = items
        self.items_lower = [str(item).lower() for item in self.items]

        self.clear()
        self.addItems(items)

    def set_selected_items(self, items: Union[Iterable[str], None]):
        """Set selected items

        Parameters
        ----------
        items : Union[Iterable[str], None]
            items to select
        """
        if items is None:
            items = []
        else:
            items = [str(item) for item in items]

        self.clearSelection()

        for item in items:
            try:
                index = self.items_lower.index(item.lower())
                self.setCurrentRow(index, QItemSelectionModel.SelectionFlag.Select)
            except:
                log.warning(f'Failed to set item {item}')

    def get_selected_items(self) -> List[str]:
        return [self.item(i).text() for i in range(self.count()) if self.item(i).isSelected()]
