import sys
from typing import *

# import qdarkstyle
from PyQt6.QtCore import QPoint, QSettings, QSize
from PyQt6.QtGui import QFont, QIcon
from PyQt6.QtWidgets import QApplication, QMainWindow

from guesttracker import config as cf
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.errors import ReadOnlyError

if TYPE_CHECKING:
    from PyQt6.QtGui import QScreen

    from guesttracker.gui.gui import MainWindow

log = getlog(__name__)

# global functions to handle getting mainwindow/settings in dev or production independent of gui
# NOTE need to either move this out of .gui, or move .gui.__init__ imports somewhere else

global title, minsize, minsize_ss, minesite, customer
title = 'SMS Event Log'
minsize = QSize(200, 100)
minsize_ss = 'QLabel{min-width: 100px}'
minesite_default, customer = 'FortHills', 'Suncor'


def get_mainwindow() -> Union['MainWindow', None]:
    """Global function to find the (open) QMainWindow in application"""
    app = QApplication.instance()
    if not app is None:
        for widget in app.topLevelWidgets():
            if isinstance(widget, QMainWindow):
                return widget
    return None


def get_minesite() -> str:
    """Get minesite from mainwindow, or use global default for dev > FortHills"""
    mainwindow = get_mainwindow()
    if not mainwindow is None:
        return mainwindow.minesite
    else:
        return minesite_default


def get_settings() -> QSettings:
    app = get_qt_app()
    mainwindow = get_mainwindow()

    if not mainwindow is None:
        return mainwindow.settings
    else:
        return QSettings('sms', 'guesttracker')


def get_setting(key: str, default: Union[str, bool] = None) -> Any:
    """Get setting from global app settings storage

    Parameters
    ----------
    key : str
        unique setting key
    default : Union[str, bool], optional
        default None

    Returns
    -------
    Any
        any saved setting value
    """
    s = get_settings()
    val = s.value(key, defaultValue=default)
    if isinstance(val, str) and val.strip() == '':
        return default

    # windows bool settings saved as strings eg 'true'
    if val in ('true', 'false'):
        val = f.str_to_bool(val)

    return val


def update_statusbar(msg, *args, **kw) -> None:
    mw = get_mainwindow()
    if not mw is None:
        mw.update_statusbar(msg, *args, **kw)
    else:
        print(msg)


def app_running() -> bool:
    """Check if app is running"""
    return not QApplication.instance() is None


def check_read_only() -> None:
    """Check if app is in read_only mode and warn in statusbar

    Returns
    -------
    bool
        if app is read_only
    """
    read_only = get_setting('read_only', False)
    mw = get_mainwindow()

    if not mw is None:
        if read_only or mw.active_table().read_only_all:
            raise ReadOnlyError()


def check_screen_point(chk_point: QPoint) -> bool:
    """Check if point exists inside current screen's ranges
    - Screen geometry is measured from top left (0, 0) with increasing values downward
    - Used for startup to show main window and splash screen

    Parameters
    ----------
    chk_point : QPoint
        center point to check

    Returns
    -------
    bool
        if point is in screen or not
    """

    # testing
    # set QSetting "sreen" to point outside

    if not isinstance(chk_point, QPoint):
        return False

    app = get_qt_app()
    screens = app.screens()  # type: List[QScreen]

    for screen in screens:
        rect = screen.geometry()

        # check horizontal bwtn left-right and vertical btwn top-bottom
        if rect.left() <= chk_point.x() < rect.right() and \
                rect.top() <= chk_point.y() < rect.bottom():
            return True

    return False


def get_qt_app() -> QApplication:
    """Get base QApplication

    Returns
    -------
    QApplication
    """
    app = QApplication.instance()

    if app is None:
        app = QApplication([sys.executable])

        app.setWindowIcon(QIcon(str(cf.p_res / 'images/sms_icon.png')))
        p = cf.p_res / 'darkstyle.qss'
        with open(p, 'r') as file:
            app.setStyleSheet(file.read())

        app.setStyle('Fusion')

        set_font_size()

    return app


def set_font_size(size: int = None) -> None:
    """Set global font size
    - Dialogs etc changed, but most of app needs app restart before font size changes take effect

    Parameters
    ----------
    size : int, optional
        font size, default 15 (mac), 11 (win)
    """
    app = get_qt_app()
    size = size or get_setting('font_size', cf.config_platform['font size'])

    # font_family = '.AppleSystemUIFont'
    font_family = 'Calibri'
    app.setFont(QFont(font_family, int(size)))
