
from PyQt6.QtCore import QDir, QSettings, Qt
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import QSplashScreen

from guesttracker import VERSION
from guesttracker import config as cf
from guesttracker import dt
from guesttracker import errors as er
from guesttracker import getlog
from guesttracker.gui import _global as gbl
from guesttracker.gui import delegates, gui, tables
from guesttracker.gui.dialogs import base, refreshtables

# from guesttracker.gui.multithread import Worker

log = getlog(__name__)

# add icons path to Qt to be loaded in darkstyle.qss
QDir.addSearchPath('qdark_icons', str(cf.p_res / 'images/qdark_icons'))


def decorate_modules():
    # decorate all classes' methods in these modules with @e error handler
    modules = [delegates, base, gui, refreshtables, tables]
    for module in modules:
        er.decorate_all_classes(module=module)


@er.errlog('Error in main process.')
def launch():
    log.info(f'\n\n\n{dt.now():%Y-%m-%d %H:%M} | init | {VERSION}')

    app = gbl.get_qt_app()

    s = QSettings('hba', 'guesttracker', app)

    pixmap = QPixmap(str(cf.p_res / 'images/gt_icon.png'))
    splash = QSplashScreen(pixmap, Qt.WindowType.WindowStaysOnTopHint)
    splash.showMessage(f'HBA Guest Tracker\nVersion {VERSION}', color=Qt.GlobalColor.white)

    # move splash screen, this is pretty janky
    try:
        # geometry = PyQt6.QtCore.QRect(0, 0, 2560, 1440) -> (left, top, width, height)
        # center = PyQt6.QtCore.QPoint(895, 906) -> (xpos, ypos)
        default_center = app.screens()[0].geometry().center()
        last_center = s.value('screen', defaultValue=default_center)
        log.debug(f'last center: {last_center}')

        # last center not in range, just use default
        if not gbl.check_screen_point(last_center):
            log.warning(f'last_center [{last_center}] out of range, using default [{default_center}]')
            last_center = default_center

        splash_rect = splash.frameGeometry()
        splash_rect.moveCenter(last_center)
        splash.move(splash_rect.topLeft())
    except:
        log.warning('Couldn\'t move splash screen.')

    splash.show()
    app.processEvents()

    w = gui.MainWindow()
    w.setUpdatesEnabled(False)
    w.show()
    w.setUpdatesEnabled(True)
    app.processEvents()

    splash.finish(w)
    w.after_init()

    # if cf.SYS_FROZEN:
    #     try:
    #         Worker(func=cf.set_config_remote, mw=w).start()
    #     except Exception as e:
    #         log.error(f'Could not set remote config: {e}', exc_info=True)

    return app.exec()
