from collections import defaultdict as dd
from pathlib import Path
from typing import *

from PyQt6.QtCore import (
    QPoint, QSettings, QSize, QThreadPool, QTimer, pyqtSignal)
from PyQt6.QtGui import QAction, QColor, QKeySequence, QPalette
from PyQt6.QtWidgets import (
    QApplication, QLabel, QMainWindow, QMenu, QTableWidget, QTabWidget,
    QWidget)

from guesttracker import config as cf
from guesttracker import delta
from guesttracker import errors as er
from guesttracker import functions as f
from guesttracker import getlog, users
from guesttracker.database import db
from guesttracker.gui import _global as gbl
from guesttracker.gui import tables as tbls
from guesttracker.gui.dialogs import base as dlgs
from guesttracker.gui.multithread import Worker
from guesttracker.utils import fileops as fl

# from guesttracker.utils.credentials import CredentialManager

log = getlog(__name__)


class MainWindow(QMainWindow):
    """Main application window"""
    minesite_changed = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.app = QApplication.instance()
        self.setWindowTitle(gbl.title)
        self.setMinimumSize(QSize(1000, 400))
        # self.minesite_changed.connect(self.update_minesite_label)
        # self.minesite_label = QLabel(self)  # permanent label for status bar so it isnt changed by statusTips
        # self.minesite_label.setToolTip('Global MineSite > Set with [Ctrl + Shift + M]')
        self.rows_label = QLabel(self)
        self.statusBar().addPermanentWidget(self.rows_label)
        # self.statusBar().addPermanentWidget(self.minesite_label)

        # Settings
        s = QSettings('hba', 'guesttracker', self)

        screen_point = s.value('window position', False)
        screen_size = s.value('window size', False)
        log.debug(f'last screen_point: {screen_point}')
        log.debug(f'last screen_size: {screen_size}')

        # if screen size/left anchor pt values are not set or out of range, use default
        in_frame = gbl.check_screen_point(screen_point)
        if not (screen_point and screen_size and in_frame):
            screen_point = QPoint(50, 50)
            screen_point = QPoint(0, 0)
            screen_size = QSize(1200, 1000)
            log.debug(f'Using default screen size/position: {screen_size}, {screen_point}')

        # move/resize MainWindow to last position/size
        self.resize(screen_size)
        self.move(screen_point)
        self.settings = s

        self.menus = {}
        self.create_actions()

        self.tabs = TabWidget(self)
        self.setCentralWidget(self.tabs)
        # self.update_minesite_label()

        self.threadpool = QThreadPool(self)
        log.debug('Mainwindow init finished.')

    # @property
    # def minesite(self) -> str:
    #     """Global minesite setting"""
    #     return self.settings.value('minesite', defaultValue='FortHills')
    #     # return self._minesite

    # @minesite.setter
    # def minesite(self, val: Any) -> None:
    #     """Save minesite back to settings"""
    #     # self._minesite = val
    #     self.settings.setValue('minesite', val)
    #     self.minesite_changed.emit(val)

    # def update_minesite_label(self, *args):
    #     """minesite_label is special label to always show current minesite (bottom right)"""
    #     self.minesite_label.setText(f'Minesite: {self.minesite}')

    def update_rows_label(self, *args):
        view = self.active_table()
        if view is None:
            return  # not init yet

        model = view.data_model
        visible_rows = model.visible_rows
        total_rows = model.total_rows

        if total_rows == visible_rows:
            num_rows = visible_rows
        else:
            num_rows = f'{visible_rows}/{total_rows}'

        self.rows_label.setText(f'Rows: {num_rows}')

    def warn_not_implemented(self) -> None:
        """Let user know feature not implemented"""
        self.update_statusbar('Warning: This feature not yet implemented.')

    def update_statusbar(
            self,
            msg: str = None,
            warn: bool = False,
            success: bool = False,
            log_: bool = False,
            *args) -> None:
        """Statusbar shows temporary messages that disappear on any context event"""
        if not msg is None:

            # allow warn or success status to be passed with msg as dict
            if isinstance(msg, dict):
                warn = msg.get('warn', False)
                success = msg.get('success', False)
                msg = msg.get('msg', None)  # kinda sketch

            if log_:
                log.info(msg)

            bar = self.statusBar()
            self.prev_status = bar.currentMessage()
            bar.showMessage(msg)

            msg_lower = msg.lower()
            if warn or 'warn' in msg_lower or 'error' in msg_lower:
                color = '#ff5454'  # '#fa7070'
            elif success or 'success' in msg_lower:
                color = '#70ff94'
            else:
                color = 'white'

            palette = bar.palette()
            palette.setColor(QPalette.ColorRole.WindowText, QColor(color))
            bar.setPalette(palette)

            self.app.processEvents()

    def revert_status(self):
        # revert statusbar to previous status
        if not hasattr(self, 'prev_status'):
            self.prev_status = ''

        self.update_statusbar(msg=self.prev_status)

    @er.errlog()
    def after_init(self):
        """Steps to run before MainWindow is shown.
        - Everything in here must suppress errors and continue
        """
        # self.username = self.get_username()
        # self.init_sentry()

        # self.u = users.User(username=self.username, mainwindow=self).login()
        self.u = None
        # log.debug('user init')

        last_tab_name = self.settings.value('active table', 'Event Log')
        self.tabs.init_tabs()
        self.tabs.activate_tab(title=last_tab_name)
        log.debug('last tab activated')

        # initialize updater
        # self.updater = Updater(mw=self, dev_channel=self.get_setting('dev_channel'))
        # log.debug(f'updater initialized, channel={self.updater.channel}')

        t = self.active_table_widget()
        # TODO turn this back on
        if t.refresh_on_init:
            t.refresh(default=True, save_query=False)
            log.debug('last table refreshed')

        # startup update checks can allow ignoring dismissed versions
        # self.check_update(allow_dismissed=True)
        # self.start_update_timer()
        log.debug('Finished after_init')

    def start_update_timer(self, mins: int = 180) -> None:
        """Check for updates every 3 hrs"""
        if not cf.SYS_FROZEN:
            return

        msec = mins * 60 * 1000

        self.update_timer = QTimer(parent=self)
        self.update_timer.timeout.connect(self.check_update)
        self.update_timer.start(msec)

    # @er.errlog('Failed to check for update!', display=True)
    # def check_update(self, allow_dismissed: bool = False, *args):
    #     """Check for update and download in a worker thread
    #     """
    #     if not cf.SYS_FROZEN:
    #         self.update_statusbar('App not frozen, not checking for updates.')
    #         return

    #     if self.updater.update_available:
    #         # update has been previously checked and downloaded but user declined to install initially
    #         self._install_update(updater=self.updater, allow_dismissed=allow_dismissed)
    #     else:
    #         Worker(func=self.updater.check_update, mw=self) \
    #             .add_signals(signals=(
    #                 'result',
    #                 dict(func=lambda updater: self._install_update(updater, allow_dismissed=allow_dismissed)))) \
    #             .start()

    # def _install_update(
    #         self,
    #         updater: Updater = None,
    #         ask_user: bool = True,
    #         allow_dismissed: bool = False) -> None:
    #     """Ask if user wants to update and show changelog

    #     Parameters
    #     ----------
    #     updater : Updater, optional
    #         Updater obj, default None
    #     ask_user : bool, optional
    #         prompt user to update or just install, default True
    #     allow_dismissed : bool, optional
    #         allow ignoring patch updates if user has dismissed once
    #     """

    #     # update check failed, None result from thread
    #     if updater is None:
    #         return

    #     v_current = updater.version
    #     v_latest = updater.ver_latest

    #     # check if PATCH update has been dismissed
    #     if not updater.needs_update and allow_dismissed:
    #         log.info('User declined current update. current:'
    #                  + f'{v_latest}, dismissed: {updater.ver_dismissed}')
    #         return

    #     # show changelog between current installed and latest version
    #     markdown_msg = updater.get_changelog_new()

    #     # prompt user to install update and restart
    #     msg = 'An updated version of the Event Log is available.\n\n' \
    #         + f'Current: {v_current}\n' \
    #         + f'Latest: {v_latest}\n\n' \
    #         + 'Would you like to restart and update now?' \
    #         + '\n\nNOTE - Patch updates (eg x.x.1) can be dismissed. Use Help > Check for Update ' \
    #         + 'to prompt again.'

    #     if ask_user:
    #         if not dlgs.msgbox(msg=msg, yesno=True, markdown_msg=markdown_msg):

    #             # mark version as dismissed
    #             self.settings.setValue('ver_dismissed', str(v_latest))
    #             self.update_statusbar(f'User dismissed update version: {v_latest}', log_=True)
    #             return

    #     Worker(func=updater.install_update, mw=self).start()
    #     self.update_statusbar('Extracting update and restarting...')

    def show_full_changelog(self) -> None:
        """Show full changelog"""
        msg = self.updater.get_changelog_full()
        dlgs.msgbox(msg='Changelog:', markdown_msg=msg)

    def init_sentry(self):
        """Add user-related scope information to sentry"""
        with configure_scope() as scope:  # type: ignore
            scope.user = dict(
                username=self.username,
                email=self.get_setting('email'))
            # scope.set_extra('version', VERSION) # added to sentry release field

    def active_table_widget(self) -> tbls.TableWidget:
        """Current active TableWidget"""
        return self.tabs.currentWidget()

    @property
    def t(self) -> tbls.TableWidget:
        """Convenience property wrapper for active TableWidget"""
        return self.active_table_widget()

    def active_table(self) -> Union[tbls.TableView, None]:
        """Current active TableView"""
        table_widget = self.active_table_widget()
        if not table_widget is None:
            return table_widget.view

    @property
    def tv(self) -> Union[tbls.TableView, None]:
        """Convenience property wrapper for active TableView"""
        return self.active_table()

    def show_changeminesite(self):
        dlg = dlgs.ChangeMinesite(parent=self)
        return dlg.exec()

    @er.errlog('Close event failed.')
    def closeEvent(self, event):
        s = self.settings
        s.setValue('window size', self.size())
        s.setValue('window position', self.pos())
        s.setValue('screen', self.geometry().center())

        log.warning(f'pos: {self.pos()}')
        log.warning(f'frameGeometry: {self.frameGeometry()}')
        log.warning(f'close event center point: {self.geometry().center()}')

        app = gbl.get_qt_app()
        screens = app.screens()
        for screen in screens:
            rect = screen.geometry()
            log.warning(f'screen geom: {rect}')

        # s.setValue('minesite', self.minesite)
        # TODO turn back on
        s.setValue('active table', self.active_table_widget().title)

        # save current TableView column state
        # TODO turn back on
        self.tv.save_header_state()

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Convenience accessor to global settings"""
        return gbl.get_setting(key=key, default=default)

    def get_username(self):
        s = self.settings
        username = self.get_setting('username')
        email = self.get_setting('email')

        if username is None or email is None:
            self.set_username()
            username = self.username

        return username

    def set_username(self):
        # show username dialog and save first/last name to settings
        s = self.settings
        dlg = dlgs.InputUserName(self)
        if not dlg.exec():
            return

        s.setValue('username', dlg.username)
        s.setValue('email', dlg.email)
        self.username = dlg.username

        if hasattr(self, 'u'):
            self.u.username = dlg.username
            self.u.email = dlg.email

    def get_menu(self, name: Union[str, 'QMenu']) -> 'QMenu':
        """Get QMenu if exists or create

        Returns
        -------
        QMenu
            menu bar
        """
        if isinstance(name, str):
            menu = self.menus.get(name, None)

            if menu is None:
                bar = self.menuBar()
                menu = bar.addMenu(name.title())
                self.menus[name] = menu
        else:
            menu = name

        return menu

    def add_action(
            self,
            name: str,
            func: Callable,
            menu: str = None,
            shortcut: str = None,
            tooltip: str = None,
            label_text: str = None,
            parent: QWidget = None,
            **kw) -> QAction:
        """Convenience func to create QAction and add to menu bar

        Parameters
        ----------
        name : str
            Action name

        Returns
        -------
        QAction
        """
        name_action = name.replace(' ', '_').lower()
        name_key = f'act_{name_action}'
        name = f.nice_title(name.replace('_', ' ')) if label_text is None else label_text

        if parent is None:
            parent = self

        act = QAction(name, parent, triggered=func, **kw)

        if not shortcut is None:
            act.setShortcut(QKeySequence(shortcut))

        act.setToolTip(tooltip)
        # act.setShortcutContext(Qt.ShortcutContext.WidgetShortcut)
        act.setShortcutVisibleInContextMenu(True)

        setattr(parent, name_key, act)

        if not menu is None:
            menu = self.get_menu(menu)

            menu.addAction(act)
        else:
            parent.addAction(act)

        return act

    def add_actions(self, actions: dict, menu: Union[str, 'QMenu'] = None) -> None:
        """Add dict of multiple actions to menu bar

        Parameters
        ----------
        actions : dict
            dict of menu_name: {action: func|kw}
        """
        menu = self.get_menu(menu)

        for name, kw in actions.items():
            if not isinstance(kw, dict):
                kw = dict(func=kw)

            if 'submenu' in name:
                # create submenu, recurse
                submenu = menu.addMenu(name.replace('submenu_', '').title())
                self.add_actions(menu=submenu, actions=kw)

            else:
                if 'sep' in kw:
                    kw.pop('sep')
                    menu.addSeparator()

                self.add_action(name=name, menu=menu, **kw)

    def create_actions(self) -> None:
        """Initialize menubar actions"""
        t, tv = self.active_table_widget, self.active_table

        menu_actions = dict(
            file=dict(
                add_new_row=dict(func=lambda: t().show_addrow(), shortcut='Ctrl+Shift+N'),
                refresh_menu=dict(sep=True, func=lambda: t().show_refresh(), shortcut='Ctrl+R'),
                refresh_all_open=dict(func=lambda: t().refresh_allopen(default=True), shortcut='Ctrl+Shift+R'),
                reload_last_query=dict(func=lambda: t().refresh(last_query=True), shortcut='Ctrl+Shift+L'),
                previous_tab=dict(sep=True, func=lambda: self.tabs.activate_previous(), shortcut='Meta+Tab'),
                # change_minesite=dict(func=self.show_changeminesite, shortcut='Ctrl+Shift+M'),
                # view_folder=dict(func=lambda: t().view_folder(), shortcut='Ctrl+Shift+V'),
                submenu_reports=dict(
                    fleet_monthly_report=lambda: self.create_monthly_report('Fleet Monthly'),
                    # FC_report=lambda: self.create_monthly_report('FC'),
                    # SMR_report=lambda: self.create_monthly_report('SMR'),
                    # PLM_report=dict(sep=True, func=self.create_plm_report),
                    # import_PLM_manual=self.import_plm_manual
                ),
                # import_downloads=dict(sep=True, func=self.import_downloads),
                preferences=dict(sep=True, func=self.show_preferences, shortcut='Ctrl+,')),
            edit=dict(
                find=dict(func=lambda: tv().show_search(), shortcut='Ctrl+F')),
            table=dict(
                email_table=lambda: t().email_table(),
                email_table_selection=lambda: t().email_table(selection=True),
                export_table_excel=lambda: t().export_df('xlsx'),
                export_table_CSV=lambda: t().export_df('csv'),
                toggle_color=dict(sep=True, func=lambda: tv().data_model.toggle_color()),
                jump_first_last_row=dict(func=lambda: tv().jump_top_bottom(), shortcut='Ctrl+Shift+J'),
                reset_column_layout=dict(func=lambda: tv().reset_header_state())),
            rows=dict(
                open_tsi=dict(func=lambda: t().open_tsi(), label_text='Open TSI'),
                delete_row=lambda: t().remove_row(),
                update_component=lambda: t().show_component(),
                details_view=dict(func=lambda: t().show_details(), shortcut='Ctrl+Shift+D')),
            database=dict(
                # update_component_SMR=update_comp_smr,
                # update_FC_status_clipboard=lambda: fc.update_scheduled_sap(
                #     exclude=dlgs.inputbox(
                #         msg='1. Enter FCs to exclude\n2. Copy FC Data from SAP to clipboard\n\nExclude:',
                #         title='Update Scheduled FCs SAP'),
                #     table_widget=t()),
                reset_database_connection=dict(sep=True, func=db.reset),
                reset_database_tables=db.clear_saved_tables,
                # open_SAP=dict(sep=True, func=self.open_sap)
            ),
            help=dict(
                about=dlgs.about,
                # check_for_update=self.check_update,
                show_changelog=self.show_full_changelog,
                email_error_logs=self.email_err_logs,
                open_documentation=lambda: f.open_url(cf.config['url']['docs']),
                submit_issue=dict(
                    func=lambda: f.open_url(cf.config['url']['issues']),
                    label_text='Submit issue or Feature Request'),
                reset_username=dict(sep=True, func=self.set_username),
                test_error=self.test_error))

        # reset credentials prompts
        # for c in ('TSI', 'SMS', 'exchange', 'SAP'):
        #     menu_actions['help'][f'reset_{c}_credentials'] = lambda x, c=c: CredentialManager(c).prompt_credentials()

        for menu, m_act in menu_actions.items():
            self.add_actions(actions=m_act, menu=menu)

        # other actions which don't go in menubar
        other_actions = dict(
            refresh_last_week=lambda: t().refresh_lastweek(base=True),
            refresh_last_month=lambda: t().refresh_lastmonth(base=True),
            update_SMR=dict(
                func=lambda: t().update_smr(),
                tooltip='Update selected event with SMR from database.'),
            show_SMR_history=lambda: t().show_smr_history())

        self.add_actions(actions=other_actions)

    def test_error(self) -> None:
        """Just raise test error"""
        raise RuntimeError('This is a test error.')

    def contextMenuEvent(self, event):
        """Add actions to right click menu, dependent on currently active table
        """
        child = self.childAt(event.pos())

        menu = QMenu(self)
        # menu.setToolTipsVisible(True)

        table_widget = self.active_table_widget()
        for section in table_widget.context_actions.values():
            for action in section:
                name_action = f'act_{action}'
                try:
                    menu.addAction(getattr(self, name_action))
                except Exception as e:
                    try:
                        menu.addAction(getattr(table_widget, name_action))
                    except Exception as e:
                        log.warning(f'Couldn\'t add action to context menu: {action}')

            menu.addSeparator()

        action = menu.exec(self.mapToGlobal(event.pos()))

    def create_monthly_report(self, name: str):
        """Create report in worker thread from dialog menu

        Parameters
        ----------
        name : str
            ['Fleet Monthly', 'FC']
        """

        dlg = dlgs.BaseReportDialog(window_title=f'{name} Report')
        if not dlg.exec():
            return

        from guesttracker.reports import FCReport, FleetMonthlyReport
        from guesttracker.reports import Report as _Report
        from guesttracker.reports import SMRReport
        Report = {
            'Fleet Monthly': FleetMonthlyReport,
            'FC': FCReport,
            'SMR': SMRReport}[name]  # type: _Report

        rep = Report(
            d=dlg.d,
            minesite=dlg.items['MineSite'])  # type: ignore

        Worker(func=rep.create_pdf, mw=self) \
            .add_signals(signals=('result', dict(func=self.handle_monthly_report_result))) \
            .start()

        self.update_statusbar('Creating Fleet Monthly Report...')

    def handle_monthly_report_result(self, rep=None):
        if rep is None:
            return
        rep.open_()

        msg = f'Report:\n\n"{rep.title}"\n\nsuccessfully created. Email now?'
        if dlgs.msgbox(msg=msg, yesno=True):
            rep.email()

    def import_plm_manual(self):
        """Allow user to manually select haulcycle files to upload"""
        t = self.active_table_widget()
        e = t.e
        if not e is None:
            from guesttracker import eventfolders as efl
            unit, dateadded = e.Unit, e.DateAdded
            uf = efl.UnitFolder(unit=unit)
            p = uf.p_unit
        else:
            # No unit selected, try to get minesite equip path
            p = cf.p_drive / cf.config['EquipPaths'].get(self.minesite.replace('-', ''), '')

        if p is None:
            p = Path.home() / 'Desktop'

        lst_csv = dlgs.select_multi_files(p_start=p)
        if not lst_csv:
            return  # user didn't select anything

        from guesttracker.data.internal import utils as utl
        Worker(func=utl.combine_import_csvs, mw=self, lst_csv=lst_csv, ftype='plm') \
            .add_signals(('result', dict(func=self.handle_import_result_manual))) \
            .start()

        self.update_statusbar('Importing haul cylce files from network drive (this may take a few minutes)...')

    def create_plm_report(self):
        """Trigger plm report from current unit selected in table"""
        from guesttracker.data.internal import plm

        view = self.active_table()
        try:
            e = view.e
            unit, d_upper = e.Unit, e.DateAdded
        except er.NoRowSelectedError:
            # don't set dialog w unit and date, just default
            unit, d_upper, e = None, None, None

        # Report dialog will always set final unit etc
        dlg = dlgs.PLMReport(unit=unit, d_upper=d_upper)
        ok = dlg.exec()
        if not ok:
            return  # user exited

        m = dlg.get_items(lower=True)  # unit, d_upper, d_lower

        # check if unit selected matches event selected
        if not e is None:
            if not e.Unit == m['unit']:
                e = None

        m['e'] = e
        m['include_overloads'] = True
        # NOTE could make a func 'rename_dict_keys'
        m['d_upper'], m['d_lower'] = m['date upper'], m['date lower']

        # check max date in db
        maxdate = plm.max_date_plm(unit=m['unit'])

        if maxdate + delta(days=5) < m['d_upper']:
            # worker will call back and make report when finished
            if not fl.drive_exists(warn=False):
                msg = 'Can\'t connect to P Drive. Create report without updating records first?'
                if dlgs.msgbox(msg=msg, yesno=True):
                    self.make_plm_report(**m)

                return

            Worker(func=plm.update_plm_single_unit, mw=self, unit=m['unit']) \
                .add_signals(
                    signals=('result', dict(
                        func=self.handle_import_result,
                        kw=m))) \
                .start()

            msg = f'Max date in db: {maxdate:%Y-%m-%d}. ' \
                + 'Importing haul cylce files from network drive, this may take a few minutes...'
            self.update_statusbar(msg=msg)

        else:
            # just make report now
            self.make_plm_report(**m)

    def handle_import_result_manual(self, rowsadded=None, **kw):
        if not rowsadded is None:
            msg = dict(msg=f'PLM records added to database: {rowsadded}', success=rowsadded > 0)
        else:
            msg = 'Warning: Failed to import PLM records.'

        self.update_statusbar(msg)

    def handle_import_result(self, m_results=None, **kw):
        if m_results is None:
            return

        rowsadded = m_results['rowsadded']
        self.update_statusbar(f'PLM records added to database: {rowsadded}', success=True)

        self.make_plm_report(**kw)

    def make_plm_report(self, e=None, **kw):
        """Actually make the report pdf"""
        from guesttracker import eventfolders as efl
        from guesttracker.reports import PLMUnitReport
        rep = PLMUnitReport(mw=self, **kw)

        if not e is None:
            ef = efl.EventFolder.from_model(e)
            p = ef._p_event
        else:
            ef = None

        # If cant get event folder, ask to create at desktop
        if ef is None or not ef.check(check_pics=False, warn=False):
            p = Path.home() / 'Desktop'
            msg = 'Can\'t get event folder, create report at desktop?'
            if not dlgs.msgbox(msg=msg, yesno=True):
                return

        Worker(func=rep.create_pdf, mw=self, p_base=p) \
            .add_signals(signals=('result', dict(func=self.handle_plm_result, kw=kw))) \
            .start()

        self.update_statusbar(f'Creating PLM report for unit {kw["unit"]}...')

    def handle_plm_result(self, rep=None, unit=None, **kw):
        if rep is False:
            # not super robust, but just warn if no rows in query
            msg = 'No rows returned in query, can\'t create report!'
            dlgs.msg_simple(msg=msg, icon='warning')

        if not rep or not rep.p_rep.exists():
            self.update_statusbar('Failed to create PLM report.', warn=True)
            return

        self.update_statusbar(f'PLM report created for unit {unit}', success=True)

        msg = f'Report:\n\n"{rep.title}"\n\nsuccessfully created. Open now?'
        if dlgs.msgbox(msg=msg, yesno=True):
            rep.open_()

    def email_err_logs(self):
        """Collect and email error logs to simplify for user"""
        docs = []

        def _collect_logs(p):
            return [p for p in p.glob('*log*')] if p.exists() else []

        # collect sms logs
        p_sms = cf.p_applocal / 'logging'
        docs.extend(_collect_logs(p_sms))

        # collect pyupdater logs
        i = 1 if cf.is_win else 0
        p_pyu = cf.p_applocal.parents[1] / 'Digital Sapphire/PyUpdater/logs'
        docs.extend(_collect_logs(p_pyu))

        from guesttracker.utils import email as em

        subject = f'Error Logs - {self.username}'
        body = 'Thanks Jayme,<br><br>I know you\'re trying your best. \
            The Event Log is amazing and we appreciate all your hard work!'

        msg = em.Message(subject=subject, body=body, to_recip=['jgordon@smsequip.com'], show_=False)
        msg.add_attachments(docs)
        msg.show()

    def import_downloads(self) -> None:
        """Select and import dls files to p-drive"""
        if not fl.drive_exists():
            return

        from guesttracker.data.internal import dls

        # get dls filepath
        lst_dls = dlgs.select_multi_folders(p_start=cf.desktop)
        if lst_dls is None:
            msg = 'User failed to select downloads folders.'
            self.update_statusbar(msg=msg, warn=True)
            return

        # start uploads for each dls folder selected
        for p_dls in lst_dls:
            Worker(func=dls.import_dls, mw=self, p=p_dls) \
                .add_signals(signals=('result', dict(func=self.handle_dls_result))) \
                .start()

        self.update_statusbar(msg='Started downloads upload in worker thread.')

    def handle_dls_result(self, result: dict = None, **kw):
        if isinstance(result, dict):
            name, time_total = '', ''
            try:
                name = result.pop('name')
                time_total = f.mins_secs(result.pop('time_total'))

            # join remaining processed files/times
                msg_result = ', '.join([f'{k}: ({m["num"]}, {f.mins_secs(m["time"])})' for k, m in result.items()])
            except:
                msg_result = ''
                log.warning('Failed to build upload string')

            msg = f'Successfully uploaded downloads folder "{name}", ({time_total}). \
            Files processed/rows imported: {msg_result}'
            msg = dict(msg=msg, success=True)

        else:
            msg = dict(msg='Failed to upload downloads.', warn=True)

        self.update_statusbar(msg=msg)

    def show_preferences(self) -> None:
        """Show preferences dialog to allow user to change global settings"""
        dlg = dlgs.Preferences(parent=self)
        dlg.exec()


class TabWidget(QTabWidget):
    """TabWidget to hold all TableWidgets"""

    def __init__(self, parent: MainWindow):
        super().__init__(parent)
        self.tabindex = dd(int)  # dont think this needs to be a defaultdict?

        self.prev_index = self.currentIndex()
        self.current_index = self.prev_index
        self.mainwindow = parent
        self.m_table = cf.config['TableName']['Class']  # get list of table classes from config
        self.m_table_inv = f.inverse(self.m_table)

        self.is_init = False

        self.currentChanged.connect(self.save_activetab)
        self.currentChanged.connect(self.mainwindow.update_rows_label)

    @staticmethod
    def available_tabs(user: Union[users.User, None]) -> List[str]:
        """Return list of available tabs based on user"""
        lst = list(cf.config['Headers'].keys())  # default use all named tables
        exclude = ['Package Units']

        if not user is None:
            # Hide specific tabs per usergroup/domain
            m_hide = dict(
                CED=['FCSummary', 'FCDetails', 'Availability'])

            exclude = m_hide.get(user.domain, [])

            if not user.is_admin:
                exclude.append('UserSettings')

        return [item for item in lst if not item in exclude]

    def init_tabs(self):
        """Add user-defined visible tabs to widget"""
        available_tabs = TabWidget.available_tabs(user=self.mainwindow.u)
        visible_tabs = self.mainwindow.get_setting('visible_tabs', default=available_tabs)

        for i, title in enumerate(visible_tabs):
            self.init_tab(title=title, i=i)

        self.is_init = True

    def init_tab(self, name: str = None, title: str = None, i: int = 0) -> None:
        """Init tab """

        # init with either 'EventLog' or 'Event Log'
        if title is None:
            title = self.m_table.get(name, None)
        elif name is None:
            name = self.m_table_inv.get(title, None)

        if name is None or title is None:
            log.warning('Missing name or title, can\'t init tab.')
            return

        if title in self.tabindex:
            return  # tab already init

        table_widget = getattr(tbls, name, tbls.HBATableWidget)(parent=self, name=name)
        self.insertTab(i, table_widget, title)
        self.tabindex[title] = i

    def get_index(self, title: str) -> int:
        """Return index number of table widget by title"""
        return self.tabindex[title]

    def get_widget(self, title: str) -> QTableWidget:
        """Return table widget by title"""
        i = self.get_index(title)
        return self.widget(i)

    def activate_tab(self, title: str):
        """Activate table widget by title"""
        i = self.get_index(title)
        self.setCurrentIndex(i)

    def save_activetab(self, i: int = None) -> None:
        """Save current active tab to settings
        - used to restore after startup, and for switching back to last tab with ctrl+tab
        """
        if not self.is_init:
            return

        s = self.parent().settings
        s.setValue('active table', self.currentWidget().title)

        # keep track of previous indexes for ctrl+tab to revert
        self.prev_index = self.current_index
        self.current_index = self.currentIndex()

        # save TableView header state
        if not self.prev_index == -1:
            self.widget(self.prev_index).view.save_header_state()

    def activate_previous(self):
        self.setCurrentIndex(self.prev_index)
