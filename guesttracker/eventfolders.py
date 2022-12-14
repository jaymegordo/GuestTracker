import shutil
from pathlib import Path
from typing import Union

import pandas as pd
import pypika as pk

from guesttracker import config as cf
from guesttracker import dt
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.database import db
from guesttracker.dbtransaction import Row
from guesttracker.utils import fileops as fl
from guesttracker.utils.dbmodel import EventLog
from jgutils import pandas_utils as pu

log = getlog(__name__)


class UnitFolder(object):

    def __init__(self, unit: str):
        """Object that represents path to unit's base folder

        Parameters
        ---
        Unit: string

        Examples
        -------
        >>> uf = UnitFolder(unit='F301')
        >>> uf.p_unit
        '/Volumes/Public/Fort Hills/02. Equipment Files/1. 980E Trucks/F301 - A40017'
        """

        # get unit's row from unit table, save to self attributes
        m = db.get_df_unit() \
            .pipe(pu.lower_cols) \
            .loc[unit]

        # f.copy_dict_attrs(m=m, target=self)
        self.unit = unit
        self.minesite = m.mine_site
        self.model = m.model
        self.modelbase = m.model_base
        self.serial = m.serial
        self.is_component = m.is_component

        # get component name from component map
        if self.is_component:
            self.comp_prefix = unit.split('_')[0].upper()  # eg 'WM_1234' -> 'WM'
            self.component = cf.config['ComponentMap'].get(self.comp_prefix, self.comp_prefix)
            self.unitpath = f'{self.component}/{self.serial}'  # eg 'Wheel Motor/1234'
        else:
            self.component = None
            self.unitpath = f'{unit} - {self.serial}'

        self.modelpath = self.get_modelpath()  # needs model and model_map

        if not 'shovels' in self.minesite.lower():
            self.p_unit = cf.p_drive / f'{self.equippath}/{self.modelpath}/{self.unitpath}'
        else:
            # shovels doesn't want modelpath. Could make this a list of exclusions or something
            self.p_unit = cf.p_drive / f'{self.equippath}/{self.unitpath}'

        self.p_dls = self.p_unit / 'Downloads'
        self.p_dls_year = self.p_dls / str(dt.now().year)

    @classmethod
    def from_model(cls, e, **kw):
        return cls(unit=e.Unit)

    def check(self) -> Union[bool, None]:
        """Check if UnitFolder exists on p drive"""
        if not fl.drive_exists():
            return

        return self.p_unit.exists()

    def create(self) -> None:
        """Create unit folder on p drive"""
        if not fl.drive_exists():
            return

        p = self.p_unit
        if p.exists():
            return

        p.mkdir(parents=True)
        log.info(f'Created unit folder: {p}')

    def show(self, dls: bool = False) -> None:
        """Open main Event folder or downloads folder

        Parameters
        ----------
        dls : bool, optional
            open downloads folder, by default False
        """
        if not self.check():
            from guesttracker.gui import _global as gbl

            msg = f'Unit folder not found, creating: {self.p_unit}'
            gbl.update_statusbar(msg=msg, warn=True)
            self.create()

        if not dls:
            p = self.p_unit
        else:
            p = self.p_dls_year if self.p_dls_year.exists() else self.p_dls

        fl.open_folder(p=p, check_drive=False)

    @property
    def equippath(self):
        """Return specific equippath from data/config.yaml if exists for self.minesite, else default

        eg:
        - 'Fort McMurray/service/2. Customer Equipment Files/1. Suncor'
        - 'Regional/SMS West Mining/Equipment'
        """
        minesite = self.minesite.replace('-', '')  # handle Shovels-63N
        s = cf.config['EquipPaths'].get(minesite, None)
        if not s is None:
            return s
        else:
            return 'Regional/SMS West Mining/Equipment'

    def get_modelpath(self):
        """Return section of path to bridge equippath and unitpath.

        Some units have specific path for model, others use default modelbase eg 'HM400'

        Examples
        ---
        ModelPaths:
            FortHills:
                980E: 1. 980E Trucks
                930E: 2. 930E Trucks
                HD1500: 3. HD1500
            BaseMine:
                980E: 1. Trucks/2. 980E
                930E: 1. Trucks/1. 930E
                HD1500: 1. Trucks/3. HD1500
        """
        model = self.model

        modelbase = cf.config.get('ModelPaths', {}).get(self.minesite, {}).get(self.modelbase, None)
        if not modelbase is None:
            return modelbase

        # NOTE very ugly and sketch but w/e
        lst_full_model = ['Bighorn', 'RainyRiver', 'GahchoKue', 'CoalValley', 'ConumaCoal', 'IOC-RioTinto']

        if not self.minesite in lst_full_model:
            modelbase = db.get_modelbase(model=model)
        else:
            # TODO eventually maybe migrate all these folders to a modelbase structure
            modelbase = model

        return modelbase if not modelbase is None else 'temp'


class EventFolder(UnitFolder):
    def __init__(
            self,
            unit: str,
            dateadded: dt,
            workorder: str,
            title: str,
            uid: int = None,
            data_model=None,
            irow: int = None,
            table_widget=None,
            **kw):
        super().__init__(unit=unit, **kw)
        # data_model only needed to set pics in table view
        self.dt_format = '%Y-%m-%d'

        year = dateadded.year

        wo_blank = 'WO' + ' ' * 14
        if not workorder or 'nan' in str(workorder).lower():
            workorder = wo_blank

        # confirm unit, date, title exist?
        folder_title = self.get_folder_title(unit, dateadded, workorder, title)

        p_base = self.p_unit / f'Events/{year}'
        _p_event = p_base / folder_title
        p_event_blank = p_base / self.get_folder_title(unit, dateadded, wo_blank, title)

        f.set_self(vars())

    @classmethod
    def from_model(cls, e, **kw):
        """Create eventfolder from database/row model 'e'. Used when single query to db first is okay.
        - NOTE works with row model OR df.itertuples"""
        efl = cls(unit=e.Unit, dateadded=e.DateAdded, workorder=e.WorkOrder, title=e.Title, **kw)

        if hasattr(e, 'Pictures'):
            efl.pictures = e.Pictures

        return efl

    @classmethod
    def example(cls, uid=108085410910, e=None):
        from guesttracker.gui import _global as gbl
        app = gbl.get_qt_app()

        if e is None:
            from guesttracker import dbtransaction as dbt
            e = dbt.Row.example(uid=uid)

        return cls.from_model(e=e)

    @property
    def p_event(self) -> Path:
        """NOTE careful when using this if don't want to check/correct path > use _p_event instead"""
        self.check()
        return self._p_event

    @property
    def p_pics(self) -> Path:
        return self._p_event / 'Pictures'

    @property
    def pics(self) -> list:
        """Return pics from self.p_pics"""
        return [p for p in self.p_pics.glob('*') if not p.is_dir()]

    def update_eventfolder_path(self, vals: dict):
        """Update folder path with defaults (actually new vals) + changed vals (previous)"""
        m_prev = dict(
            unit=self.unit,
            dateadded=self.dateadded,
            workorder=self.workorder,
            title=self.title)

        m_prev.update(vals)

        p_prev = self.p_base / self.get_folder_title(**m_prev)
        if not p_prev == self._p_event:
            self.check(p_prev=p_prev)

            if not self.table_widget is None:
                self.table_widget.mainwindow.update_statusbar(msg=f'Folder path updated: {self.folder_title}')

    @property
    def title_short(self) -> str:
        """Short title used for failure reports etc"""
        return f'{self.unit} - {self.dateadded:{self.dt_format}} - {f.nice_title(self.title)}'

    def get_folder_title(self, unit: str, dateadded: dt, workorder: str, title: str) -> str:
        title = f.nice_title(title=title)
        workorder = f.remove_bad_chars(w=workorder)

        if pd.isna(dateadded) or dateadded == '':
            dateadded = dt.now()

        return f'{unit} - {dateadded:{self.dt_format}} - {workorder} - {title}'

    @property
    def exists(self):
        """Simple check if folder exists"""
        fl.drive_exists()
        return self._p_event.exists()

    @property
    def num_files(self):
        """Return number of files in event folder and subdirs, good check before deleting"""
        return len(list(self._p_event.glob('**/*'))) if self.exists else 0

    def remove_folder(self):
        try:
            p = self._p_event
            shutil.rmtree(p)
            return True
        except:
            return False

    def check(self, p_prev: Path = None, check_pics=True, **kw):
        """Check if self.p_event exists.

        Parameters
        ----------
        p_prev : Path, optional
            Compare against manually given path, default None\n
        check_pics : bool, optional

        Returns
        -------
        bool
            True if folder exists or replace was successful.
        """
        from guesttracker.gui.dialogs import dialogbase as dlgs
        if not fl.drive_exists(**kw):
            return

        p = self._p_event
        p_blank = self.p_event_blank

        if not p.exists():
            if not p_prev is None and p_prev.exists():
                # rename folder when title changed
                fl.move_folder(p_src=p_prev, p_dst=p)
            elif p_blank.exists():
                # if wo_blank stills exists but event now has a WO, automatically rename
                fl.move_folder(p_src=p_blank, p_dst=p)
            else:
                # show folder picker dialog
                msg = f'Can\'t find folder:\n\'{p.name}\'\n\nWould you like to link it?'
                if dlgs.msgbox(msg=msg, yesno=True):
                    p_old = dlgs.select_single_folder(p_start=p.parent)

                    # if user selected a folder
                    if p_old:
                        fl.move_folder(p_src=p_old, p_dst=p)

                if not p.exists():
                    # if user declined to create OR failed to choose a folder, ask to create
                    msg = f'Folder:\n\'{p.name}\' \n\ndoesn\'t exist, create now?'
                    if dlgs.msgbox(msg=msg, yesno=True):
                        self.create_folder()

        if p.exists():
            if check_pics:
                self.set_pics()
            return True
        else:
            return False

    def show(self):
        if self.check():
            fl.open_folder(p=self._p_event, check_drive=False)

    def set_pics(self):
        # count number of pics in folder and set model + save to db
        model, irow = self.data_model, self.irow

        num_pics = fl.count_files(p=self.p_pics, ftype='pics')
        if hasattr(self, 'pictures') and num_pics == self.pictures:
            return  # same as previous pictures

        # if WorkOrders table active, use setData to set table + db
        if not model is None and model.table_widget.title in ('Work Orders', 'TSI', 'FC Details'):
            if irow is None:
                return  # need model/row to set value in table view

            index = model.createIndex(irow, model.get_col_idx('Pics'))
            model.setData(index=index, val=num_pics)
        else:
            # just set str8 to db with uid
            if self.uid is None:
                return

            row = Row(keys=dict(UID=self.uid), dbtable=EventLog)
            row.update(vals=dict(Pictures=num_pics))
            print(f'num pics updated in db: {num_pics}')

    def create_folder(self, show=True, ask_show=False):
        from guesttracker.gui.dialogs import dialogbase as dlgs
        fl.drive_exists()

        try:
            p = self._p_event
            p_pics = p / 'Pictures'
            # p_dls = p / 'Downloads'

            if not p.exists():
                p_pics.mkdir(parents=True)
                # p_dls.mkdir(parents=True)

                if ask_show:
                    msg = 'Event folder created. Would you like to open?'
                    if dlgs.msgbox(msg=msg, yesno=True):
                        self.show()
                elif show:
                    self.show()
        except:
            msg = 'Can\'t create folder!'
            dlgs.msg_simple(msg=msg, icon='critical')
            log.error(msg, exc_info=True)

    @property
    def has_condition_report(self):
        return len(self.condition_reports) > 0

    @property
    def condition_reports(self):
        if not hasattr(self, '_condition_reports') or self._condition_reports is None:
            self._condition_reports = fl.find_files_partial(p=self._p_event, partial_text='cond')

        return self._condition_reports


def get_fc_folders(fc_number='19H086-1', minesite='FortHills', units=None, complete=False):
    """Example query to get all event folders for specific FC
    - Useful to check for existence of docs

    Parameters
    ----------
    fc_number : str, optional
        default '19H086-1'
    minesite : str, optional
        default 'FortHills'

    Returns
    -------
    [type]
        [description]
    """
    from guesttracker import queries as qr
    query = qr.FCDetails()

    args = [
        dict(vals=dict(MineSite=minesite), table=query.d),
        dict(vals=dict(FCNumber=fc_number))]

    if complete:
        args.append(dict(vals=dict(complete=1)))

    if not units is None:
        args.append(dict(ct=query.a.unit.isin(units)))

    # add extra table + wo column for query
    t = pk.Table('EventLog')
    query.q = query.q.left_join(t).on_field('UID')

    query.add_fltr_args(args)
    query.add_extra_cols([t.WorkOrder, t.Title, t.DateAdded])

    df = query.get_df()
    # return df

    # list of event folders
    efs = [EventFolder.from_model(e=row) for row in df.itertuples()]

    return efs


def collect_pdfs(efs: list, p_dst=None, name='ac_motor_inspections'):
    """Collect/move list of pdfs from event folders

    Parameters
    ----------
    efs : list
        [description]
    """
    if p_dst is None:
        p_dst = cf.desktop / name

    # find pdfs
    # super confusing listcomp syntax
    # list of [('F301', path)]
    pdfs = [(unit, dateadded, p) for unit, dateadded, lst in [
        (ef.unit, ef.dateadded, ef._p_event.rglob('*.pdf')) for ef in efs] for p in lst]

    for unit, dateadded, p in pdfs:
        fl.copy_file(p_src=p, p_dst=p_dst / f'{unit}_{dateadded:%Y-%m-%d}_{p.name}')

    return pdfs
