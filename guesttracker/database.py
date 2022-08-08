import functools
import socket
from typing import *
from urllib import parse

import pandas as pd
import pyodbc
import pypika as pk
from pypika import MSSQLQuery as Query
from pypika import Order
from pypika import Table as T
from pypika import functions as fn
from sqlalchemy import create_engine, exc
from sqlalchemy.engine.base import Connection  # just to wrap errors
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool.base import Pool

from guesttracker import config as cf
from guesttracker import delta, dt
from guesttracker import errors as er
from guesttracker import functions as f
from guesttracker import getlog
from jgutils.jgutils import pandas_utils as pu
from jgutils.jgutils.secrets import SecretsManager

log = getlog(__name__)

# TODO Need to re implement error wrapping.
# Need to either only wrap specific high-level session funcs, or call everything through a custom db method.
# cursor/session .execute, .query


#  (sqlalchemy.exc.InvalidRequestError) Can't reconnect until invalid transaction is rolled back - setModelData

#   File "C:\Users\Jayme\AppData\Local\pypoetry\Cache\virtualenvs\guesttracker-vpjpMWts-py3.
# 9\lib\site-packages\sqlalchemy\engine\default.py", line 717, in do_execute
#     cursor.execute(statement, parameters)
# sqlalchemy.exc.OperationalError: (pyodbc.OperationalError) ('08S01', '[08S01] [Microsoft][ODBC Driver 17
# for SQL Server]TCP Provider: An existing connection was forcibly closed by the remote host.


def wrap_single_class_func(cls, func_name, err_func):
    func = getattr(cls, func_name)
    setattr(cls, func_name, err_func(func))


def wrap_connection_funcs():
    """
    try to wrap specific sqlalchemy class funcs in error handler to reset db connection on disconnects
    - NOTE may need to wrap more than just this
    """

    funcs = [
        (Connection, 'execute'),
        (Pool, 'connect')]

    for cls, func_name in funcs:
        wrap_single_class_func(cls=cls, func_name=func_name, err_func=e)


def get_odbc_driver() -> str:
    """Get best odbc driver installed on system"""
    avail_drivers = pyodbc.drivers()
    preferred_drivers = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server',
        'SQL Server Native Client 11.0']

    # compare preferred drivers with existing, loop until match
    for driver in preferred_drivers:
        if driver in avail_drivers:
            return driver


def get_db_creds() -> Dict[str, str]:
    m = SecretsManager('db.yaml').load
    driver = get_odbc_driver()

    if not driver is None:
        m['driver'] = f'{{{driver}}}'
        log.info(f'driver: {driver}')
        return m
    else:
        # raise error to user
        from guesttracker.gui.dialogs.base import msg_simple
        msg = 'No database drivers available, please download "ODBC Driver 17 for SQL Server" (or newer) from:\n\n \
        https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15'
        msg_simple(icon='critical', msg=msg)


def str_conn():
    m = get_db_creds()
    db_string = ';'.join('{}={}'.format(k, v) for k, v in m.items())
    params = parse.quote_plus(db_string)
    return f'mssql+pyodbc:///?odbc_connect={params}'


def _create_engine():
    """Create sqla engine object
    - sqlalchemy.engine.base.Engine
    - Used in DB class and outside, eg pd.read_sql
    - any errors reading db_creds results in None engine"""

    # connect_args = {'autocommit': True}
    # , isolation_level="AUTOCOMMIT"

    return create_engine(
        str_conn(),
        fast_executemany=True,
        pool_pre_ping=True,
        pool_timeout=5,
        pool_recycle=1700)
    # connect_args={'Remote Query Timeout': 5})


def e(func):

    # exc.IntegrityError,
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except exc.IntegrityError as e:
            log.warning(f'***** Re-raising: {type(e)}')
            raise
        except (exc.IntegrityError, exc.ProgrammingError, exc.StatementError, pyodbc.ProgrammingError) as e:
            log.warning(f'***** Not handling error: {type(e)}')

            # print(f'Error message:\n\n{e}') # sqlalchemy wraps pyodbc.IntegreityError and returns msg as string

            db.rollback()
            return None  # re raising the error causes sqlalchemy to catch it and raise more errors

        except exc.InvalidRequestError as e:
            # rollback invalid transaction
            log.warning(f'Rollback and retry operation: {type(e)}')
            db.rollback()
            return func(*args, **kwargs)

        except (exc.OperationalError, exc.DBAPIError, exc.ResourceClosedError) as e:
            log.warning(f'Handling {type(e)}')
            db.reset()
            return func(*args, **kwargs)

        except Exception as e:
            log.warning(f'Handling other errors: {type(e)}')
            db.reset()
            return func(*args, **kwargs)

    return wrapper


class DB(object):
    def __init__(self):
        __name__ = 'HBA Guest Tracker Database'
        log.info('Initializing database')
        self.reset(False)

        df_unit = None
        df_fc = None
        df_component = None
        dfs = {}
        domain_map = dict(SMS='KOMATSU', Cummins='CED', Suncor='NETWORK')
        domain_map_inv = f.inverse(m=domain_map)
        last_internet_success = dt.now() + delta(seconds=-61)
        f.set_self(vars())

        self.expected_exceptions = []

    def check_internet(self, host='8.8.8.8', port=53, timeout=3, recheck_time=60):
        """
        Test if internet connection exists before attempting any database operations
        Host: 8.8.8.8 (google-public-dns-a.google.com)
        OpenPort: 53/tcp
        Service: domain (DNS/TCP)
        recheck_time : int, default 60
            only re-check every x seconds
        """
        # raise er.NoInternetError() # testing

        # Kinda sketch, but just avoid re-checking too frequently
        if (dt.now() - self.last_internet_success).seconds < recheck_time:
            return True

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((host, port))
            s.shutdown(socket.SHUT_RDWR)
            s.close()
            self.last_internet_success = dt.now()
            return True
        except socket.error as ex:
            raise er.NoInternetError()

    def rollback(self):
        """Wrapper for session rollback"""
        try:
            self.session.rollback()
        except Exception as e:
            # not sure if this would be critical or can just be ignored yet
            log.warning(f'Failed to rollback session.: {type(e)}')

    def reset(self, warn: bool = True) -> None:
        """set engine objects to none to force reset, not ideal"""
        if warn:
            log.warning('Resetting database.')

        self._engine, self._session = None, None

    def clear_saved_tables(self):
        # reset dfs so they are forced to reload from the db
        from guesttracker.gui._global import update_statusbar
        self.dfs = {}
        update_statusbar('Saved database tables cleared.')

    def safe_func(self, func: Callable, *args, **kw) -> Any:
        """Call func and reset db one time if failed (try to reconnect)

        Parameters
        ----------
        func : Callable
            function to wrap
        expected_exceptions : Union[Exception, List[Exception]]
            hidden kw, pass in to not suppress specific expected exceptions

        Returns
        -------
        Any
            result of sqlalchemy function call

        Raises
        ------
        er.SMSDatabaseError
            if second attempt fails
        """

        # always check for expected_exceptions in kws
        expected_exceptions = f.as_list(kw.pop('expected_exceptions', []))

        _func = functools.partial(func, *args, **kw)

        try:
            return _func()

        except Exception as e:
            # pyodbc.Error raised as generic sqlalchemy.exc.DBAPIError

            # allow not suppressing exception
            if type(e) in expected_exceptions:
                raise e
            else:
                log.warning(f'type e: {type(e)}')
                if isinstance(e, exc.DBAPIError):
                    log.warning(f'_message: {e._message}')

            log.warning(f'Failed db func (retrying): {func}, {e}')
            self.reset()

            # try one more time after reset
            try:
                return _func()
            except Exception as e:
                fail_msg = f'Failed db func: {func}\n\targs: {args}, kw: {kw}\n\troot error: {str(e)}'
                raise er.SMSDatabaseError(fail_msg) from e

    def safe_execute(self, sql: str, **kw) -> None:
        """Convenience wrapper for session.execute

        Parameters
        ----------
        sql : str
            query to execute
        """
        self.safe_func(self.session.execute, sql, **kw)

    def safe_commit(self, fail_msg: str = None) -> bool:
        """Commit transaction to db and rollback if fail

        Parameters
        ----------
        fail_msg : str, optional
            default None

        Returns
        -------
        bool
            if commit succeeded
        """
        session = self.session
        try:
            session.commit()
            return True
        except Exception as e:
            # wrapping all sqla funcs causes this error to be exc.ResourceClosedError, not IntegrityError
            if isinstance(e, pyodbc.IntegrityError):
                fail_msg = 'Can\'t add row to database, already exists!'

            if fail_msg is None:
                fail_msg = f'Failed to commit database transaction | {type(e)}'

            er.log_error(msg=fail_msg, log=log, display=True)
            session.rollback()
            return False

    @property
    def engine(self):
        self.check_internet()

        if self._engine is None:
            self._engine = _create_engine()

        if self._engine is None:
            raise er.SMSDatabaseError('Can\'t connect to database.')

        return self._engine

    @property
    def conn(self):
        """Raw connection obj
        """
        return self.engine.raw_connection()

    @property
    def is_closed(self) -> bool:
        """Wrapper to test if connection is open/closed"""
        return self.conn.closed

    @property
    def cursor(self):
        """Raw cursor used for db operations other than refreshing main tables
        - saving cursor to self._cursor (for no reason) was holding onto connection and causing
        errors, which wasn't cleared till error properly handled
        """

        try:
            try:
                return self.conn.cursor()
            # except (pyodbc.ProgrammingError, pyodbc.OperationalError) as e:
            except Exception as e:
                log.warning(f'Resetting cursor: {type(e)}: {e}')
                self.reset()  # retry onece to clear everything then try again
                return self.conn.cursor()
        except Exception as e:
            raise er.SMSDatabaseError('Couldn\'t create cursor.') from e

    @property
    def session(self) -> Session:
        self.check_internet()  # need to call every time in case using _session
        if self._session is None:
            try:
                # create session, this is for the ORM part of sqlalchemy
                self._session = sessionmaker(bind=self.engine)()
                # TODO wrap session methods to retry?

            except Exception as e:
                raise er.SMSDatabaseError('Couldn\'t create session.') from e

        return self._session

    @er.errlog('Error closing raw_connection')
    def close(self):
        if self._engine is None:
            return
        self._engine.raw_connection().close()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def add_row(self, row):
        """Simple add single row to database.
        - Row must be created with sqlalchemy model
        """
        self.session.add(row)
        return self.safe_commit()

    def read_query(self, q):
        return pd.read_sql(sql=q.get_sql(), con=self.engine)

    def get_smr(self, unit: str, date: dt) -> int:
        """Get smr for unit on specific date from db

        Parameters
        ----------
        unit : str
        date : dt

        Returns
        -------
        int
        """
        a = T('UnitSMR')
        q = Query().from_(a).select('SMR') \
            .where(a.DateSMR == date) \
            .where(a.Unit == unit)
        return self.query_single_val(q)

    def get_smr_prev_co(self, unit: str, date: dt, floc: str) -> int:
        """Return UNIT SMR at previous changeout

        Parameters
        ----------
        unit : str
        date : dt
            date to check before
        floc : str
            component floc

        Returns
        -------
        int
            UNIT smr at prev changeout, or 0 if none found
        """
        a = T('EventLog')
        q = Query().from_(a).select(a.SMR) \
            .where(a.Unit == unit) \
            .where(a.DateAdded <= date) \
            .where(a.Floc == floc) \
            .orderby(a.DateAdded, order=Order.desc)

        return self.query_single_val(q)

    def get_df_saved(self, name: str) -> Union[pd.DataFrame, None]:
        """Return df from saved cache"""
        # TODO load if doesn't exist?
        return self.dfs.get(name, None)

    def save_df(self, df: Union[pd.DataFrame, List[str]], name: str) -> None:
        """Save dataframe (or list) to cache"""
        self.dfs[name] = df

    def fix_customer_units(self, df: pd.DataFrame, col: str = 'unit') -> pd.DataFrame:
        """Replace Suncor's leading zeros in unit columnm
        - Fix fluidlife's complicated units
        - Split on delmitiers eg "-" and chose unit as first part
        - eg "F0301 - SMR" > "F301"
        """

        m = {'^F0': 'F', '^03': '3', '^02': '2', '^06': '6'}
        df[col] = df[col].replace(m, regex=True)

        # split on " ", "-(", "/" to fix fluidlife units
        split_tokens = [' ', r'-\(', r'/']
        for token in split_tokens:
            df[col] = df[col].str.split(token, expand=True)[0]

        return df

    def get_unit_val(self, unit: str, field: Union[str, list]) -> Union[str, pd.DataFrame, None]:
        """Get single/multiple values for unit from df unit
        - TODO bit messy, should have better structure to get any val from saved table

        Parameters
        ----------
        unit : str
        field : Union[str, list]
            single field or list of fields

        Returns
        -------
        Union[str, pd.DataFrame, None]
            single val or horizontal df of all vals with index dropped
        """
        df = self.get_df_unit()

        try:
            df = df.loc[unit.strip(), field]

            if isinstance(field, list):
                df = df.to_frame().T.reset_index(drop=True)

            return df
        except KeyError:
            log.warning(f'Couldn\'t get value "{field}" for unit "{unit}" in unit table.')
            return None

    def unit_exists(self, unit: str) -> bool:
        """Checck if unit exists in database"""
        df = self.get_df_unit()
        return unit in df.Unit

    def units_not_in_db(self, units: list):
        """Check list of units in db
        Returns
        -------
        list
            list of units not in db
        """
        df = self.get_df_unit()
        lst = df[df.Unit.isin(units)].Unit.to_list()
        return list(set(units) - set(lst))

    def unit_from_serial(
            self,
            serial: str,
            model: str = None,
            minesite: str = None) -> Union[str, None]:
        """Try to get unit from serial and model/minesite if provided

        Parameters
        ----------
        serial : str
            unit serial number
        model : str, optional
            model number to fitler on, default None
        minesite : str, optional
            minesite to fitler on, default None

        Returns
        -------
        Union[str, None]
            unit if matched, else None
        """
        unit = None

        df = self.get_df_unit() \
            .query('Serial == @serial')

        if not model is None:
            model = model.replace("'", '')
            df = df.query('Model.str.contains(@model)')

        if not minesite is None:
            df = df.query('MineSite.str.contains(@minesite)')

        # unit found
        if df.shape[0] == 1:
            unit = df.iloc[0, df.columns.get_loc('Unit')]

        return unit

    def get_modelbase(self, model: str) -> Union[str, None]:
        """Get model base from model"""
        df = self.get_df_equiptype()
        try:
            return df.loc[model].ModelBase
        except KeyError:
            # model not in database
            return None

    def get_df_equiptype(self):
        if not hasattr(self, 'df_equiptype') or self.df_equiptype is None:
            self.set_df_equiptype()

        return self.df_equiptype

    def unique_units(self, **kw):
        df = self.get_df_unit(**kw)
        return df.Unit.unique()

    def filter_database_units(self, df: pd.DataFrame, col: str = 'Unit') -> pd.DataFrame:
        """Filter dataframe to only units in database

        Parameters
        ----------
        df : pd.DataFrame
        col : str, optional
            col to filter on, by default 'Unit'

        Returns
        -------
        pd.DataFrame
        """
        return df[df[col].isin(self.unique_units())]

    def get_df_emaillist(self, force=False):
        name = 'emaillist'
        df = self.get_df_saved(name)

        if df is None or force:
            from guesttracker.queries import EmailList
            query = EmailList()
            df = query.get_df()
            self.save_df(df, name)

        return df

    def get_email_list(self, name, minesite, usergroup=None):
        """Get list of emails from db for specified name eg 'WO Request'
        - NOTE not used, just use EmailListShort to refresh on every call"""
        if usergroup is None:
            usergroup = 'SMS'
        df = self.get_df_emaillist()

        return list(df[
            (df.MineSite == minesite) &
            (df.UserGroup == usergroup) &
            (df[name].str.lower() == 'x')].Email)

    def get_df_issues(self, force=False):
        name = 'issues'
        df = self.get_df_saved(name)

        if df is None or force:
            df = pd.read_csv(cf.p_res / 'csv/issue_categories.csv')
            self.save_df(df, name)

        return df

    def get_issues(self):
        df = self.get_df_issues()
        return f.clean_series(df.category)

    def get_sub_issue(self, issue):
        df = self.get_df_issues()
        return list(df.sub_category[df.category == issue])

    def get_list_minesite(self, include_custom: bool = True) -> List[str]:
        """Get list of all unique minesites in UnitInfo table from db

        Parameters
        ----------
        include_custom : bool, optional
            include "CustomSites" for returning multiple MineSites in queries, default True

        Returns
        -------
        List[str]
            list of minesites
        """
        name = 'lst_minesite'
        lst_minesite = self.get_df_saved(name)

        if lst_minesite is None:
            lst_minesite = f.clean_series(s=self.get_df_unit().MineSite)
            self.save_df(df=lst_minesite, name=name)

        if include_custom:
            lst_minesite = ['CustomSites'] + lst_minesite

        return lst_minesite

    def get_df_parts(self, force: bool = False) -> pd.DataFrame:
        name = 'parts'
        df = self.get_df_saved(name)

        if df is None or force:
            from guesttracker.queries.misc import Parts
            query = Parts()
            df = query.get_df()
            self.save_df(df, name)

        return df

    def get_df_unit(
            self,
            minesite: Union[str, None] = None,
            model: Union[str, None] = None,
            force: bool = False,
            **kw) -> pd.DataFrame:
        """Return df of all units in database

        Parameters
        ----------
        minesite : Union[str, None], optional
            default None
        model : Union[str, None], optional
            default None
        force : bool, optional
            force reload, default False

        Returns
        -------
        pd.DataFrame
        """
        name = 'units'
        df = self.get_df_saved(name)

        # load if doesn't exist
        if df is None or force:
            a, b = pk.Tables('UnitID', 'EquipType')
            cols = [a.MineSite, a.Customer, a.Model, a.Unit, a.Serial,
                    a.DeliveryDate, b.EquipClass, b.ModelBase, a.is_component]
            q = Query.from_(a).select(*cols) \
                .left_join(b).on_field('Model')

            df = pd.read_sql(sql=q.get_sql(), con=self.engine) \
                .set_index('Unit', drop=False) \
                .pipe(pu.parse_datecols)

            self.save_df(df, name)

        # sometimes need to filter other minesites due to serial number duplicates
        if not minesite is None:
            df = df[df.MineSite == minesite].copy()

        if not model is None:
            df = df[df.Model.str.contains(model)]

        return df

    def set_df_equiptype(self):
        a = T('EquipType')
        q = Query().from_(a).select(a.star)
        self.df_equiptype = pd.read_sql(sql=q.get_sql(), con=self.engine) \
            .set_index('Model', drop=False)

    def get_df_fc(
            self,
            minesite: Union[str, None] = None,
            unit: Union[str, None] = None,
            default: bool = True) -> pd.DataFrame:
        """Get df of FCs per unit

        Parameters
        ----------
        minesite : Union[str, None]
        unit : Union[str, None]
        default : bool
            Filter to Mandatory or Non-Expired and Complete==False, default True

        Returns
        -------
        pd.DataFrame
        """

        name = 'fc'
        df = self.get_df_saved(name)

        if df is None:
            from guesttracker.queries import FCOpen
            df = FCOpen().get_df(default=False)
            self.save_df(df, name)

        if not minesite is None:
            df = df[df.MineSite == minesite]

        if not unit is None:
            df = df[df.Unit == unit]

        # kinda sketch to filter here
        if default:
            df = df[
                ((df.Type == 'M') | (df.ExpiryDate >= dt.now())) &
                (df.Complete == False)]  # noqa (needs to be ==False)

        return df

    def set_df_fc(self):
        from guesttracker.queries import FCOpen
        self.df_fc = FCOpen().get_df(default=False)

    def combine_comp_modifier(self, df, cols: list, target: str = 'combined', sep: str = ', '):
        """Create combined col for component/modifier"""
        df[target] = df[cols].apply(
            lambda x: f'{x[0]}{sep}{x[1]}' if not x[1] is None else x[0], axis=1)

    def get_df_component(self):
        name = 'component'
        df = self.get_df_saved(name)

        if df is None:
            a = T('ComponentType')
            q = Query.from_(a).select('*')
            df = self.read_query(q=q)

            self.combine_comp_modifier(df=df, cols=['Component', 'Modifier'], target='Combined')
            self.save_df(df, name)

        return df

    def get_df_oil_components(self, unit: str = None, minesite: str = None):
        """Return uniqe unit/component/modifier combinations from oil samples

        Parameters
        ----------
        unit : str, optional
            filter to unit, default None
        minesite : str, optional
            filter to components per minesite

        Returns
        -------
        pd.DataFrame
            df of unit, component, modifier
        """
        name = 'oil_comps'
        df = self.get_df_saved(name)

        if df is None:
            a = T('OilSamples')
            cols = [a.unit, a.component_id, a.modifier]
            q = Query.from_(a) \
                .select(*cols) \
                .groupby(*cols)

            df = self.read_query(q=q) \
                .sort_values(by=['unit', 'component_id', 'modifier'])

            self.combine_comp_modifier(df=df, cols=['component_id', 'modifier'], target='combined', sep=' - ')

            dfu = self.get_df_unit() \
                .rename_axis('index')

            df = df \
                .merge(
                    right=dfu[['Unit', 'MineSite', 'ModelBase']],
                    how='left',
                    left_on='unit',
                    right_on='Unit') \
                .drop(columns=['Unit']) \
                .rename(columns=dict(MineSite='minesite', ModelBase='model_base'))

            self.save_df(df, name)

        if not unit is None:
            df = df[df.unit == unit]

        if not minesite is None:
            df = df[df]

        return df

    @er.errlog('Failed to import dataframe')
    def import_df(
            self,
            df: pd.DataFrame,
            imptable: str,
            impfunc: str,
            notification: bool = True,
            prnt: bool = False,
            chunksize: int = None,
            index: bool = False,
            if_exists: str = 'append',
            import_name: str = None) -> Union[int, None]:

        rowsadded = 0
        if df is None or len(df) == 0:
            if imptable == 'temp_import' and not import_name is None:
                imptable = import_name
            log.warning(f'No rows to import to: {imptable}')

            fmt = '%Y-%m-%d %H:%M'
            if notification:
                f.discord(
                    msg=f'{dt.now().strftime(fmt)} - {imptable}: No rows to import',
                    channel='sms')

            return

        df.to_sql(
            name=imptable,
            con=self.engine,
            if_exists=if_exists,
            index=index,
            chunksize=chunksize)

        cursor = self.cursor
        rowsadded = cursor.execute(impfunc).rowcount
        cursor.commit()

        import_name = import_name or imptable
        msg = f'{import_name}: {rowsadded}'
        if prnt:
            log.info(msg)

        if notification:
            f.discord(msg=msg, channel='sms')

        return rowsadded

    def insert_update(
            self,
            a: str,
            df: pd.DataFrame,
            join_cols: list = None,
            b: str = 'temp_import',
            **kw) -> int:
        """Insert values from df into temp update table b and merge to a

        Parameters
        ----------
        a : str
            insert into table
        b : str
            select from table (temp table)
        join_cols : str
            colums to join a/b on
        df : pd.DataFrame

        Returns
        -------
        int
            rows added
        """
        if b == 'temp_import':
            kw['if_exists'] = 'replace'

        if join_cols is None:
            from guesttracker import dbtransaction as dbt
            join_cols = dbt.get_dbtable_keys(dbtable=a)

        imptable = b

        # drop duplicates
        if not df is None and len(df) > 0:

            # sometimes df will have been converted to lower cols
            join_cols_lower = [c.lower() for c in join_cols]
            subset = join_cols if not all(c in df.columns for c in join_cols_lower) else join_cols_lower

            df = df \
                .drop_duplicates(subset=subset, keep='first')

            a, b = pk.Tables(a, b)
            cols = df.columns

            # this builds an import function from scratch, replaces stored proceedures
            q = Query.into(a) \
                .columns(*cols) \
                .from_(b) \
                .left_join(a).on_field(*join_cols) \
                .select(*cols) \
                .where(a.field(join_cols[0]).isnull())
        else:
            q = ''

        rowsadded = self.import_df(df=df, imptable=imptable, impfunc=str(q), import_name=a, **kw)
        # self.cursor.execute(f'TRUNCATE TABLE {b};')
        self.cursor.execute(f'DROP TABLE {b};')
        self.cursor.commit()

        msg = f'{a}: {rowsadded}'
        log.info(msg)

        return rowsadded

    def query_single_val(self, q: Query) -> Any:
        """Query single val from db

        Parameters
        ----------
        q : Query

        Returns
        -------
        Any
        """
        return self.cursor.execute(q.get_sql()).fetchval()

    def max_date_db(self, table=None, field=None, q=None, join_minesite=True, minesite='FortHills'):
        a = T(table)
        b = T('UnitID')

        if q is None:
            q = a.select(fn.Max(a[field]))

            if join_minesite:
                q = q.left_join(b).on_field('Unit') \
                    .where(b.MineSite == minesite)

        val = self.query_single_val(q)

        return f.convert_date(val)


db = DB()
