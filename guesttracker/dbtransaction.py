from typing import *

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import and_, literal
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.query import Query as SQLAQuery

from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.database import db
from guesttracker.utils import dbmodel as dbm

if TYPE_CHECKING:
    from guesttracker.gui.datamodel import TableDataModel

log = getlog(__name__)


class DBTransaction():
    def __init__(self, data_model=None, dbtable=None, title=None, table_view=True, table_widget=None, **kw):
        """Database transaction object for bulk updates/deletes etc
        - need dbtable, df or list of dicts containing appropriate pks and vals to update

        Parameters
        ----------
        data_model : guesttracker.gui.tables.TableWidget, optional
            table model from tables.py, default None
        dbtable : dbm.Base, optional
            dbtable definition not instance, default None
        title : str, optional
            table view title used for converting between db and table_view column names, default None
        table_view : bool, optional
            pass in False if cols are already in db view, default True

        Raises
        ------
        ValueError
            if missing dbtable
        """

        update_items = []

        if not data_model is None:
            table_widget = data_model.table_widget
            title = table_widget.title
            dbtable = table_widget.get_dbtable()

        if dbtable is None:
            raise ValueError('dbtable cannot be none!')

        pks = get_dbtable_keys(dbtable)

        # convert db to view cols first
        all_cols = f.convert_list_db_view(title=title, cols=pks) if table_view else pks

        f.set_self(vars())

    def update_statusbar(self, msg, *args, **kw):
        if not self.table_widget is None:
            self.table_widget.mainwindow.update_statusbar(msg=msg, *args, **kw)

        print(msg)

    def add_df(self, df, update_cols=None):
        """Add full or sliced df to update queue"""
        if not update_cols is None:
            if not isinstance(update_cols, list):
                update_cols = [update_cols]
            self.all_cols.extend(update_cols)

        # pass in df with all rows to update, then filter update_cols + pk_cols
        df = df[self.all_cols]

        if self.table_view:
            df = f.convert_df_db_cols(title=self.title, df=df)

        self.update_items = df.to_dict(orient='records')

        return self

    def add_items(self, update_items):
        # update_items is list of dicts
        # convert all view cols to db cols
        self.update_items = [f.convert_dict_db_view(title=self.title, m=item, output='db') for item in update_items]

        return self

    def add_row(self, irow):
        """Add single row by index number from table
        - NOTE probably need to work with values passed in manually, maybe won't use this, df is pretty gr8"""
        df = self.df

        # convert all col_ixs to db_field names and attach values to update
        m = {}
        for icol in self.col_nums:
            view_header = df.columns[icol]
            db_field = f.convert_header(title=self.title, header=view_header, inverse_=True)
            m[db_field] = df.iloc[irow, icol]

        self.update_items.append(m)

    def update_all(self, operation_type: str = 'update') -> Union['DBTransaction', None]:
        """Update all rows in self.update_items

        Parameters
        ----------
        operation_type : str, optional
            update or delete, default 'update'

        Returns
        -------
        DBTransaction
            self
        """
        txn_func = getattr(db.session, f'bulk_{operation_type}_mappings')
        # txn_func(self.dbtable, self.update_items)

        db.safe_func(txn_func, self.dbtable, self.update_items)

        num_recs = len(self.update_items)
        if num_recs == 0:
            log.info('No records to update.')
            return

        msg = f'Bulk {operation_type} records: {num_recs}'

        if db.safe_commit():
            self.update_statusbar(msg, success=True)
        else:
            msg = f'Failed: {msg}'
            self.update_statusbar(msg, warn=True)

        return self

    def print_items(self):
        print(self.update_items)
        return self


class Row():
    def __init__(
            self,
            data_model: 'TableDataModel' = None,
            i: int = None,
            col: int = None,
            keys: Dict[str, str] = None,
            dbtable: dbm.Base = None,
            df: pd.DataFrame = None,
            title: str = None):
        """Create with either: 1. gui.Table + row, or 2. dbtable + keys/values

        tbl = gui.Table class > the 'model' in mvc
        """
        if keys is None:
            keys = {}  # don't know why, but set_self doesnt work if keys={}

        if not data_model is None:
            df = data_model.df
            title = data_model.table_widget.title
            if dbtable is None:
                dbtable = data_model.table_widget.get_dbtable()
                # dbm.TableName = table definition, NOT table object (eg TableName())

        if dbtable is None:
            raise AttributeError('db model table not set!')

        pks = get_dbtable_keys(dbtable)  # list of pk field names eg ['UID']

        if not (i is None and col is None):  # update keys from df
            if df is None:
                raise AttributeError('df not set!')

            for pk in pks:
                if not i is None:
                    header = f.convert_header(title=title, header=pk, inverse_=True)
                    keys[pk] = df.iloc[i, df.columns.get_loc(header)]  # get key value from df, key must exist in df
                elif col:
                    # transposed df from Details dialog, all fields are db_cols eg 'FCNumber' not 'FC Number'
                    keys[pk] = df.loc[pk, col]

        f.set_self(vars())

    @classmethod
    def example(cls, uid: int = None, from_db: bool = True):
        """Create instance of self with uid, only for EventLog table"""
        if uid is None:
            uid = 12602260565

        row = cls(keys=dict(UID=uid), dbtable=dbm.EventLog)

        if from_db:
            return row.create_model_from_db()
        else:
            return row

    def update_single(
            self,
            val: Any,
            header: str = None,
            field: str = None,
            check_exists: bool = False) -> None:
        """Convenience func to update single field/header: val in db"""

        # convert table header to db field name
        if field is None:
            field = f.convert_header(title=self.title, header=header)

        self.update(vals={field: val}, check_exists=check_exists)

    def update(
            self,
            vals: Dict[str, Any] = None,
            delete: bool = False,
            check_exists: bool = False) -> None:
        """Update (multiple) values in database, based on unique row, field, value, and primary keys(s)
        - key must either be passed in manually or exist in current table's df"""
        t, keys = self.dbtable, self.keys

        if len(keys) == 0:
            raise AttributeError('Need to set keys before update!')

        session = db.session
        cond = [getattr(t, pk) == keys[pk] for pk in keys]  # list of multiple key:value pairs for AND clause

        if not delete:
            if vals is None:
                raise AttributeError('No values to update!')

            sql = sa.update(t).values(vals).where(and_(*cond))
            print(sql)
        else:
            sql = sa.delete(t).where(and_(*cond))  # kinda sketch to even have this here..

        if not check_exists:
            db.safe_execute(sql)
        else:
            # Check if row exists, if not > create new row object, update it, add to session, commit
            q = session.query(t).filter(and_(*cond))
            func = session.query(literal(True)).filter(q.exists()).scalar
            exists = db.safe_func(func)

            if not exists:
                e = t(**keys, **vals)
                session.add(e)
            else:
                db.safe_execute(sql)

        return db.safe_commit()  # True if transaction succeeded

    def create_model_from_db(self) -> SQLAQuery:
        """Query sqalchemy orm session using model eg dbo.EventLog, and keys eg {UID=123456789}

        Returns
        ------
            instance of model
        """
        func = db.session.query(self.dbtable).get
        return db.safe_func(func, self.keys)

    def printself(self):
        m = dict(
            title=self.tbl.title,
            table=self.tbl.tablename,
            pk=self.pk,
            id=self.id)
        # display(m)
        print(m)


def get_rowset_db(irows: List[int], df: pd.DataFrame, dbtable: dbm.Base) -> List[dbm.Base]:
    """Get multiple row objs from db at once based on row indexes from df
    - Results are sorted in original order of irows
    - This is used to get multiple results from db in one call (instead of having to iterate eg 10 times)

    Parameters
    ----------
    irows : List[int]
        list of irows eg [2, 5, 6]
    df : pd.DataFrame
        df with all data (to get pk values)
    dbtable : dbm.Base

    Returns
    -------
    List[dbm.Base]
        list of db row results
    """
    pks = get_dbtable_keys(dbtable)
    conds = []
    df = df.copy() \
        .reset_index(drop=True)

    # create ordered keys for sorting results
    ordered_keys = [tuple(df.loc[df.index.get_loc(i), pks].tolist()) for i in irows]

    for pk in pks:
        db_col = getattr(dbtable, pk)  # eg EventLog.UID
        pk_values = df.iloc[irows, df.columns.get_loc(pk)].unique().tolist()  # eg [12345, 12334, 23442]
        conds.append(db_col.in_(pk_values))

    func = db.session.query(dbtable).filter(*conds).all
    result = db.safe_func(func)

    # get keys back from result
    m_result = {tuple([getattr(e, pk) for pk in pks]): e for e in result}

    # sort results
    return [m_result[k] for k in ordered_keys]


def select_row_by_secondary(dbtable, col, val):
    """Select single row from table by attr other than pk"""
    try:
        func = db.session.query(dbtable).filter(getattr(dbtable, col) == val).one
        return db.safe_func(func, expected_exceptions=NoResultFound)
    except NoResultFound:
        return None


def get_dbtable_key_vals(dbtable, vals: dict) -> Tuple[tuple, dict]:
    """Return tuple of one or more keys in dbtable, given dict of all vals (including keys)

    - used for update queue so far
    """
    pks = get_dbtable_keys(dbtable)
    key_tuple = tuple(vals[k] for k in pks)
    key_dict = {k: vals[k] for k in pks}
    return key_tuple, key_dict


def get_dbtable_keys(dbtable: Union[dbm.Base, str]) -> list:
    """Get list of dbtable keys

    Parameters
    ----------
    dbtable : Union[dbm.Base, str]
        eg dbm.FactoryCampaign

    Returns
    -------
    list
        list of dbtable pks eg ['Unit', 'FCNumber']
    """
    if isinstance(dbtable, str):
        table_name = dbtable
        dbtable = getattr(dbm, table_name, None)
        if dbtable is None:
            raise AttributeError(f'Can\'t get table for: {table_name}')

    return dbtable.__table__.primary_key.columns.keys()


def print_model(model, include_none=False):
    m = model_dict(model, include_none=include_none)
    try:
        print(m)
    except:
        pass


def model_dict(model, include_none=False):
    # create dict from table model
    m = {a.key: getattr(model, a.key) for a in sa.inspect(model).mapper.column_attrs}
    if not include_none:
        m = {k: v for k, v in m.items() if v is not None}

    return m


def df_from_row(model):
    # convert single row model from db to df with cols as index (used to display all data single row)
    m = model_dict(model, include_none=True)
    df = pd.DataFrame.from_dict(m, orient='index', columns=['Value']) \

    df.index.rename('Fields', inplace=True)
    return df


def join_query(tables, keys, join_field):
    """pretty ugly, but used to use an sqlachemy join query and merge dict of results
    - tables is tuple/list of 2 tables
    - NOTE not actually used yet
    """
    if not len(tables) == 2:
        raise AttributeError('"tables" must have 2 tables')

    session = db.session
    a, b = tables[0], tables[1]
    cond = getattr(a, join_field) == getattr(b, join_field)  # condition for join eg a.Unit==b.Unit

    key = list(keys.keys())[0]  # NOTE sketch, needs to be changed for multiple keys
    fltr_ = getattr(a, key) == keys[key]  # eg a.UID==uid

    func = session.query(a, b).join(b, cond).filter(fltr_).one
    res = db.safe_func(func)

    m = {}
    for item in res:
        m.update(model_dict(item, include_none=True))

    return m
