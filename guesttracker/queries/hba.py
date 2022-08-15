from typing import TYPE_CHECKING

from pypika import MSSQLQuery as Query
from pypika import Table as T

from guesttracker import config as cf
from guesttracker import functions as f
from guesttracker.queries import QueryBase
from guesttracker.utils import dbconfig as dbc
from guesttracker.utils import dbmodel as dbm

if TYPE_CHECKING:
    from pypika.queries import QueryBuilder
    from sqlalchemy.orm.decl_api import DeclarativeMeta
    from sqlalchemy.sql.schema import Table


class HBAQueryBase(QueryBase):

    col_aliases = dict(
        customer_name='customers.name',
        class_name='classes.name',
        package_name='packages.name',
        account_name='accounts.name',
    )

    def __init__(self, name: str, **kw):
        super().__init__(select_tablename=name, title=name, **kw)
        # name = self.__class__.__name__
        self.name = name
        self.cols = self.set_cols()

        self.a = self.select_table
        self.model = getattr(dbm, name)  # type: DeclarativeMeta
        self.table = self.model.metadata.tables[self.model.__tablename__]  # type: Table

        q = Query.from_(self.select_table)
        self.q = self.join_fk_tables(q)

    def set_default_filter(self, **kw):

        if hasattr(self, 'set_allopen'):
            self.set_allopen(**kw)

    def set_cols(self):
        if not hasattr(self, 'name'):
            raise ValueError('table "name" is not set')

        name = self.name
        headers = {k.replace(' ', ''): v for k, v in cf.config['Headers'].items()}
        _cols = list(headers[name].values())

        cols = []
        for col_name in _cols:

            # check if query has defined a column alias
            alias = self.col_aliases.get(col_name, None)
            if alias:
                parts = alias.split('.')
                col_name_real = parts[-1]
                table = T(parts[0]) if len(parts) > 1 else self.select_table
                col = getattr(table, col_name_real)
                col = col.as_(col_name)
            else:
                table = self.select_table
                col = getattr(table, col_name)

            cols.append(col)

        return cols

    def join_fk_tables(self, q: 'QueryBuilder') -> 'QueryBuilder':
        """Join table's foreign keys to the query

        Parameters
        ----------
        q : QueryBuilder

        Returns
        -------
        QueryBuilder
        """
        for fk in self.table.foreign_keys:
            join_table = T(fk.column.table.name)
            q = q.left_join(join_table).on(self.a[fk.parent.name] == join_table['uid'])

        return q

    def set_allopen(self, **kw) -> None:
        """Set all open filters"""
        primary_date_col = dbc.table_data[self.name].get('primary_date', None)

        if primary_date_col:
            self.filter_last_dates(primary_date_col)


class Reservations(HBAQueryBase):
    def __init__(self, **kw):
        kw['name'] = self.__class__.__name__
        super().__init__(**kw)
        self.b = T('Customers')


class Charges(HBAQueryBase):
    def __init__(self, **kw):
        kw['name'] = self.__class__.__name__
        super().__init__(**kw)

        self.default_dtypes |= f.dtypes_dict('Int64', ['Sub Kind'])
