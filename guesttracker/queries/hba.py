from pypika import MSSQLQuery as Query

from guesttracker import config as cf
from guesttracker.queries import QueryBase


class HBAQueryBase(QueryBase):
    def __init__(self, name: str, **kw):
        super().__init__(select_tablename=name, title=name, **kw)
        # name = self.__class__.__name__
        self.name = name
        _cols = list(cf.config['Headers'][name].values())
        self.cols = [getattr(self.select_table, c) for c in _cols]

        self.q = Query.from_(self.select_table)


class People(QueryBase):
    def __init__(self, **kw):
        super().__init__(**kw)
        a = self.select_table

        self.cols = [a.uid, a.name, a.email, a.first_contact, a.last_contact, a.home_phone,
                     a.work_phone, a.alt_phone, a.addr1_1, a.city_1, a.state_1, a.zip_1, a.country_1, a.notes]

        self.q = Query.from_(a)
        # .where(a.deleted == 0) \
        # .left_join(b).on_field('Unit') \
        # .left_join(c).on(a.CreatedBy == c.UserName) \
        # .left_join(d).on(b.Model == d.Model) \

        # self.default_dtypes |= \
        #     f.dtypes_dict('Int64', ['SMR', 'Unit SMR', 'Comp SMR', 'Part SMR', 'Pics']) | \
        #     f.dtypes_dict('bool', ['Comp CO'])
