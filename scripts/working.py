# %%
import sqlite3
import time
import uuid

import pandas as pd
from IPython.display import display
from sqlalchemy.orm.decl_api import DeclarativeMeta

from guesttracker import getlog
from guesttracker.database import db
from guesttracker.utils import dbmodel as dbm
from jgutils.jgutils import pandas_utils as pu
from jgutils.jgutils.functions import PrettyDict as PD

log = getlog('working')

# %%
con = sqlite3.connect('guesttracker.sqlite')
c = con.cursor()

# %%

# recalculate later
# last_stay

tables = [
    'Accounts',
    'ChargeItems',
    'Charges',
    'Classes',
    'Packages',
    'PackageUnits',
    'People',
    'Reservations',
    'Units'
]

rename_table = dict(
    People='Customers'
)

# %%


def int_fields(name, n, n0: int = 1):
    return [f'{name}{i}' for i in range(n0, n + 1)]


drop_cols = dict(
    Accounts=['tax2_rate'],
    ChargeItems=['tax2_rate'],
    Charges=['exported', 'cc_kind', 'cc_number', 'cc_exp_date', 'cc_name_on_card', 'item_id', 'taken_by'],
    Customers=['email2', 'next_stay', 'other_names', 'name_prefix', 'addr2_1', 'addr2_2', 'city_state_zip_1', 'company_2', 'city_2', 'country_2',
               'state_2', 'zip_2', 'city_state_zip_2', 'fax_phone', 'call_back', 'name', 'all_interests', 'first_last', 'last_stay', 'primary_address'] + int_fields('user_field', 6),
    Packages=['plan', 'rate2', 'extra_person_charge', 'nights', 'minimum_nights', 'service_charge_percent',
              'service_charge_id', 'has_units', 'has_seasons', 'has_components', 'balanced', 'for_up_to'] + int_fields('rate_option', 4),
    Reservations=['credit_card_name', 'credit_card_number', 'lock_units', 'taken_by', 'arrival_time', 'departure_time', 'name', 'confirmation_method', 'confirmation_number',
                  'credit_card_exp', 'credit_card_kind', 'group_notes', ] + int_fields('number_of_people', 4, 2) + int_fields('user_field', 6) + int_fields('user_check', 2),
    Units=['book_online', 'extension2', 'show_online', 'status',
           'url', 'extension', 'description', 'has_charges', 'notes']
)

rename_cols = dict(
    Accounts=dict(tax1_rate='tax_rate'),
    ChargeItems=dict(tax1_rate='tax_rate'),
    Charges=dict(person_id='customer_id'),
    Customers=dict(
        first_name='name_first', last_name='name_last', company_1='company', addr1_1='addr1', addr1_2='addr2', city_1='city', state_1='state', zip_1='zip', country_1='country'),
    Packages=dict(rate1='rate'),
    Reservations=dict(number_of_people1='num_persons', primary_contact_id='customer_id'),
    Units=dict(capacity='max_persons')
)

date_cols = dict(
    Customers=['first_contact', 'last_contact'],
)

int_cols = dict(
    Accounts=['number'],
    Charges=['sub_kind', 'quantity'],
    Reservations=['num_persons']
)

bool_cols = dict(
    ChargeItems=['includes_tax'],
    Charges=['includes_tax'],
    Customers=['international']
)

foreign_keys = dict(
    ChargeItems=dict(account_id='Accounts'),
    Charges=dict(customer_id='Customers', reservation_id='Reservations',
                 unit_id='Units', account_id='Accounts', package_id='Packages'),
    Packages=dict(account_id='Accounts'),
    PackageUnits=dict(package_id='Packages', unit_id='Units'),
    Reservations=dict(customer_id='Customers'),
    Units=dict(class_id='Classes'),
)

dfs_proc = {}
for table in tables:
    sql = f'select * from {table}'
    table = rename_table.get(table, table)

    df = pd.read_sql(sql=sql, con=con) \
        .pipe(pu.lower_cols) \
        .drop(columns=drop_cols.get(table, [])) \
        .rename(columns=rename_cols.get(table, {})) \
        .pipe(pu.parse_datecols, include_cols=date_cols.get(table, [])) \
        .assign(**{c: lambda x, c=c: x[c].astype(pd.Int64Dtype()) for c in int_cols.get(table, [])}) \
        .assign(**{c: lambda x, c=c: x[c].astype(pd.BooleanDtype()) for c in bool_cols.get(table, [])}) \
        .assign(uid=lambda x: x.index.to_series().map(lambda x: uuid.uuid4()))

    if table == 'Customers':
        df = df.assign(
            name_first=lambda x: x.name_first.str.title(),
            name_last=lambda x: x.name_last.str.title(),
            name=lambda x: x.name_first + ' ' + x.name_last)

    elif table == 'Units':
        df = df.assign(active=True)

    # reorder df columns so uid is first
    first_cols = ['id', 'uid']
    cols_reordered = first_cols + [c for c in df.columns if c not in first_cols]
    df = df[cols_reordered]

    dfs_proc[table] = df

# join foreign id tables on id and replace id with uid
for table, m_fk in foreign_keys.items():
    for col_rel, table_rel in m_fk.items():
        df1 = dfs_proc[table]
        df2 = dfs_proc[table_rel][['id', 'uid']] \
            .rename(columns={'id': 'id_rel', 'uid': 'uid_rel'})

        dfs_proc[table] = df1 \
            .merge(df2, left_on=col_rel, right_on='id_rel', how='left') \
            .drop(columns=[col_rel, 'id_rel']) \
            .rename(columns={'uid_rel': col_rel})


for table, df in dfs_proc.items():
    print(table)
    display(df.head())
    display(df.info())

# %%

for table, df in dfs_proc.items():
    start = time.time()

    log.info(f'Uploading: {table}')

    df = df \
        .drop(columns='id') \
        .to_sql(name=table, con=db.engine, if_exists='replace', index=False)

    log.info(f'Uploaded: {table} in {time.time() - start:.0f} seconds\n')

# %%
for table in tables:
    table = rename_table.get(table, table)
    # sql to set primary key to uid column
    sql = f'alter table {table} alter column uid varchar(36) not null;'
    sql += f' alter table {table} add primary key (uid);'
    print(sql)
    try:
        db.safe_execute(sql)
    except:
        pass

db.safe_commit()

# %% - make table key vals for config.yaml

models = {k: v for k, v in dbm.__dict__.items() if isinstance(v, DeclarativeMeta) and k != 'Base'}

m = {k: {
    pu.from_snake(c.name): c.name for c in model.__table__.columns if not c.name.endswith('_id')}
    for k, model in models.items()}

PD(m)


# %%
table = 'People'
sql = f'select * from {table}'
df = pd.read_sql(sql=sql, con=con)
df

# %%

tables = c.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
tables = [r[0] for r in tables]
tables
