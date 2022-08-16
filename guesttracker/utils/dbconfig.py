
from collections import defaultdict
from datetime import datetime as dt
from typing import Dict, List, TypedDict

import pandas as pd

# TODO add validation funcs here?


class TDItem(TypedDict, total=False):
    primary_date: str
    exclude_add_fields: List[str]
    exclude_refresh_fields: List[str]
    add_enforce: List[str]
    warn_delete_fields: List[str]


table_data: Dict[str, TDItem] = defaultdict(
    TDItem,
    Reservations=TDItem(
        primary_date='arrival_date',
        warn_delete_fields=['unit_assignments', 'customer_name', 'arrival_date'],
        exclude_add_fields=['cancel_date', 'notes', 'requests']
    ),
    Customers=TDItem(
        primary_date='first_contact',
        exclude_add_fields=['first_contact', 'last_contact', 'notes', 'relationship', 'source', 'name'],
        add_enforce=['addr1', 'city', 'state', 'zip', 'country', 'email', 'name_first', 'name_last'],
    ),
    Charges=TDItem(
        primary_date='charge_date',
    ),
)


def set_unit_availability(
        df_unit: pd.DataFrame,
        df_res: pd.DataFrame,
        date_arrival: dt,
        date_departure: dt) -> pd.DataFrame:
    """Add "reserved" column to unit df based on all previous reservations (excluding cancellations)

    Parameters
    ----------
    df_unit : pd.DataFrame
    df_res : pd.DataFrame
        df with reservations split into units and dates
    date_arrival : dt
    date_departure : dt

    Returns
    -------
    pd.DataFrame
    """
    # check wether each unit is available for the given dates
    df_res = df_res \
        .assign(reserved=lambda x:
                ((date_arrival >= x.arrival_date) &
                 (date_arrival < x.departure_date)) |
                ((date_departure > x.arrival_date) &
                    (date_departure <= x.departure_date))) \
        .query('reserved == True')

    return df_unit \
        .assign(reserved=lambda x: x.abbr.isin(df_res.unit.unique()))
