
from collections import defaultdict
from typing import Dict, List, TypedDict

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
        warn_delete_fields=['unit_assignments'],
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


def validate_unit_availability(session, unit_id: str, arrival_date: str, departure_date: str) -> bool:
    """
    Validate that the unit is available for the given dates.
    """
    # TODO
    return True
