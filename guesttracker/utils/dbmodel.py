from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Float, PrimaryKeyConstraint, String,
    text)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Accounts(Base):
    __tablename__ = 'Accounts'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Accounts__DD70126441B923B5'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    number = Column(BigInteger)
    tax_rate = Column(Float(53))


class ChargeItems(Base):
    __tablename__ = 'ChargeItems'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__ChargeIt__DD7012641FCC1C26'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    pre_tax_price = Column(Float(53))
    tax_rate = Column(Float(53))
    post_tax_price = Column(Float(53))
    includes_tax = Column(Boolean)
    account_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


class Charges(Base):
    __tablename__ = 'Charges'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Charges__DD701264C0BAC2F8'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    kind = Column(BigInteger)
    sub_kind = Column(BigInteger)
    item = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    charge_date = Column(DateTime)
    charge_time = Column(DateTime)
    posting_date = Column(DateTime)
    quantity = Column(BigInteger)
    pre_tax_price = Column(Float(53))
    post_tax_price = Column(Float(53))
    total_amount = Column(Float(53))
    tax1_rate = Column(Float(53))
    tax2_rate = Column(Float(53))
    total_tax1 = Column(Float(53))
    total_tax2 = Column(Float(53))
    includes_tax = Column(Boolean)
    discount = Column(Float(53))
    customer_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    reservation_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    unit_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    account_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    package_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


class Classes(Base):
    __tablename__ = 'Classes'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Classes__DD701264B976AA82'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


class Customers(Base):
    __tablename__ = 'Customers'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Customer__DD7012648DC939B4'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    international = Column(Boolean)
    relationship = Column(BigInteger)
    company = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    addr1 = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    city = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    state = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    zip = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    country = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    addr2 = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    home_phone = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    work_phone = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    alt_phone = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    email = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    notes = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    first_contact = Column(DateTime)
    last_contact = Column(DateTime)
    source = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    name_first = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    name_last = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


class PackageUnits(Base):
    __tablename__ = 'PackageUnits'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__PackageU__DD701264ED75F9B5'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    package_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    unit_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


class Packages(Base):
    __tablename__ = 'Packages'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Packages__DD701264B9F574B2'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    description = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    rate = Column(Float(53))
    account_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


class Reservations(Base):
    __tablename__ = 'Reservations'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Reservat__DD70126439EE7832'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    cancel_date = Column(DateTime)
    status = Column(BigInteger)
    date_made = Column(DateTime)
    arrival_date = Column(DateTime)
    departure_date = Column(DateTime)
    num_persons = Column(BigInteger)
    deposit_amount = Column(Float(53))
    deposit_date = Column(DateTime)
    notes = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    requests = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    unit_assignments = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    customer_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


class Units(Base):
    __tablename__ = 'Units'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Units__DD7012649A0D159A'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    abbr = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    max_persons = Column(BigInteger)
    active = Column(Boolean, server_default=text("('1')"))
    class_id = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
