from sqlalchemy import (
    BigInteger, Boolean, Column, Float, ForeignKeyConstraint,
    PrimaryKeyConstraint, String)
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Accounts(Base):
    __tablename__ = 'Accounts'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Accounts__DD70126436FC5DC6'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    number = Column(BigInteger)
    tax_rate = Column(Float(53))

    ChargeItems = relationship('ChargeItems', back_populates='account')
    Packages = relationship('Packages', back_populates='account')
    Charges = relationship('Charges', back_populates='account')


class Classes(Base):
    __tablename__ = 'Classes'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Classes__DD701264EB115946'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))

    Units = relationship('Units', back_populates='class_')


class Customers(Base):
    __tablename__ = 'Customers'
    __table_args__ = (
        PrimaryKeyConstraint('uid', name='PK__Customer__DD701264B30D394D'),
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    international = Column(Boolean)
    relationship_ = Column('relationship', BigInteger)
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
    first_contact = Column(DATETIME2)
    last_contact = Column(DATETIME2)
    source = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    name_first = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    name_last = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))

    Reservations = relationship('Reservations', foreign_keys='[Reservations.customer_id]', back_populates='customer')
    Charges = relationship('Charges', back_populates='customer')


class ChargeItems(Base):
    __tablename__ = 'ChargeItems'
    __table_args__ = (
        ForeignKeyConstraint(['account_id'], ['Accounts.uid'], name='FK__ChargeIte__accou__2BFE89A6'),
        PrimaryKeyConstraint('uid', name='PK__ChargeIt__DD7012648B015C9E')
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    pre_tax_price = Column(Float(53))
    tax_rate = Column(Float(53))
    post_tax_price = Column(Float(53))
    includes_tax = Column(Boolean)
    account_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))

    account = relationship('Accounts', back_populates='ChargeItems')


class Packages(Base):
    __tablename__ = 'Packages'
    __table_args__ = (
        ForeignKeyConstraint(['account_id'], ['Accounts.uid'], name='FK__Packages__accoun__31B762FC'),
        PrimaryKeyConstraint('uid', name='PK__Packages__DD70126430F10859')
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    description = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    rate = Column(Float(53))
    account_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))

    account = relationship('Accounts', back_populates='Packages')
    Charges = relationship('Charges', back_populates='package')
    PackageUnits = relationship('PackageUnits', back_populates='package')


class Reservations(Base):
    __tablename__ = 'Reservations'
    __table_args__ = (
        ForeignKeyConstraint(['customer_id'], ['Customers.uid'], name='FK__Reservati__custo__3493CFA7'),
        PrimaryKeyConstraint('uid', name='PK__Reservat__DD701264EB9EA6DE')
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    cancel_date = Column(DATETIME2)
    status = Column(BigInteger)
    date_made = Column(DATETIME2)
    arrival_date = Column(DATETIME2)
    departure_date = Column(DATETIME2)
    num_persons = Column(BigInteger)
    deposit_amount = Column(Float(53))
    deposit_date = Column(DATETIME2)
    notes = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    requests = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    unit_assignments = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    customer_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))

    customer = relationship('Customers', foreign_keys=[customer_id], back_populates='Reservations')
    Charges = relationship('Charges', back_populates='reservation')


class Units(Base):
    __tablename__ = 'Units'
    __table_args__ = (
        ForeignKeyConstraint(['class_id'], ['Classes.uid'], name='FK__Units__class_id__3587F3E0'),
        PrimaryKeyConstraint('uid', name='PK__Units__DD70126478204927')
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    name = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    abbr = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    max_persons = Column(BigInteger)
    active = Column(Boolean)
    class_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))

    class_ = relationship('Classes', back_populates='Units')
    Charges = relationship('Charges', back_populates='unit')
    PackageUnits = relationship('PackageUnits', back_populates='unit')


class Charges(Base):
    __tablename__ = 'Charges'
    __table_args__ = (
        ForeignKeyConstraint(['account_id'], ['Accounts.uid'], name='FK__Charges__account__2FCF1A8A'),
        ForeignKeyConstraint(['customer_id'], ['Customers.uid'], name='FK__Charges__custome__2CF2ADDF'),
        ForeignKeyConstraint(['package_id'], ['Packages.uid'], name='FK__Charges__package__30C33EC3'),
        ForeignKeyConstraint(['reservation_id'], ['Reservations.uid'], name='FK__Charges__reserva__2DE6D218'),
        ForeignKeyConstraint(['unit_id'], ['Units.uid'], name='FK__Charges__unit_id__2EDAF651'),
        PrimaryKeyConstraint('uid', name='PK__Charges__DD701264AC0AA4BC')
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    kind = Column(BigInteger)
    sub_kind = Column(BigInteger)
    item = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    charge_date = Column(DATETIME2)
    posting_date = Column(DATETIME2)
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
    customer_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    reservation_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    unit_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    account_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    package_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))

    account = relationship('Accounts', back_populates='Charges')
    customer = relationship('Customers', back_populates='Charges')
    package = relationship('Packages', back_populates='Charges')
    reservation = relationship('Reservations', back_populates='Charges')
    unit = relationship('Units', back_populates='Charges')


class PackageUnits(Base):
    __tablename__ = 'PackageUnits'
    __table_args__ = (
        ForeignKeyConstraint(['package_id'], ['Packages.uid'], name='FK__PackageUn__packa__32AB8735'),
        ForeignKeyConstraint(['unit_id'], ['Units.uid'], name='FK__PackageUn__unit___339FAB6E'),
        PrimaryKeyConstraint('uid', name='PK__PackageU__DD7012649FEB152B')
    )

    uid = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    package_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))
    unit_id = Column(String(36, 'SQL_Latin1_General_CP1_CI_AS'))

    package = relationship('Packages', back_populates='PackageUnits')
    unit = relationship('Units', back_populates='PackageUnits')
