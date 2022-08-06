from sqlalchemy import (
    DECIMAL, BigInteger, Boolean, Column, Date, Float, ForeignKeyConstraint,
    Identity, Index, Integer, PrimaryKeyConstraint, SmallInteger, String,
    Table, Unicode, text)
from sqlalchemy.dialects.mssql import (
    DATETIME2, MONEY, SMALLDATETIME, TIMESTAMP)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()
metadata = Base.metadata


t_Comp_Latest_CO = Table(
    'Comp Latest CO', metadata,
    Column('Unit', Unicode(255)),
    Column('Floc', Unicode(255)),
    Column('MaxUnitSMR', Integer),
    Column('MaxDateAdded', DATETIME2)
)


t_Component_CO_to_check_SMR = Table(
    'Component CO to check SMR', metadata,
    Column('Unit', Unicode(255)),
    Column('Title', Unicode(255)),
    Column('DateAdded', DATETIME2),
    Column('Floc', Unicode(255)),
    Column('SMR', Integer),
    Column('ComponentSMR', Integer)
)


t_ComponentBench = Table(
    'ComponentBench', metadata,
    Column('floc', Unicode(255), nullable=False),
    Column('bench_smr', Integer),
    Column('equip_class', String(50, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('model_base', String(10, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('model', String(20, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('minesite', String(50, 'SQL_Latin1_General_CP1_CI_AS'))
)


class ComponentLookAhead(Base):
    __tablename__ = 'ComponentLookAhead'
    __table_args__ = (
        PrimaryKeyConstraint('SuncorWO', name='ComponentLookAhead$SuncorWO'),
    )

    Week = Column(Integer, nullable=False, index=True)
    SuncorWO = Column(Integer)
    SSMA_TimeStamp = Column(TIMESTAMP, nullable=False)
    Status = Column(Unicode(255))
    StartDate = Column(DATETIME2)
    SMSWO = Column(Unicode(255))
    MainWC = Column(Unicode(255))
    Description = Column(Unicode(255))
    FLOC = Column(Unicode(255))
    DueDate = Column(DATETIME2)
    WOType = Column(Unicode(255))
    DueWeek = Column(Integer)
    Category = Column(Unicode(255))
    Old = Column(Boolean, server_default=text('((0))'))


class ComponentType(Base):
    __tablename__ = 'ComponentType'
    __table_args__ = (
        PrimaryKeyConstraint('EquipClass', 'Floc', name='PK_EquipClass_Floc'),
        Index('ComponentType$ComponentModifier', 'Component', 'Modifier')
    )

    Floc = Column(Unicode(255), nullable=False)
    EquipClass = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False, index=True)
    Component = Column(Unicode(255), index=True)
    Modifier = Column(Unicode(255))
    BenchSMR = Column(Integer, server_default=text('((0))'))
    Major = Column(Boolean, server_default=text('((0))'))
    component_id = Column(String(255, 'SQL_Latin1_General_CP1_CI_AS'))
    component_location = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))


class Downtime(Base):
    __tablename__ = 'Downtime'
    __table_args__ = (
        PrimaryKeyConstraint('Unit', 'StartDate', 'EndDate', name='PK_Unit_StartDate_EndDate'),
    )

    Unit = Column(Unicode(255), nullable=False)
    StartDate = Column(DATETIME2, nullable=False)
    EndDate = Column(DATETIME2, nullable=False)
    CategoryAssigned = Column(Unicode(255))
    DownReason = Column(Unicode(255))
    Comment = Column(String(8000, 'SQL_Latin1_General_CP1_CI_AS'))
    Duration = Column(DECIMAL(10, 2))
    Responsible = Column(Unicode(255))
    SMS = Column(DECIMAL(10, 2))
    Suncor = Column(DECIMAL(10, 2))
    ShiftDate = Column(Date)
    Origin = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))


class DowntimeExclusions(Base):
    __tablename__ = 'DowntimeExclusions'
    __table_args__ = (
        PrimaryKeyConstraint('Unit', 'Date', name='PK_Unit_Date'),
    )

    Unit = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Date_ = Column('Date', Date, nullable=False)
    Hours = Column(Float(53), nullable=False)
    MA = Column(Boolean, nullable=False, server_default=text('((1))'))


class EmailList(Base):
    __tablename__ = 'EmailList'
    __table_args__ = (
        PrimaryKeyConstraint('UserGroup', 'MineSite', 'Email', name='PK_UserGroup_MineSite_Email'),
    )

    Email = Column(Unicode(255), nullable=False)
    MineSite = Column(Unicode(255), nullable=False)
    UserGroup = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False, server_default=text("('SMS')"))
    Passover = Column(Unicode(255))
    WORequest = Column(Unicode(255))
    FCCancelled = Column(Unicode(255))
    PicsDLS = Column(Unicode(255))
    PRP = Column(Unicode(255))
    FCSummary = Column(Unicode(255))
    TSI = Column(Unicode(255))
    RAMP = Column(Unicode(255))
    Service = Column(Unicode(255))
    Parts = Column(Unicode(255))
    AvailDaily = Column(String(10, 'SQL_Latin1_General_CP1_CI_AS'))
    AvailReports = Column(String(10, 'SQL_Latin1_General_CP1_CI_AS'))
    FleetReport = Column(String(10, 'SQL_Latin1_General_CP1_CI_AS'))
    SMRReport = Column(String(10, 'SQL_Latin1_General_CP1_CI_AS'))


class EquipType(Base):
    __tablename__ = 'EquipType'
    __table_args__ = (
        PrimaryKeyConstraint('Model', name='PK__EquipTyp__FB104C122BAEC192'),
    )

    Model = Column(String(255, 'SQL_Latin1_General_CP1_CI_AS'))
    EquipClass = Column(String(255, 'SQL_Latin1_General_CP1_CI_AS'))
    ModelBase = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), index=True)
    TargetPayload = Column(Float(53))


t_Errors = Table(
    'Errors', metadata,
    Column('UserName', String(255, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('ErrTime', DATETIME2),
    Column('ErrNum', Integer),
    Column('ErrDescrip', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('Sub', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('Version', Float(53)),
    Column('WorkSheet', Integer)
)


class EventLog(Base):
    __tablename__ = 'EventLog'
    __table_args__ = (
        PrimaryKeyConstraint('UID', name='EventLog$PrimaryKey'),
        Index('EventLog$MineSite-StatusEvent', 'MineSite', 'PassoverSort', 'StatusEvent'),
        Index('EventLog$MineSite-StatusWO', 'MineSite', 'WarrantyYN', 'StatusWO'),
        Index('EventLog$SuncorWOFloc', 'SuncorWO', 'Floc'),
        Index('nci_wi_EventLog_CBCBDC05D6F073546FA6219DAB22A491', 'CreatedBy', 'StatusWO'),
        Index('unit_compco', 'Unit', 'ComponentCO')
    )

    UID = Column(Float(53), server_default=text('((0))'))
    MineSite = Column(Unicode(255))
    PassoverSort = Column(Unicode(255))
    StatusEvent = Column(Unicode(255))
    StatusWO = Column(Unicode(255))
    CreatedBy = Column(Unicode(255))
    ClosedBy = Column(Unicode(255))
    Unit = Column(Unicode(255), index=True)
    Title = Column(Unicode(255))
    Description = Column(String(8000, 'SQL_Latin1_General_CP1_CI_AS'))
    Required = Column(String(8000, 'SQL_Latin1_General_CP1_CI_AS'))
    SMR = Column(Integer)
    DateAdded = Column(Date)
    DateCompleted = Column(Date)
    TimeCalled = Column(DATETIME2)
    IssueCategory = Column(Unicode(255))
    SubCategory = Column(Unicode(255))
    Cause = Column(Unicode(255))
    WarrantyYN = Column(Unicode(255))
    WorkOrder = Column(Unicode(255), index=True)
    Seg = Column(Unicode(255))
    PartNumber = Column(Unicode(255))
    SuncorWO = Column(Unicode(255))
    SuncorPO = Column(Unicode(255))
    Downloads = Column(Boolean, server_default=text('((0))'))
    Pictures = Column(SmallInteger, server_default=text('((0))'))
    CCOS = Column(Unicode(255))
    WOComments = Column(String(8000, 'SQL_Latin1_General_CP1_CI_AS'))
    StatusTSI = Column(Unicode(255), index=True)
    DateInfo = Column(Date)
    DateTSISubmission = Column(Date)
    TSINumber = Column(Unicode(255))
    TSIPartName = Column(Unicode(255))
    TSIDetails = Column(String(8000, 'SQL_Latin1_General_CP1_CI_AS'))
    TSIPartNo = Column(Unicode(255))
    TSIAuthor = Column(Unicode(255))
    FilePath = Column(String(8000, 'SQL_Latin1_General_CP1_CI_AS'))
    ComponentCO = Column(Boolean, index=True, server_default=text('((0))'))
    Floc = Column(Unicode(255))
    GroupCO = Column(Boolean, server_default=text('((0))'))
    ComponentSMR = Column(Integer)
    SNRemoved = Column(Unicode(255))
    SNInstalled = Column(Unicode(255))
    CapUSD = Column(MONEY)
    RemovalReason = Column(Unicode(255))
    COConfirmed = Column(Boolean, server_default=text('((0))'))
    DateReturned = Column(Date)
    SunCOReason = Column(String(255, 'SQL_Latin1_General_CP1_CI_AS'))
    Reman = Column(Boolean)
    FailureCause = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    OrderParts = Column(String(1000, 'SQL_Latin1_General_CP1_CI_AS'))
    KAPhenomenon = Column(String(60, 'SQL_Latin1_General_CP1_CI_AS'))
    KAComponentGroup = Column(String(60, 'SQL_Latin1_General_CP1_CI_AS'))
    install_smr = Column(Integer)
    deleted = Column(Boolean, server_default=text('((0))'))

    FactoryCampaign = relationship('FactoryCampaign', back_populates='EventLog_')


class FCSummary(Base):
    __tablename__ = 'FCSummary'
    __table_args__ = (
        PrimaryKeyConstraint('FCNumber', name='FCSummary$PrimaryKey'),
    )

    FCNumber = Column(Unicode(255))
    Subject = Column(Unicode(255))
    SubjectShort = Column(Unicode(255))
    Classification = Column(Unicode(255))
    NotCustomerFriendly = Column(Boolean, server_default=text('((0))'))
    Hours = Column(Float(53))
    DowntimeEst = Column(Float(53), server_default=text('((0))'))
    PartNumber = Column(Unicode(255))
    CustomSort = Column(Integer)
    ReleaseDate = Column(Date)
    ExpiryDate = Column(Date)


class FCSummaryMineSite(Base):
    __tablename__ = 'FCSummaryMineSite'
    __table_args__ = (
        PrimaryKeyConstraint('FCNumber', 'MineSite', name='FCSummaryMineSite$PrimaryKey'),
    )

    FCNumber = Column(Unicode(255), nullable=False, index=True)
    MineSite = Column(Unicode(255), nullable=False)
    Resp = Column(Unicode(255))
    Comments = Column(String(8000, 'SQL_Latin1_General_CP1_CI_AS'))
    ManualClosed = Column(Boolean, server_default=text('((0))'))
    PartAvailability = Column(Unicode(255))


t_FactoryCampaignImport = Table(
    'FactoryCampaignImport', metadata,
    Column('FCNumber', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('Model', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('Serial', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('Unit', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('StartDate', Date),
    Column('EndDate', Date),
    Column('DateCompleteKA', Date),
    Column('Subject', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('Classification', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('Branch', Float(53)),
    Column('Status', String(collation='SQL_Latin1_General_CP1_CI_AS'))
)


class FaultCodes(Base):
    __tablename__ = 'FaultCodes'
    __table_args__ = (
        PrimaryKeyConstraint('Description', 'Code', name='FaultCodes$PrimaryKey'),
    )

    Description = Column(Unicode(255), nullable=False)
    Code = Column(Unicode(255), nullable=False)
    Type = Column(Unicode(255))


class Faults(Base):
    __tablename__ = 'Faults'
    __table_args__ = (
        PrimaryKeyConstraint('Unit', 'Code', 'Time_From', name='PK_Unit_Code_Timefrom'),
        Index('IX_Code_Timefrom', 'Code', 'Time_From')
    )

    Unit = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Code = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Time_From = Column(DATETIME2, nullable=False, index=True)
    Time_To = Column(DATETIME2)
    FaultCount = Column(Integer)
    Message = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


t_Find_duplicates_for_EventLog = Table(
    'Find duplicates for EventLog', metadata,
    Column('Unit', Unicode(255)),
    Column('DateAdded', DATETIME2),
    Column('Floc', Unicode(255)),
    Column('UID', Float(53), nullable=False),
    Column('MineSite', Unicode(255)),
    Column('Title', Unicode(255)),
    Column('DateCompleted', DATETIME2),
    Column('SuncorWO', Unicode(255)),
    Column('SuncorPO', Unicode(255)),
    Column('ComponentCO', Boolean),
    Column('SNRemoved', Unicode(255)),
    Column('SNInstalled', Unicode(255)),
    Column('RemovalReason', Unicode(255))
)


t_MAGuarantee = Table(
    'MAGuarantee', metadata,
    Column('MaxAge', Integer),
    Column('MA', DECIMAL(4, 3)),
    Column('MAExisting', DECIMAL(4, 3))
)


class MineSite(Base):
    __tablename__ = 'MineSite'
    __table_args__ = (
        PrimaryKeyConstraint('MineSite', 'Model', name='PK_minesite_model'),
    )

    MineSite = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Model = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    TargetPayload = Column(Float(53))


class OilSamples(Base):
    __tablename__ = 'OilSamples'
    __table_args__ = (
        PrimaryKeyConstraint('hist_no', name='PK_hist_no'),
        Index('IX_oilsamples_unit_comp_modifier', 'unit', 'component_id', 'modifier')
    )

    hist_no = Column(BigInteger)
    unit = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    sample_date = Column(Date, index=True)
    process_date = Column(Date)
    component_id = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    component_type = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    modifier = Column(String(30, 'SQL_Latin1_General_CP1_CI_AS'))
    unit_smr = Column(Integer)
    component_smr = Column(Integer)
    sample_rank = Column(Float(53))
    oil_changed = Column(Boolean)
    test_results = Column(String(2000, 'SQL_Latin1_General_CP1_CI_AS'))
    test_flags = Column(String(1000, 'SQL_Latin1_General_CP1_CI_AS'))
    results = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    recommendations = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    comments = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))


class PLM(Base):
    __tablename__ = 'PLM'
    __table_args__ = (
        PrimaryKeyConstraint('Unit', 'DateTime', name='PK_Unit_DateTime'),
    )

    Unit = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    DateTime = Column(DATETIME2, nullable=False)
    Payload = Column(Float(53), nullable=False)
    Swingloads = Column(BigInteger)
    StatusFlag = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    Carryback = Column(Float(53))
    CycleTime = Column(Float(53))
    L_HaulDistance = Column(Float(53))
    L_MaxSpeed = Column(Float(53))
    E_MaxSpeed = Column(Float(53))
    MaxSprung = Column(Float(53))
    TruckType = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    SprungWeight = Column(Float(53))
    Payload_Est = Column(Float(53))
    Payload_Quick = Column(Float(53))
    Payload_Gross = Column(Float(53))


t_PSNList = Table(
    'PSNList', metadata,
    Column('Reference Number', Unicode(255)),
    Column('Comp Code', Unicode(255), index=True),
    Column('Subject', Unicode(255)),
    Column('Model(s)', Unicode(255)),
    Column('Product Line', Unicode(255)),
    Column('Issue Date', DATETIME2),
    Column('Customer friendly?', Unicode(255)),
    Index('PSNList$Comp Code', 'Comp Code')
)


class Parts(Base):
    __tablename__ = 'Parts'
    __table_args__ = (
        PrimaryKeyConstraint('Model', 'PartNo', name='Parts$PrimaryKey'),
    )

    PartNo = Column(Unicode(255), nullable=False, index=True)
    Model = Column(Unicode(255), nullable=False, index=True)
    PartName = Column(Unicode(255), index=True)
    PartNameAlt = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'))


class PicsDLS(Base):
    __tablename__ = 'PicsDLS'
    __table_args__ = (
        PrimaryKeyConstraint('FolderName', 'Type', name='PicsDLS$PrimaryKey'),
    )

    FolderName = Column(Unicode(255), nullable=False)
    Type = Column(Unicode(255), nullable=False)
    Unit = Column(Unicode(255), index=True)
    DateAdded = Column(DATETIME2)
    SortDate = Column(DATETIME2)
    FaultLinesAdded = Column(Integer)
    PLMLinesAdded = Column(Integer)
    FilePath = Column(Unicode(255))


t_SunBench = Table(
    'SunBench', metadata,
    Column('ID', Integer, Identity(start=1, increment=1), nullable=False),
    Column('Unit', Unicode(255)),
    Column('MineSite', Unicode(255)),
    Column('Component', Unicode(255)),
    Column('Modifier', Unicode(255)),
    Column('WorkOrder', Unicode(255)),
    Column('SuncorWO', Unicode(255)),
    Column('SMR', Integer),
    Column('ComponentSMR', Integer),
    Column('Part_Num', Unicode(255)),
    Column('SNRemoved', Unicode(255)),
    Column('DateAdded', Date),
    Column('CO_Mode', Unicode(255)),
    Column('Reason', Unicode(255)),
    Column('PO', Unicode(20)),
    Column('Notes', Unicode(255)),
    Column('Convert_Indicator', Integer),
    Column('CapUSD', MONEY),
    Column('PercentBench', Float(53)),
    Column('Warranty', Unicode(255)),
    Column('SUN_CO_Reason', Unicode(255)),
    Column('Group_CO', Boolean),
    Column('TSI', Unicode(25)),
    Column('Warranty_Hours', Integer),
    Column('UID', Float(53)),
    Column('Transaction_Hour', Integer),
    Column('Floc', Unicode(255))
)


class TSIValues(Base):
    __tablename__ = 'TSIValues'
    __table_args__ = (
        PrimaryKeyConstraint('Element', name='TSIValues$Element'),
    )

    Element = Column(Unicode(255))
    Type = Column(Unicode(255))
    Active = Column(Unicode(255))
    DefaultVal = Column(Unicode(255))


class TechLogActions(Base):
    __tablename__ = 'TechLogActions'
    __table_args__ = (
        PrimaryKeyConstraint('UID', name='PK_TechLogActions3'),
    )

    UID = Column(Float(53))
    UIDParent = Column(Float(53), nullable=False)
    Added_By = Column(Unicode(50), nullable=False)
    Issue_Title = Column(Unicode(50))
    Action = Column(Unicode)
    Responsible = Column(Unicode(50))
    Date_Added = Column(Date)
    Date_Complete = Column(Date)
    Status = Column(Unicode(50))


class TechLogSummary(Base):
    __tablename__ = 'TechLogSummary'
    __table_args__ = (
        PrimaryKeyConstraint('UID', name='PK_Summary2'),
    )

    UID = Column(Float(53))
    Status = Column(Unicode(50))
    Title = Column(Unicode(1000))
    Risk_Rank = Column(Unicode(50))
    Model = Column(Unicode(50))
    Owner = Column(Unicode(50))
    Description = Column(Unicode(1000))
    Date_Added = Column(Date)
    Date_Complete = Column(Date)
    Resolution = Column(Unicode(1000))
    Receptor = Column(Unicode(50))
    EquipType_ = Column('EquipType', String(255, 'SQL_Latin1_General_CP1_CI_AS'))


class TechLogUsers(Base):
    __tablename__ = 'TechLogUsers'
    __table_args__ = (
        PrimaryKeyConstraint('UserName', name='PK_TechLogUsers'),
    )

    UserName = Column(Unicode(50))
    Email = Column(Unicode(50))
    Company = Column(Unicode(50))


class UnitID(Base):
    __tablename__ = 'UnitID'
    __table_args__ = (
        PrimaryKeyConstraint('Unit', name='UnitID$Unit'),
        Index('UnitID$ModelSerial', 'Model', 'Serial')
    )

    Model = Column(Unicode(50), nullable=False)
    Serial = Column(Unicode(50), nullable=False, index=True)
    Unit = Column(Unicode(255))
    MineSite_ = Column('MineSite', Unicode(255), index=True)
    EngineSerial = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    DeliveryDate = Column(Date)
    VerPLM = Column(SmallInteger)
    SubSite = Column(String(255, 'SQL_Latin1_General_CP1_CI_AS'))
    Active = Column(Boolean)
    ExcludeMA = Column(Boolean)
    OffContract = Column(Boolean)
    DateOffContract = Column(Date)
    AHSActive2 = Column(Boolean, server_default=text('((0))'))
    AHSActive = Column(Boolean, server_default=text('((0))'))
    Customer = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    AHSStart = Column(Date)
    TargetPayload = Column(Float(53))
    Notes = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    is_component = Column(Boolean, server_default=text('((0))'))


class UnitSMR(Base):
    __tablename__ = 'UnitSMR'
    __table_args__ = (
        PrimaryKeyConstraint('Unit', 'DateSMR', name='Unit_DateSMR'),
    )

    Unit = Column(Unicode(255), nullable=False, index=True)
    DateSMR = Column(Date, nullable=False)
    SMR = Column(Integer)


class UserSettings(Base):
    __tablename__ = 'UserSettings'
    __table_args__ = (
        PrimaryKeyConstraint('UserName', name='UserSettings$PrimaryKey'),
    )

    UserName = Column(Unicode(255))
    LastLogin = Column(DATETIME2)
    NumOpens = Column(Integer, index=True, server_default=text('((0))'))
    Email = Column(String(255, 'SQL_Latin1_General_CP1_CI_AS'))
    Domain = Column(String(255, 'SQL_Latin1_General_CP1_CI_AS'))
    Ver = Column(String(10, 'SQL_Latin1_General_CP1_CI_AS'))
    UserGroup = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    MineSite_ = Column('MineSite', String(100, 'SQL_Latin1_General_CP1_CI_AS'))
    odbc_driver = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'))
    install_dir = Column(String(300, 'SQL_Latin1_General_CP1_CI_AS'))


class Users(Base):
    __tablename__ = 'Users'
    __table_args__ = (
        PrimaryKeyConstraint('UserName', name='Users$UserName'),
    )

    UserName = Column(Unicode(255))
    Title = Column(Unicode(255))
    Manager = Column(Unicode(255))


class Credentials(Base):
    __tablename__ = 'credentials'
    __table_args__ = (
        PrimaryKeyConstraint('name', 'id', name='PK_id'),
    )

    name = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    id = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    password = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)


t_temp_import = Table(
    'temp_import', metadata,
    Column('Unit', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('SMR', BigInteger),
    Column('DateSMR', Date)
)


t_viewComponentCO = Table(
    'viewComponentCO', metadata,
    Column('MineSite', Unicode(255)),
    Column('Unit', Unicode(255)),
    Column('ModelBase', String(50, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('Component', Unicode(255)),
    Column('DateAdded', SMALLDATETIME),
    Column('Reman', String(5, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False),
    Column('SunCOReason', String(255, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('ComponentSMR', Integer),
    Column('PercentBench', Float(53))
)


t_viewEventLog = Table(
    'viewEventLog', metadata,
    Column('UID', Float(53), nullable=False),
    Column('MineSite', Unicode(255)),
    Column('PassoverSort', Unicode(255)),
    Column('StatusEvent', Unicode(255)),
    Column('StatusWO', Unicode(255)),
    Column('CreatedBy', Unicode(255)),
    Column('ClosedBy', Unicode(255)),
    Column('Unit', Unicode(255)),
    Column('Title', Unicode(255)),
    Column('Description', String(8000, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('Required', String(8000, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('SMR', Integer),
    Column('DateAdded', Date),
    Column('DateCompleted', Date),
    Column('TimeCalled', DATETIME2),
    Column('IssueCategory', Unicode(255)),
    Column('SubCategory', Unicode(255)),
    Column('Cause', Unicode(255)),
    Column('WarrantyYN', Unicode(255)),
    Column('WorkOrder', Unicode(255)),
    Column('Seg', Unicode(255)),
    Column('PartNumber', Unicode(255)),
    Column('SuncorWO', Unicode(255)),
    Column('SuncorPO', Unicode(255)),
    Column('Downloads', Boolean),
    Column('Pictures', SmallInteger),
    Column('CCOS', Unicode(255)),
    Column('WOComments', String(8000, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('StatusTSI', Unicode(255)),
    Column('DateInfo', Date),
    Column('DateTSISubmission', Date),
    Column('TSINumber', Unicode(255)),
    Column('TSIPartName', Unicode(255)),
    Column('TSIDetails', String(8000, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('TSIPartNo', Unicode(255)),
    Column('TSIAuthor', Unicode(255)),
    Column('FilePath', String(8000, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('ComponentCO', Boolean),
    Column('Floc', Unicode(255)),
    Column('GroupCO', Boolean),
    Column('ComponentSMR', Integer),
    Column('SNRemoved', Unicode(255)),
    Column('SNInstalled', Unicode(255)),
    Column('CapUSD', MONEY),
    Column('RemovalReason', Unicode(255)),
    Column('COConfirmed', Boolean),
    Column('DateReturned', Date),
    Column('SunCOReason', String(255, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('Reman', Boolean),
    Column('FailureCause', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('Model', Unicode(50)),
    Column('Customer', String(50, 'SQL_Latin1_General_CP1_CI_AS'))
)


t_viewFactoryCampaign = Table(
    'viewFactoryCampaign', metadata,
    Column('UID', Float(53)),
    Column('Unit', Unicode(255), nullable=False),
    Column('FCNumber', Unicode(255), nullable=False),
    Column('Serial', Unicode(255)),
    Column('Model', Unicode(255)),
    Column('Complete', Boolean),
    Column('Ignore', Boolean, nullable=False),
    Column('Classification', Unicode(255)),
    Column('Hours', Float(53)),
    Column('Subject', Unicode(255)),
    Column('DateCompleteSMS', Date),
    Column('DateCompleteKA', Date),
    Column('SMR', Integer),
    Column('ReleaseDate', Date),
    Column('ExpiryDate', Date),
    Column('Pictures', SmallInteger),
    Column('Notes', Unicode(255)),
    Column('Scheduled', Boolean, nullable=False),
    Column('MinDateComplete', Date)
)


t_viewMaxSMR = Table(
    'viewMaxSMR', metadata,
    Column('Unit', Unicode(255), nullable=False),
    Column('CurrentUnitSMR', Integer)
)


t_viewOilSamples = Table(
    'viewOilSamples', metadata,
    Column('labTrackingNo', String(100, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False),
    Column('Unit', String(100, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('Component', Unicode(255)),
    Column('Modifier', Unicode(255)),
    Column('sampleDate', Date),
    Column('unitSMR', Float(53)),
    Column('componentSMR', Float(53)),
    Column('oilChanged', Boolean),
    Column('sampleRank', Float(53)),
    Column('testResults', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('results', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('recommendations', String(collation='SQL_Latin1_General_CP1_CI_AS')),
    Column('comments', String(collation='SQL_Latin1_General_CP1_CI_AS'))
)


t_viewPAMonthly = Table(
    'viewPAMonthly', metadata,
    Column('MonthStart', Date),
    Column('MonthEnd', Date),
    Column('Year', Integer),
    Column('Month', Integer),
    Column('Unit', Unicode(255), nullable=False),
    Column('Sum_DT', DECIMAL(38, 2)),
    Column('Hrs_Period', Integer),
    Column('PA', DECIMAL(38, 6))
)


t_viewPLM = Table(
    'viewPLM', metadata,
    Column('Unit', String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False),
    Column('DateTime', DATETIME2, nullable=False),
    Column('TargetPayload', Float(53)),
    Column('Payload_Net', Float(53), nullable=False),
    Column('Payload_Gross', Float(53)),
    Column('Payload_Quick_Carry', Float(53)),
    Column('StatusFlag', String(50, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('L_HaulDistance', Float(53)),
    Column('GrossPayload_pct', Float(53)),
    Column('QuickPayload_pct', Float(53)),
    Column('QuickShovelEst_pct', Float(53)),
    Column('ExcludeFlags', Integer, nullable=False)
)


t_viewPayload = Table(
    'viewPayload', metadata,
    Column('MineSite', Unicode(255)),
    Column('Unit', Unicode(255), nullable=False),
    Column('TargetPayload', Float(53))
)


t_viewPredictedCO = Table(
    'viewPredictedCO', metadata,
    Column('MineSite', Unicode(255)),
    Column('ModelBase', String(50, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('Model', Unicode(50), nullable=False),
    Column('Unit', Unicode(255), nullable=False),
    Column('Component', Unicode(255)),
    Column('Modifier', Unicode(255)),
    Column('bench_smr', Integer),
    Column('CurrentUnitSMR', Integer),
    Column('SMRLastCO', Integer),
    Column('CurrentComponentSMR', Integer),
    Column('PredictedCODate', Date),
    Column('Floc', Unicode(255)),
    Column('Major', Boolean),
    Column('SNInstalled', Unicode(255)),
    Column('LifeRemaining', Integer)
)


t_viewUnitEquip = Table(
    'viewUnitEquip', metadata,
    Column('EquipClass', String(255, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('MineSite', Unicode(255)),
    Column('Customer', String(50, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('ModelBase', String(50, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('Model', Unicode(50), nullable=False),
    Column('Unit', Unicode(255), nullable=False),
    Column('Serial', Unicode(50), nullable=False),
    Column('Active', Boolean)
)


class TMPCLP446861(Base):
    __tablename__ = '~TMPCLP446861'
    __table_args__ = (
        PrimaryKeyConstraint('Unit', 'Floc', 'DateCO', name='~TMPCLP446861$UnitFlocDate'),
    )

    Unit = Column(Unicode(255), nullable=False)
    Floc = Column(Unicode(255), nullable=False, index=True)
    DateCO = Column(DATETIME2, nullable=False)
    SSMA_TimeStamp = Column(TIMESTAMP, nullable=False)
    DateCOCorrected = Column(DATETIME2)
    WO_SMS = Column(Unicode(255))
    WO_Customer = Column(Unicode(255))
    PO_Customer = Column(Unicode(255))
    UnitSMR_ = Column('UnitSMR', Integer)
    SMR = Column(Integer)
    SNRemoved = Column(Unicode(255))
    SNInstalled = Column(Unicode(255))
    Warranty = Column(Unicode(255))
    CapUSD = Column(MONEY)
    Notes = Column(Unicode(255))
    Confirmed = Column(Boolean, server_default=text('((0))'))


t__TMPCLP503061 = Table(
    '~TMPCLP503061', metadata,
    Column('F1', Unicode(255)),
    Column('F2', DATETIME2),
    Column('F3', Float(53)),
    Column('SSMA_TimeStamp', TIMESTAMP, nullable=False)
)


class FactoryCampaign(Base):
    __tablename__ = 'FactoryCampaign'
    __table_args__ = (
        ForeignKeyConstraint(['UID'], ['EventLog.UID'], onupdate='CASCADE',
                             name='FactoryCampaign$EventLogFactoryCampaign'),
        PrimaryKeyConstraint('FCNumber', 'Unit', name='FactoryCampaign$PrimaryKey')
    )

    Unit = Column(Unicode(255), nullable=False)
    FCNumber = Column(Unicode(255), nullable=False, index=True)
    Ignore = Column(Boolean, nullable=False, server_default=text('((0))'))
    Scheduled = Column(Boolean, nullable=False, server_default=text('((0))'))
    UID = Column(Float(53), index=True)
    Serial = Column(Unicode(255), index=True)
    Status = Column(Unicode(243))
    Classification = Column(Unicode(255))
    Subject = Column(Unicode(255))
    Model = Column(Unicode(255))
    Distributor = Column(Float(53))
    Branch = Column(Float(53))
    DateCompleteSMS = Column(Date)
    DateCompleteKA = Column(Date)
    Safety = Column(Unicode(255))
    StartDate = Column(Date)
    EndDate = Column(Date)
    ClaimNumber = Column(Unicode(255))
    Technician = Column(Unicode(255))
    Hours = Column(Float(53))
    Notes = Column(Unicode(255))
    CustomSort = Column(Integer)

    EventLog_ = relationship('EventLog', back_populates='FactoryCampaign')
