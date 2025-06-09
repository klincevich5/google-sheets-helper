# database/db_models.py

from sqlalchemy import Column, Integer, String, Text, Boolean, Date, Time, DateTime, UniqueConstraint, Enum, BigInteger, ForeignKey
import enum
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB, ARRAY as PG_ARRAY

Base = declarative_base()

class MonitoringStorage(Base):
    __tablename__ = "MonitoringStorage"

    id = Column(Integer, primary_key=True)
    dealer_name = Column(String, nullable=False)
    dealer_nicknames = Column(PG_ARRAY(String), default=[])
    report_date = Column(Date, nullable=False)

    shift_type = Column(String)
    shift_start = Column(Time)
    shift_end = Column(Time)

    break_number = Column(Integer, default=0)

    is_scheduled = Column(Boolean, default=False)
    is_additional = Column(Boolean, default=False)
    is_extra = Column(Boolean, default=False)
    is_sickleave = Column(Boolean, default=False)
    is_vacation = Column(Boolean, default=False)
    is_did_not_come = Column(Boolean, default=False)
    is_left_the_shift = Column(Boolean, default=False)

    assigned_floors = Column(PG_ARRAY(String), default=[])
    floor_permits = Column(JSONB, default={})
    game_permits = Column(JSONB, default={})

    has_mistakes = Column(Boolean, default=False)
    has_feedbacks = Column(Boolean, default=False)

    rotation = Column(JSONB, default=[])
    mistakes = Column(JSONB, default=[])
    feedbacks = Column(JSONB, default=[])
    raw_data = Column(JSONB, default={})

    __table_args__ = (
        UniqueConstraint('dealer_name', 'report_date', 'shift_type', name='uq_dealer_date_shift'),
    )

class BotSettings(Base):
    __tablename__ = 'BotSettings'

    key = Column(Text, primary_key=True)
    value = Column(Text)


class FeedbackStorage(Base):
    __tablename__ = 'FeedbackStorage'

    id = Column(Integer, primary_key=True)
    related_month = Column(Date)
    related_date = Column(Date)
    related_shift = Column(Text)
    floor = Column(Text)
    game = Column(Text)
    dealer_name = Column(Text)
    sm_name = Column(Text)
    reason = Column(Text)
    total = Column(Integer)
    proof = Column(Text)
    explanation_of_the_reason = Column(Text)
    action_taken = Column(Text)
    forwarded_feedback = Column(Text)
    comment_after_forwarding = Column(Text)


class MistakeStorage(Base):
    __tablename__ = 'MistakeStorage'

    id = Column(Integer, primary_key=True)
    related_month = Column(Date)
    floor = Column(Text)
    table_name = Column(Text)
    related_date = Column(Date)
    related_shift = Column(Text)
    event_time = Column(Time(timezone=True))
    game_id = Column(Text)
    mistake = Column(Text)
    mistake_type = Column(Text)
    is_cancel = Column(Integer)
    dealer_name = Column(Text)
    sm_name = Column(Text)
    last_row = Column(Integer)


class RotationsInfo(Base):
    __tablename__ = 'RotationsInfo'

    id = Column(Integer, primary_key=True)
    is_active = Column(Integer)
    related_month = Column(Date)
    name_of_process = Column(String)
    source_table_type = Column(String)
    source_page_name = Column(String)
    source_page_area = Column(String)
    scan_group = Column(String)
    last_scan = Column(DateTime(timezone=True))
    scan_interval = Column(Integer)
    scan_quantity = Column(Integer)
    scan_failures = Column(Integer)
    hash = Column(String)
    process_data_method = Column(String)
    values_json = Column(Text)
    target_table_type = Column(String)
    target_page_name = Column(String)
    target_page_area = Column(String)
    update_group = Column(String)
    last_update = Column(DateTime(timezone=True))
    update_quantity = Column(Integer)
    update_failures = Column(Integer)


class SheetsInfo(Base):
    __tablename__ = 'SheetsInfo'

    id = Column(Integer, primary_key=True)
    is_active = Column(Integer)
    related_month = Column(Date)
    name_of_process = Column(String)
    source_table_type = Column(String)
    source_page_name = Column(String)
    source_page_area = Column(String)
    scan_group = Column(String)
    last_scan = Column(DateTime(timezone=True))
    scan_interval = Column(Integer)
    scan_quantity = Column(Integer)
    scan_failures = Column(Integer)
    hash = Column(String)
    process_data_method = Column(String)
    values_json = Column(Text)
    target_table_type = Column(String)
    target_page_name = Column(String)
    target_page_area = Column(String)
    update_group = Column(String)
    last_update = Column(DateTime(timezone=True))
    update_quantity = Column(Integer)
    update_failures = Column(Integer)


class TaskTemplate(Base):
    __tablename__ = 'TaskTemplate'

    id = Column(Integer, primary_key=True)
    source_table = Column(String, nullable=False)
    name_of_process = Column(String, unique=True, nullable=False)

    source_table_type = Column(String)
    source_page_name = Column(String)
    source_page_area = Column(String)
    scan_group = Column(String)
    scan_interval = Column(Integer)
    process_data_method = Column(String)
    target_table_type = Column(String)
    target_page_name = Column(String)
    target_page_area = Column(String)
    update_group = Column(String)


class TrackedTables(Base):
    __tablename__ = 'TrackedTables'

    id = Column(Integer, primary_key=True)
    table_type = Column(Text)
    label = Column(Text)
    spreadsheet_id = Column(Text)
    valid_from = Column(Date)
    valid_to = Column(Date)


class DealerMonthlyStatus(Base):
    __tablename__ = "DealerMonthlyStatus"

    id = Column(Integer, primary_key=True)
    dealer_name = Column(String, nullable=False)
    dealer_nicknames = Column(PG_ARRAY(String), default=[])

    related_month = Column(Date, nullable=False)

    schedule = Column(Boolean)
    bonus = Column(Boolean)
    qa_list = Column(Boolean)
    feedback_status = Column(Boolean)

    __table_args__ = (
        UniqueConstraint("dealer_name", "related_month", name="uq_dealername_month"),
    )


class ScheduleOT(Base):
    __tablename__ = 'ScheduleOT'

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    dealer_name = Column(Text)
    shift_type = Column(Text)
    related_month = Column(Date)



class UserRole(enum.Enum):
    stranger = "stranger"
    shuffler = "shuffler"
    dealer = "dealer"
    manager = "manager"
    qa_manager = "qa_manager"
    hr_manager = "hr_manager"
    chief_sm_manager = "chief_sm_manager"
    trainer_manager = "trainer_manager"
    floor_manager = "floor_manager"
    admin = "admin"
    architect = "architect"

class UserStatus(enum.Enum):
    requested_access = "requested_access"
    employee = "employee"
    suspended = "suspended"
    rejected = "rejected"

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    dealer_name = Column(String, nullable=False)
    role = Column(Enum(UserRole), nullable=False)
    photo_fileID = Column(String, nullable=True, default=None)
    status = Column(Enum(UserStatus), nullable=False, default=UserStatus.requested_access)
    approved_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    approved_at = Column(DateTime, nullable=True)

    approved_by = relationship("User", remote_side=[id], backref="approved_users")

class QaList(Base):
    __tablename__ = "QaList"

    id = Column(Integer, primary_key=True)
    dealer_name = Column(String, nullable=False)

    VIP = Column(String, nullable=False)
    GENERIC = Column(String, nullable=False)
    LEGENDZ = Column(String, nullable=False)
    GSBJ = Column(String, nullable=False)
    TURKISH = Column(String, nullable=False)
    TRISTAR = Column(String, nullable=False)
    TritonRL = Column(String, nullable=False)

    QA_comment = Column(String)
    
    Male = Column(String, nullable=False)
    BJ = Column(String, nullable=False)
    BC = Column(String, nullable=False)
    RL = Column(String, nullable=False)
    DT = Column(String, nullable=False)
    HSB = Column(String, nullable=False)
    swBJ = Column(String, nullable=False)
    swBC = Column(String, nullable=False)
    swRL = Column(String, nullable=False)
    SH = Column(String, nullable=False)
    gsDT = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint('dealer_name', name='uq_dealer_name'),
    )

