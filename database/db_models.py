# database/db_models.py

from sqlalchemy import Column, Integer, String, Text, Boolean, Date, Time, DateTime, UniqueConstraint, Enum, BigInteger
import enum
from sqlalchemy.orm import declarative_base
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

class ApiUsage(Base):
    __tablename__ = 'ApiUsage'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime(timezone=True))
    token = Column(Text)
    counter = Column(Integer)
    info_scan_group = Column(Text)
    info_update_group = Column(Text)
    success = Column(Boolean)


class BotSettings(Base):
    __tablename__ = 'BotSettings'

    key = Column(Text, primary_key=True)
    value = Column(Text)


class FeedbackStorage(Base):
    __tablename__ = 'FeedbackStorage'

    id = Column(Integer, primary_key=True)
    related_month = Column(Date)
    date = Column(Date)
    shift = Column(Text)
    floor = Column(Text)
    game = Column(Text)
    gp_name_surname = Column(Text)
    sm_name_surname = Column(Text)
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
    date = Column(Date)
    time = Column(Time(timezone=True))
    game_id = Column(Text)
    mistake = Column(Text)
    type = Column(Text)
    is_cancel = Column(Integer)
    dealer = Column(Text)
    sm = Column(Text)
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


class TrackedTables(Base):
    __tablename__ = 'TrackedTables'

    id = Column(Integer, primary_key=True)
    table_type = Column(Text)
    label = Column(Text)
    spreadsheet_id = Column(Text)
    valid_from = Column(Date)
    valid_to = Column(Date)


class FeedbackStatus(Base):
    __tablename__ = 'FeedbackStatus'

    id = Column(Integer, primary_key=True)
    name_surname = Column(Text)
    status = Column(Text)

    __table_args__ = (
        UniqueConstraint('name_surname', name='uq_name_surname'),
    )

class ScheduleOT(Base):
    __tablename__ = 'ScheduleOT'

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    dealer_name = Column(Text)
    shift_type = Column(Text)
    related_month = Column(Date)

    
class UserRole(enum.Enum):
    Shuffler = "Shuffler"
    Dealer = "Dealer"
    Manager = "Manager"
    QA_Manager = "QA Manager"
    HR_Manager = "HR Manager"
    Chief_SM_Manager = "Chief SM Manager"
    Trainer_Manager = "Trainer Manager"
    Floor_Manager = "Floor Manager"
    Admin = "Admin"
    Super_Admin = "Super Admin"

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    full_name = Column(String, nullable=False)
    role = Column(Enum(UserRole), nullable=False)