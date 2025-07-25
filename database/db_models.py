# ✅ Обновлённый файл db_models.py

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, Date, Time, DateTime,
    UniqueConstraint, Enum, BigInteger, ForeignKey, Index
)
import enum
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import JSONB, ARRAY as PG_ARRAY
from sqlalchemy import Column, Integer, Text, Date, TIMESTAMP, text
from sqlalchemy.sql import func

Base = declarative_base()

class LogEntry(Base):
    __tablename__ = "Logs"

    id = Column(Integer, primary_key=True)
    log_source = Column(Text, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    level = Column(Text, nullable=False)
    phase = Column(Text)
    task = Column(Text)
    status = Column(Text)
    message = Column(Text)
    error = Column(Text)

# 📌 Персона с периодом работы
class Person(Base):
    __tablename__ = "Persons"

    id = Column(Integer, primary_key=True)
    full_name = Column(String, nullable=False)
    dealer_nickname = Column(String, nullable=False)  # Обязательное поле

    telegram_id = Column(BigInteger, unique=True, nullable=True)
    whatsapp_number = Column(String, nullable=True)
    photo_fileID = Column(String, nullable=True)

    date_start = Column(Date, nullable=False)
    date_end = Column(Date, nullable=True)

    # Отношения
    user_account = relationship("User", back_populates="person", uselist=False)
    feedbacks = relationship("FeedbackStorage", back_populates="person")
    mistakes = relationship("MistakeStorage", back_populates="person")
    qa_list_records = relationship("QaList", back_populates="person")
    dealer_monthly_statuses = relationship("DealerMonthlyStatus", back_populates="person")
    schedule_records = relationship("ScheduleOT", back_populates="person")

    def __repr__(self):
        return f"<Person(id={self.id}, name='{self.full_name}', nick='{self.dealer_nickname}')>"

    __table_args__ = (
        UniqueConstraint("full_name", "dealer_nickname", name="uq_person_fullname_nickname"),
        Index("ix_persons_full_name", "full_name"),
        Index("ix_person_telegram_id", "telegram_id"),
        Index("ix_person_date", "date_start", "date_end"),
    )


# 📌 Графики (помесячно, построчно)
class ScheduleOT(Base):
    __tablename__ = 'ScheduleOT'

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("Persons.id"), nullable=False)
    person = relationship("Person", back_populates="schedule_records")

    related_date = Column(Date, nullable=False)
    shift_type = Column(Text)
    related_month = Column(Date, nullable=False)
    break_number = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("person_id", "related_date", name="uq_schedule_per_day"),
        Index("ix_schedule_month", "related_month"),
        Index("ix_schedule_date", "related_date"),
    )



# 📌 Ручной статус на месяц
class DealerMonthlyStatus(Base):
    __tablename__ = "DealerMonthlyStatus"

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("Persons.id"), nullable=False)
    person = relationship("Person", back_populates="dealer_monthly_statuses")

    related_month = Column(Date, nullable=False)

    schedule = Column(Boolean)
    bonus = Column(Boolean)
    qa_list = Column(Boolean)
    feedback_status = Column(Boolean)

    __table_args__ = (
        UniqueConstraint("person_id", "related_month", name="uq_dealer_month"),
        Index("ix_dealer_month", "related_month"),
    )


# 📌 Telegram-аккаунты
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
    __tablename__ = "Users"

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("Persons.id"), nullable=False)
    person = relationship("Person", back_populates="user_account")

    role = Column(Enum(UserRole), nullable=False)
    status = Column(Enum(UserStatus), nullable=False, default=UserStatus.requested_access)

    approved_by_id = Column(Integer, ForeignKey("Users.id"), nullable=True)
    approved_at = Column(DateTime, nullable=True)

    approved_by = relationship("User", remote_side=[id], backref="approved_users")

    def __repr__(self):
        return f"<User(id={self.id}, role={self.role.name}, status={self.status.name})>"

    __table_args__ = (
        Index("ix_user_person_id", "person_id"),
        Index("ix_user_approved_by", "approved_by_id"),
    )



class FeedbackStorage(Base):
    __tablename__ = 'FeedbackStorage'

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("Persons.id"), nullable=True)
    person = relationship("Person", back_populates="feedbacks")
    feedback_nr = Column(Integer, nullable=False)
    related_month = Column(Date)
    related_date = Column(Date)
    related_shift = Column(Text)
    floor = Column(Text)
    game = Column(Text)
    dealer_name = Column(String)
    sm_name = Column(Text)
    reason = Column(Text)
    total = Column(Integer)
    proof = Column(Text)
    explanation_of_the_reason = Column(Text)
    action_taken = Column(Text)
    forwarded_feedback = Column(Text)
    comment_after_forwarding = Column(Text)
        
    __table_args__ = (
        Index("ix_feedback_person_id", "person_id"),
        Index("ix_feedback_related_date", "related_date"),
        UniqueConstraint("dealer_name", "feedback_nr", name="uq_feedback_dealer_nr"),
    )


class MistakeStorage(Base):
    __tablename__ = 'MistakeStorage'

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("Persons.id"), nullable=True)
    person = relationship("Person", back_populates="mistakes")

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
    dealer_name = Column(String)
    sm_name = Column(Text)
    last_row = Column(Integer)

    __table_args__ = (
        Index("ix_mistake_person_id", "person_id"),
        Index("ix_mistake_related_date", "related_date"),
    )

class QaList(Base):
    __tablename__ = "QaList"

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("Persons.id"), nullable=True)
    person = relationship("Person", back_populates="qa_list_records")
    dealer_name = Column(String, nullable=False)

    vip = Column(String, nullable=False)
    generic = Column(String, nullable=False)
    legendz = Column(String, nullable=False)
    gsbj = Column(String, nullable=False)
    turkish = Column(String, nullable=False)
    tristar = Column(String, nullable=False)
    tritonrl = Column(String, nullable=False)

    qa_comment = Column(String)

    male = Column(String, nullable=False)
    bj = Column(String, nullable=False)
    bc = Column(String, nullable=False)
    rl = Column(String, nullable=False)
    dt = Column(String, nullable=False)
    hsb = Column(String, nullable=False)
    swbj = Column(String, nullable=False)
    swbc = Column(String, nullable=False)
    swrl = Column(String, nullable=False)
    sh = Column(String, nullable=False)
    gsdt = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint('dealer_name', name='uq_dealer_name'),
        Index("ix_qalist_person_id", "person_id"),
    )

class GamingTable(Base):
    __tablename__ = "GamingTables"

    id = Column(Integer, primary_key=True)

    status = Column(Integer, nullable=False)
    local_name = Column(Text, nullable=False)
    active_from = Column(Date, nullable=False)
    active_until = Column(Date)

    notes = Column(Text)

    table_id = Column(Text, nullable=False)
    gaming_floor = Column(Text, nullable=False)
    dealers_game = Column(Text, nullable=False)
    floor_number = Column(Integer)
    end_user = Column(Text)

    dui_nr = Column(Text)
    vnc_ip = Column(Text)
    vnc_password = Column(Text)
    rec_by = Column(Text)
    encoder_ip = Column(Text)
    encoder_password = Column(Text)

    __table_args__ = (
        UniqueConstraint('local_name', 'active_from', 'table_id', name='uq_gamingtables_local_from_tableid'),
        Index('ix_gamingtables_status_floor_game', 'status', 'gaming_floor', 'dealers_game'),
    )

# 📌 Настройки, шаблоны и таблицы
class BotSettings(Base):
    __tablename__ = 'BotSettings'

    key = Column(Text, primary_key=True)
    value = Column(Text)


class TrackedTables(Base):
    __tablename__ = 'TrackedTables'

    id = Column(Integer, primary_key=True)
    table_type = Column(Text)
    label = Column(Text)
    spreadsheet_id = Column(Text)
    valid_from = Column(Date)
    valid_to = Column(Date)


class TaskTemplate(Base):
    __tablename__ = 'TaskTemplate'

    id = Column(Integer, primary_key=True)
    name_of_process = Column(String, unique=True, nullable=False)
    source_table = Column(String, nullable=False)
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

    valid_from = Column(Date)
    valid_to = Column(Date)

    __table_args__ = (
        Index("ix_template_source_table", "source_table"),
    )


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

    __table_args__ = (
        UniqueConstraint("related_month", "name_of_process", "source_page_name", name="uq_rotations_month_name_page"),
    )


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
    values_json = Column(JSONB)
    target_table_type = Column(String)
    target_page_name = Column(String)
    target_page_area = Column(String)
    update_group = Column(String)
    last_update = Column(DateTime(timezone=True))
    update_quantity = Column(Integer)
    update_failures = Column(Integer)

    __table_args__ = (
        UniqueConstraint("related_month", "name_of_process", name="uq_sheets_month_name"),
    )
