# core/data.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session
from sqlalchemy import text

from database.db_models import TrackedTables
from core.task_model import Task
from utils.logger import log_to_file, log_section
from core.config import TIMEZONE

# Проверка TIMEZONE
try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

def return_tracked_tables(session: Session) -> dict:
    """Возвращает актуальные table_type -> spreadsheet_id из TrackedTables"""
    today = datetime.now(timezone).date()
    tables = session.query(TrackedTables).all()
    return {
        table.table_type: table.spreadsheet_id
        for table in tables
        if table.valid_from <= today <= table.valid_to
    }

def get_active_tabs(now=None):
    now = now or datetime.now(timezone)
    day = now.day
    hour = now.hour

    if 9 <= hour < 19:
        return [f"DAY {day}"]
    elif 19 <= hour < 21:
        return [f"DAY {day}", f"NIGHT {day}"]
    elif 21 <= hour <= 23:
        return [f"NIGHT {day}"]
    elif 0 <= hour < 7:
        return [f"NIGHT {(now - timedelta(days=1)).day}"]
    elif 7 <= hour < 9:
        return [f"DAY {day}", f"NIGHT {(now - timedelta(days=1)).day}"]
    return []

def parse_datetime(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return datetime.min.replace(tzinfo=timezone)
    if not value:
        return datetime.min.replace(tzinfo=timezone)
    if value.tzinfo is None or isinstance(value.tzinfo, str):
        return value.replace(tzinfo=timezone)
    return value

def build_task(row, now, source_table):
    task = Task({
        "id": row.id,
        "is_active": row.is_active,
        "related_month": row.related_month,
        "name_of_process": row.name_of_process,
        "source_table_type": row.source_table_type,
        "source_page_name": getattr(row, "source_page_name", None),
        "source_page_area": row.source_page_area,
        "scan_group": row.scan_group,
        "last_scan": row.last_scan,
        "scan_interval": row.scan_interval,
        "scan_quantity": row.scan_quantity,
        "scan_failures": row.scan_failures,
        "hash": row.hash,
        "process_data_method": row.process_data_method,
        "values_json": row.values_json,
        "target_table_type": row.target_table_type,
        "target_page_name": row.target_page_name,
        "target_page_area": row.target_page_area,
        "update_group": row.update_group,
        "last_update": row.last_update,
        "update_quantity": row.update_quantity,
        "update_failures": row.update_failures
    })
    task.source_table = source_table
    return task

def load_rotationsinfo_tasks(session: Session, log_file):
    log_section("🔼 Фаза определения задач (RotationsInfo)", log_file)
    now = datetime.now(timezone)
    active_tabs = get_active_tabs(now)

    rows = session.execute(text("SELECT * FROM ready_rotations_tasks")).fetchall()
    tasks = []

    for row in rows:
        if row.source_page_name not in active_tabs:
            continue

        last_scan = parse_datetime(row.last_scan)
        next_scan_dt = last_scan + timedelta(seconds=row.scan_interval)
        minutes_left = int((next_scan_dt - now).total_seconds() / 60)

        log_to_file(
            log_file,
            (
                f"[✅READY] Task '{row.name_of_process} {row.source_page_name}' | "
                f"Last scan: {last_scan:%Y-%m-%d %H:%M:%S} | "
                f"Interval: {row.scan_interval // 60} min | "
                f"In: {minutes_left} min | "
                f"Next scan at: {next_scan_dt:%Y-%m-%d %H:%M} | "
                f"Now: {now:%Y-%m-%d %H:%M:%S}"
            )
        )

        tasks.append(build_task(row, now, "RotationsInfo"))

    return tasks

def load_sheetsinfo_tasks(session: Session, log_file):
    log_section("🔼 Фаза определения задач (SheetsInfo)", log_file)
    now = datetime.now(timezone)

    rows = session.execute(text("SELECT * FROM ready_sheets_tasks")).fetchall()
    tasks = []

    for row in rows:
        last_scan = parse_datetime(row.last_scan)
        next_scan_dt = last_scan + timedelta(seconds=row.scan_interval)
        minutes_left = int((next_scan_dt - now).total_seconds() / 60)

        log_to_file(
            log_file,
            (
                f"[✅READY] Task '{row.name_of_process} {row.source_page_name}' | "
                f"Last scan: {last_scan:%Y-%m-%d %H:%M:%S} | "
                f"Interval: {row.scan_interval // 60} min | "
                f"In: {minutes_left} min | "
                f"Next scan at: {next_scan_dt:%Y-%m-%d %H:%M} | "
                f"Now: {now:%Y-%m-%d %H:%M:%S}"
            )
        )

        tasks.append(build_task(row, now, "SheetsInfo"))

    return tasks
