# core/data.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session
from database.db_models import TrackedTables, RotationsInfo, SheetsInfo
from core.task_model import Task
from utils.logger import log_to_file, log_section
from core.config import TIMEZONE

# Проверка корректности TIMEZONE
try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

def return_tracked_tables(session: Session) -> dict:
    """
    Получение карты соответствия table_type -> spreadsheet_id из таблицы TrackedTables,
    с учётом даты действия (valid_from, valid_to).
    """
    actual_date_now = datetime.now(timezone).date()
    print(f"📅 Сегодня: {actual_date_now}")

    doc_id_map = {}
    tables = session.query(TrackedTables).all()
    for table in tables:
        if table.valid_from <= actual_date_now <= table.valid_to:
            doc_id_map[table.table_type] = table.spreadsheet_id
    
    return doc_id_map

def get_active_tabs(now=None):
    actual_date_now = datetime.now(timezone)
    if not now:
        now = actual_date_now

    hour = now.hour
    tab_list = []

    if 9 <= hour < 19:
        tab_list.append(f"DAY {now.day}")
    elif 19 <= hour < 21:
        tab_list.append(f"DAY {now.day}")
        tab_list.append(f"NIGHT {now.day}")
    elif 21 <= hour <= 23:
        tab_list.append(f"NIGHT {now.day}")
    elif 0 <= hour < 7:
        yesterday = now - timedelta(days=1)
        tab_list.append(f"NIGHT {yesterday.day}")
    elif 7 <= hour < 9:
        yesterday = now - timedelta(days=1)
        tab_list.append(f"DAY {now.day}")
        tab_list.append(f"NIGHT {yesterday.day}")

    return tab_list

def load_rotationsinfo_tasks(session: Session, log_file):
    """Загрузка актуальных задач из RotationsInfo с логированием."""
    log_section("🔼 Фаза определения задач", log_file)
    now = datetime.now(timezone)
    active_tabs = get_active_tabs(now)

    tasks = []

    rows = session.query(RotationsInfo).filter(RotationsInfo.is_active == 1).all()

    for row in rows:
        if row.source_page_name not in active_tabs:
            continue

        last_scan = row.last_scan or datetime.min.replace(tzinfo=timezone)
        if isinstance(last_scan, str):
            last_scan = datetime.fromisoformat(last_scan)
        if last_scan.tzinfo is None:
            last_scan = last_scan.replace(tzinfo=timezone)

        next_scan_dt = last_scan + timedelta(seconds=row.scan_interval)
        minutes_left = int((next_scan_dt - now).total_seconds() / 60)
        status = "READY" if now >= next_scan_dt else "WAITING"

        log_to_file(
            log_file,
            (
                f"[{status}] Task '{row.name_of_process}' | "
                f"Last scan: {last_scan.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Interval: {row.scan_interval // 60} min | "
                f"In: {minutes_left} min | "
                f"Next scan at: {next_scan_dt.strftime('%Y-%m-%d %H:%M')} | "
                f"Now: {now.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        )

        if now >= next_scan_dt:
            task = Task({
                "id": row.id,
                "is_active": row.is_active,
                "name_of_process": row.name_of_process,
                "source_table_type": row.source_table_type,
                "source_page_name": row.source_page_name,
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
            task.source_table = "RotationsInfo"
            task.actual_tab = row.source_page_name
            tasks.append(task)

    return tasks

def load_sheetsinfo_tasks(session: Session, log_file):
    """Загрузка актуальных задач из SheetsInfo с логированием."""
    log_section("🔼 Фаза определения задач", log_file)

    tz = timezone
    now = datetime.now(tz)
    tasks = []

    rows = session.query(SheetsInfo).filter(SheetsInfo.is_active == 1).all()

    for row in rows:
        last_scan = row.last_scan or datetime.min.replace(tzinfo=tz)

        # Приведение строки к datetime
        if isinstance(last_scan, str):
            try:
                last_scan = datetime.fromisoformat(last_scan)
            except ValueError:
                last_scan = datetime.min.replace(tzinfo=tz)

        # Установка tzinfo, если нет
        if last_scan.tzinfo is None or isinstance(last_scan.tzinfo, str):
            last_scan = last_scan.replace(tzinfo=tz)

        next_scan_dt = last_scan + timedelta(seconds=row.scan_interval)
        minutes_left = int((next_scan_dt - now).total_seconds() // 60)
        status = "✅READY" if now >= next_scan_dt else "❌WAITING"

        log_to_file(
            log_file,
            (
                f"[{status}] Task '{row.name_of_process}' | "
                f"Last scan: {last_scan:%Y-%m-%d %H:%M:%S} | "
                f"Interval: {row.scan_interval // 60} min | "
                f"In: {minutes_left} min | "
                f"Next scan at: {next_scan_dt:%Y-%m-%d %H:%M} | "
                f"Now: {now:%Y-%m-%d %H:%M:%S}"
            )
        )

        if now >= next_scan_dt:
            task = Task({
                "id": row.id,
                "is_active": row.is_active,
                "name_of_process": row.name_of_process,
                "source_table_type": row.source_table_type,
                "source_page_name": row.source_page_name,
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
            task.source_table = "SheetsInfo"
            tasks.append(task)

    return tasks
