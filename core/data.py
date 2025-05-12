# core/data.py

from datetime import datetime, timedelta
from core.models import Task
from zoneinfo import ZoneInfo
from utils.logger import log_to_file, log_section

from core.config import TIMEZONE


# actual_date_now = datetime(2025, 4, 4, 10, 0, tzinfo=ZoneInfo(TIMEZONE))

#################################################################################
# Получаю актуальные TrackedTables ID
#################################################################################

def return_tracked_tables(conn):
    """
    Получение карты соответствия table_type -> spreadsheet_id из таблицы TrackedTables,
    с учётом даты действия (valid_from, valid_to).
    """
    actual_date_now = datetime.now(ZoneInfo(TIMEZONE))
    today = actual_date_now.date()
    print(f"📅 Сегодня: {today}")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM TrackedTables")
    rows = cursor.fetchall()

    doc_id_map = {}

    for row in rows:
        valid_from = datetime.strptime(row["valid_from"], "%d.%m.%Y").date()
        valid_to = datetime.strptime(row["valid_to"], "%d.%m.%Y").date()
        if valid_from <= today <= valid_to:
            doc_id_map[row["table_type"]] = row["spreadsheet_id"]

    return doc_id_map

#################################################################################
# Загрузка актуальных задач из RotationsInfo
#################################################################################

def get_active_tabs(now=None):
    actual_date_now = datetime.now(ZoneInfo(TIMEZONE))
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
    
    # tab_list = ["DAY 1", "NIGHT 1"]

    return tab_list

#################################################################################
# Загрузка актуальных задач из RotationsInfo
#################################################################################

def load_rotationsinfo_tasks(conn, log_file):
    """Загрузка актуальных задач из RotationsInfo с единым логированием."""
    log_section("🔼 Фаза определения задач", log_file)
    actual_date_now = datetime.now(ZoneInfo(TIMEZONE))

    cursor = conn.cursor()
    now = actual_date_now
    active_tabs = get_active_tabs(now)

    cursor.execute("SELECT * FROM RotationsInfo")
    rows = cursor.fetchall()

    tasks = []
    for row in rows:
        if row["source_page_name"] not in active_tabs or row["is_active"] == 0:
            continue

        name_of_process = row["name_of_process"]
        scan_interval = row["scan_interval"]
        last_scan = row["last_scan"]

        # Обработка времени
        if not last_scan or last_scan == "NULL":
            last_scan_dt = datetime.min.replace(tzinfo=ZoneInfo(TIMEZONE))
        else:
            last_scan_dt = datetime.fromisoformat(last_scan)
            if last_scan_dt.tzinfo is None:
                last_scan_dt = last_scan_dt.replace(tzinfo=ZoneInfo(TIMEZONE))

        next_scan_dt = last_scan_dt + timedelta(seconds=scan_interval)
        minutes_left = int((next_scan_dt - now).total_seconds() / 60)
        status = "READY" if now >= next_scan_dt else "WAITING"

        # Отладочный лог
        log_to_file(
            log_file,
            (
                f"[{status}] Task '{name_of_process}' | "
                f"Last scan: {last_scan_dt.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Interval: {scan_interval // 60} min | "
                f"In: {minutes_left} min | "
                f"Next scan at: {next_scan_dt.strftime('%Y-%m-%d %H:%M')} | "
                f"Now: {now.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        )

        if now >= next_scan_dt:
            task = Task(dict(row))
            task.source_table = "RotationsInfo"
            task.actual_tab = task.source_page_name
            tasks.append(task)

    return tasks

#################################################################################
# Загрузка актуальных задач из SheetsInfo
#################################################################################

def load_sheetsinfo_tasks(conn, log_file):
    """Загрузка актуальных задач из SheetsInfo с единым логированием."""
    log_section("🔼 Фаза определения задач", log_file)
    actual_date_now = datetime.now(ZoneInfo(TIMEZONE))
    
    cursor = conn.cursor()
    now = actual_date_now

    cursor.execute("SELECT * FROM SheetsInfo")
    rows = cursor.fetchall()

    tasks = []
    for row in rows:
        if row["is_active"] == 0:
            continue

        name_of_process = row["name_of_process"]
        scan_interval = row["scan_interval"]
        last_scan = row["last_scan"]

        # Обработка времени
        if not last_scan or last_scan == "NULL":
            last_scan_dt = datetime.min.replace(tzinfo=ZoneInfo(TIMEZONE))
        else:
            last_scan_dt = datetime.fromisoformat(last_scan)
            if last_scan_dt.tzinfo is None:
                last_scan_dt = last_scan_dt.replace(tzinfo=ZoneInfo(TIMEZONE))

        next_scan_dt = last_scan_dt + timedelta(seconds=scan_interval)
        minutes_left = int((next_scan_dt - now).total_seconds() / 60)
        status = "READY" if now >= next_scan_dt else "WAITING"

        # Отладочный лог
        log_to_file(
            log_file,
            (
                f"[{status}] Task '{name_of_process}' | "
                f"Last scan: {last_scan_dt.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Interval: {scan_interval // 60} min | "
                f"In: {minutes_left} min | "
                f"Next scan at: {next_scan_dt.strftime('%Y-%m-%d %H:%M')} | "
                f"Now: {now.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        )

        if now >= next_scan_dt:
            task = Task(dict(row))
            task.source_table = "SheetsInfo"
            tasks.append(task)

    return tasks
