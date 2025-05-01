# data.py

from datetime import datetime, timedelta
from models import Task
from utils import get_active_tabs
from zoneinfo import ZoneInfo

from config import TIMEZONE

def return_tracked_tables(conn):
    """Получение списка таблиц из TrackedTables."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM TrackedTables")
    rows = cursor.fetchall()

    tracked_tables = []
    for row in rows:
        tracked_tables.append({
            "id": row["id"],
            "table_type": row["table_type"],
            "label": row["label"],
            "spreadsheet_id": row["spreadsheet_id"],
            "valid_from": row["valid_from"],
            "valid_to": row["valid_to"]
        })
    return tracked_tables

def load_sheetsinfo_tasks(conn):
    """Загрузка актуальных задач из SheetsInfo."""
    cursor = conn.cursor()
    now = datetime.now(ZoneInfo(TIMEZONE))

    cursor.execute("SELECT * FROM SheetsInfo")
    rows = cursor.fetchall()

    tasks = []
    for row in rows:
        last_scan = row["last_scan"]
        scan_interval = row["scan_interval"]

        # === Безопасная обработка last_scan ===
        if not last_scan or last_scan == "NULL":
            last_scan_dt = datetime.min.replace(tzinfo=ZoneInfo(TIMEZONE))
        else:
            last_scan_dt = datetime.fromisoformat(last_scan)

        if now >= last_scan_dt + timedelta(seconds=scan_interval):
            task = Task(dict(row))
            task.source_table = "SheetsInfo"
            tasks.append(task)

    return tasks

def load_rotationsinfo_tasks(conn):
    """Загрузка актуальных задач из RotationsInfo."""
    cursor = conn.cursor()
    now = datetime.now(ZoneInfo(TIMEZONE))
    active_tabs = get_active_tabs(now)

    cursor.execute("SELECT * FROM RotationsInfo")
    rows = cursor.fetchall()

    tasks = []
    for row in rows:
        if row["source_page_name"] not in active_tabs:
            continue

        last_scan = row["last_scan"]
        scan_interval = row["scan_interval"]

        if not last_scan or last_scan == "NULL":
            last_scan_dt = datetime.min.replace(tzinfo=ZoneInfo(TIMEZONE))
        else:
            last_scan_dt = datetime.fromisoformat(last_scan)

        if now >= last_scan_dt + timedelta(seconds=scan_interval):
            task = Task(dict(row))
            task.source_table = "RotationsInfo"
            task.actual_tab = task.source_page_name
            tasks.append(task)

    return tasks
