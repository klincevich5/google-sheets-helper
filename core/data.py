# data.py

from datetime import datetime, timedelta
from core.models import Task
from zoneinfo import ZoneInfo

from core.config import TIMEZONE

# actual_date_now = datetime.now(ZoneInfo(TIMEZONE))
actual_date_now = datetime(2025, 4, 4, 10, 0, tzinfo=ZoneInfo(TIMEZONE))

#################################################################################
# Получаю актуальные TrackedTables ID
#################################################################################

def return_tracked_tables(conn):
    """
    Получение карты соответствия table_type -> spreadsheet_id из таблицы TrackedTables,
    с учётом даты действия (valid_from, valid_to).
    """
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
# Загрузка актуальных задач из SheetsInfo
#################################################################################

def load_sheetsinfo_tasks(conn):
    """Загрузка актуальных задач из SheetsInfo."""
    cursor = conn.cursor()
    now = actual_date_now

    cursor.execute("SELECT * FROM SheetsInfo")
    rows = cursor.fetchall()

    tasks = []
    for row in rows:
        if row["is_active"] == 0:
            continue
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

#################################################################################
# Загрузка актуальных задач из RotationsInfo
#################################################################################

def get_active_tabs(now=None):
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
    
    tab_list = ["DAY 1", "NIGHT 1"]

    return tab_list

def load_rotationsinfo_tasks(conn):
    """Загрузка актуальных задач из RotationsInfo."""
    cursor = conn.cursor()
    now = actual_date_now
    active_tabs = get_active_tabs(now)

    cursor.execute("SELECT * FROM RotationsInfo")
    rows = cursor.fetchall()

    tasks = []
    for row in rows:
        if row["source_page_name"] not in active_tabs:
            continue
        if row["is_active"] == 0:
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