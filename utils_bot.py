# utils_bot.py

import os
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from config import DB_PATH, MAIN_LOG, TIMEZONE

def format_datetime_pl(dt_str: str) -> str:
    if not dt_str or dt_str == "—":
        return "—"
    try:
        dt = datetime.fromisoformat(dt_str)
        dt = dt.astimezone(ZoneInfo(TIMEZONE))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return dt_str

def get_connection():
    return sqlite3.connect(DB_PATH)

def get_surrounding_tabs():
    now = datetime.now(ZoneInfo(TIMEZONE))
    days = [now - timedelta(days=2), now - timedelta(days=1), now,
            now + timedelta(days=1), now + timedelta(days=2)]
    result = []
    for day in days:
        result.append(f"DAY {day.day}")
        result.append(f"NIGHT {day.day}")
    return result

def tail_log(n=10):
    if not os.path.exists(MAIN_LOG):
        return "⚠️ Файл логов не найден"
    with open(MAIN_LOG, "r", encoding="utf-8") as f:
        lines = f.readlines()[-n:]
    return "".join(lines)

def get_logs_for_scanner():
    return tail_log(15)

def get_logs_for_shift(shift_name: str):
    lines = tail_log(50).splitlines()
    filtered = [line for line in lines if shift_name in line]
    return "\n".join(filtered[-10:]) or "🔍 Нет логов по смене"

def get_logs_for_rot_task(task_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name_of_process FROM RotationsInfo WHERE id = ?", (task_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return "⚠️ Задача не найдена"
    name = row[0]
    lines = tail_log(50).splitlines()
    filtered = [line for line in lines if name in line]
    return "\n".join(filtered[-10:]) or "🔍 Нет логов по задаче"

def get_logs_for_sheet_task(task_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name_of_process FROM SheetsInfo WHERE id = ?", (task_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return "⚠️ Задача не найдена"
    name = row[0]
    lines = tail_log(50).splitlines()
    filtered = [line for line in lines if name in line]
    return "\n".join(filtered[-10:]) or "🔍 Нет логов по задаче"

def get_current_datetime():
    now = datetime.now(ZoneInfo(TIMEZONE))
    return now.strftime("%d %B %Y, %H:%M")

def get_current_month_tables():
    def to_aware(dt: datetime, tz: ZoneInfo) -> datetime:
        return dt if dt.tzinfo else dt.replace(tzinfo=tz)

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT table_type, label, spreadsheet_id, valid_from, valid_to FROM TrackedTables")
    rows = cursor.fetchall()
    conn.close()

    tz = ZoneInfo(TIMEZONE)
    now = datetime.now(tz)

    docs = {}
    for row in rows:
        table_type, label, spreadsheet_id, valid_from, valid_to = row
        try:
            valid_from_dt = to_aware(datetime.strptime(valid_from, "%d.%m.%Y"), tz)
            valid_to_dt = to_aware(datetime.strptime(valid_to, "%d.%m.%Y"), tz) if valid_to else None
        except Exception:
            continue

        is_active = (
            valid_from_dt.month == now.month and valid_from_dt.year == now.year and
            (not valid_to_dt or valid_to_dt > now)
        )

        if is_active:
            docs[label] = {
                "spreadsheet_id": spreadsheet_id,
                "type": table_type,
                "valid_from": valid_from,
                "valid_to": valid_to
            }

    return docs

def get_rotations_tasks_by_tab(tab_name):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name_of_process FROM RotationsInfo
        WHERE source_page_name = ?
        ORDER BY name_of_process
    """, (tab_name,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id": row[0], "name": row[1]} for row in rows]

def get_task_by_id(task_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name_of_process, source_page_name, source_page_area,
               hash, last_scan, last_update, scan_failures
        FROM RotationsInfo WHERE id = ?
    """, (task_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            "name": row[0],
            "source": f"{row[1]}!{row[2]}",
            "hash": row[3],
            "last_scan": row[4] or "—",
            "last_update": row[5] or "—",
            "scan_failures": row[6]
        }
    return None

# def get_logs_for_task(task_id: str):
#     conn = get_connection()
#     cursor = conn.cursor()
#     cursor.execute("SELECT name_of_process FROM RotationsInfo WHERE id = ?", (task_id,))
#     row = cursor.fetchone()
#     conn.close()
#     if not row:
#         return "⚠️ Задача не найдена"
#     name = row[0]
#     lines = tail_log(50).splitlines()
#     filtered = [line for line in lines if name in line]
#     return "\n".join(filtered[-10:]) or "🔍 Нет логов по задаче"