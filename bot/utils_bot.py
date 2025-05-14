# bot/utils_bot.py

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy.orm import Session
from sqlalchemy import select

from core.config import MAIN_LOG, TIMEZONE
from database.db_models import TrackedTables, RotationsInfo, SheetsInfo
from database.session import SessionLocal


def format_datetime_pl(dt_str: str) -> str:
    if not dt_str or dt_str == "—":
        return "—"
    try:
        dt = datetime.fromisoformat(dt_str)
        dt = dt.astimezone(ZoneInfo(TIMEZONE))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return dt_str


def get_surrounding_tabs():
    now = datetime.now(ZoneInfo(TIMEZONE))
    days = [now + timedelta(days=delta) for delta in (-2, -1, 0, 1, 2)]
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


def get_logs_for_task(task_id: int, model, session: Session = None):
    session = session or SessionLocal()
    task = session.get(model, task_id)
    if not task:
        return "⚠️ Задача не найдена"

    name = task.name_of_process
    lines = tail_log(50).splitlines()
    filtered = [line for line in lines if name in line]
    return "\n".join(filtered[-10:]) or "🔍 Нет логов по задаче"


def get_logs_for_rot_task(task_id: int):
    return get_logs_for_task(task_id, RotationsInfo)


def get_logs_for_sheet_task(task_id: int):
    return get_logs_for_task(task_id, SheetsInfo)


def get_current_datetime():
    now = datetime.now(ZoneInfo(TIMEZONE))
    return now.strftime("%d %B %Y, %H:%M")


def get_current_month_tables(session: Session = None):
    session = session or SessionLocal()
    now = datetime.now(ZoneInfo(TIMEZONE)).date()

    tables = session.query(TrackedTables).all()
    result = {}

    for table in tables:
        if not table.valid_from:
            continue
        if table.valid_from.month != now.month or table.valid_from.year != now.year:
            continue
        if table.valid_to and table.valid_to < now:
            continue

        result[table.label] = {
            "spreadsheet_id": table.spreadsheet_id,
            "type": table.table_type,
            "valid_from": table.valid_from.strftime("%d.%m.%Y"),
            "valid_to": table.valid_to.strftime("%d.%m.%Y") if table.valid_to else None
        }

    return result


def get_rotations_tasks_by_tab(tab_name: str, session: Session = None):
    session = session or SessionLocal()
    rows = session.query(RotationsInfo.id, RotationsInfo.name_of_process).filter(
        RotationsInfo.source_page_name == tab_name
    ).order_by(RotationsInfo.name_of_process).all()

    return [{"id": row[0], "name": row[1]} for row in rows]


def get_task_by_id(task_id: int, session: Session = None):
    session = session or SessionLocal()
    task = session.get(RotationsInfo, task_id)

    if task:
        return {
            "name": task.name_of_process,
            "source": f"{task.source_page_name}!{task.source_page_area}",
            "hash": task.hash,
            "last_scan": task.last_scan or "—",
            "last_update": task.last_update or "—",
            "scan_failures": task.scan_failures
        }
    return None