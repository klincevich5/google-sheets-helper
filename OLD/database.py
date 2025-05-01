# database.py

import sqlite3
from datetime import datetime, timedelta
from config import DB_PATH, WARSAW_TZ
from logger import log_to_file
import json

def get_doc_id_map():
    """
    Возвращает словарь {source_table_type: spreadsheet_id}
    Только для документов, актуальных по дате.
    """
    today = datetime.now(WARSAW_TZ).date()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT table_type, spreadsheet_id, valid_from, valid_to FROM TrackedTables")
    rows = cursor.fetchall()
    conn.close()

    result = {}
    for row in rows:
        start = parse_ddmmyyyy(row["valid_from"])
        end = parse_ddmmyyyy(row["valid_to"]) if row["valid_to"] else None
        if start and start <= today and (not end or today <= end):
            result[row["table_type"]] = row["spreadsheet_id"]
    return result

def get_pending_scans(table_name: str):
    now = datetime.now(WARSAW_TZ)  # текущее локальное время

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    rows = cursor.execute(f"SELECT * FROM {table_name}").fetchall()
    pending = []

    for row in rows:
        interval_raw = row["scan_interval"]
        if interval_raw is None or str(interval_raw).strip() == "":
            continue

        try:
            interval = int(interval_raw)
        except Exception:
            continue

        raw_last = row["last_scan"]
        if not raw_last:  # если даты нет — нужно отсканировать
            pending.append(dict(row))
            continue

        try:
            last = datetime.fromisoformat(raw_last).astimezone(WARSAW_TZ)
        except Exception:
            last = datetime.min.replace(tzinfo=WARSAW_TZ)

        if now - last >= timedelta(seconds=interval):
            pending.append(dict(row))

    conn.close()
    return pending

def update_sheet_task_data(task, table_name, log_file):
    # Защита: допустимые названия таблиц
    allowed_tables = {"SheetsInfo", "RotationsInfo"}
    if table_name not in allowed_tables:
        log_to_file(log_file, f"❌ Неверное имя таблицы: {table_name}")
        return
    log_to_file(log_file, f"🟢 Обновление таблицы {table_name} для задачи ID={task['id']}\n")
    log_to_file(log_file, "-" * 30 + "\n")
    query = f"""
        UPDATE {table_name} 
        SET
            last_scan = ?,
            scan_quantity = ?,
            scan_failures = ?,
            hash = ?,
            values_json = ?
        WHERE id = ?
    """
    now_local = datetime.now(WARSAW_TZ).isoformat()
    values = (
        task.get("last_scan", now_local),
        task.get("scan_quantity", 0),
        task.get("scan_failures", 0),
        task.get("hash"),
        task.get("values_json"),
        task.get("id")
    )
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()
    conn.close()

def update_sheet_import_data(task, table_name, log_file):
    query = f"""
        UPDATE {table_name}
        SET
            last_update = ?,
            update_quantity = ?,
            update_failures = ?
        WHERE id = ?
    """
    now_local = datetime.now(WARSAW_TZ).isoformat()
    values = (
        task.get("last_update", now_local),
        task.get("update_quantity", 0),
        task.get("update_failures", 0),
        task.get("id")
    )
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()
    conn.close()

def parse_ddmmyyyy(date_str):
    try:
        return datetime.strptime(date_str, "%d.%m.%Y").date()
    except Exception:
        return None

def update_rotation_row_data(task_id, start, end, hash_val, values, need_update, table_name, log_file):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    now_str = datetime.now(WARSAW_TZ).isoformat()
    values_json = json.dumps(values, ensure_ascii=False, default=str)

    query = f"""
        UPDATE {table_name} SET
            start_row = ?,
            end_row = ?,
            last_update = ?,
            update_quantity = COALESCE(update_quantity, 0) + 1,
            hash = ?,
            values_json = ?,
            need_update = ?
        WHERE id = ?
    """
    cursor.execute(query, (
        start,
        end,
        now_str,
        hash_val,
        values_json,
        "TRUE" if need_update else "FALSE",
        task_id
    ))
    conn.commit()
    conn.close()

    if log_file:
        log_to_file(log_file, f"✅ Обновлены значения в RotationsInfo (ID={task_id})")

def set_need_update_true_below(task_id, table_name, log_file):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    query = f"""
        UPDATE {table_name}
        SET need_update = 'TRUE'
        WHERE id > ?
    """
    cursor.execute(query, (task_id,))
    conn.commit()
    conn.close()

    if log_file:
        log_to_file(log_file, f"🔁 Проставлено need_update=TRUE для всех ID > {task_id}")
