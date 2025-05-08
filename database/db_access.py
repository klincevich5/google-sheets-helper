# db_access.py

import sqlite3
from core.config import DB_PATH

def get_connection():
    return sqlite3.connect(DB_PATH)

def get_rotations_stats():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM RotationsInfo")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM RotationsInfo WHERE scan_failures > 0")
    errors = cursor.fetchone()[0]

    conn.close()
    return {"total": total, "errors": errors}

def get_rotations_tasks_by_tab(tab_name):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, name_of_process
        FROM RotationsInfo
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
               hash, last_scan, last_update,
               scan_quantity, update_quantity,
               scan_failures, update_failures,
               scan_interval
        FROM RotationsInfo
        WHERE id = ?
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
            "scan_quantity": row[6],
            "update_quantity": row[7],
            "scan_failures": row[8],
            "update_failures": row[9],
            "scan_interval": row[10]
        }
    return None

def get_sheets_tasks():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name_of_process, source_page_name, source_page_area,
               target_page_name, target_page_area, scan_failures, hash,
               last_scan, last_update
        FROM SheetsInfo
        ORDER BY name_of_process
    """)
    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": row[0],
            "name": row[1],
            "source": f"{row[2]}!{row[3]}",
            "target": f"{row[4]}!{row[5]}",
            "failures": row[6],
            "hash": row[7],
            "last_scan": row[8] or "—",
            "last_update": row[9] or "—"
        } for row in rows
    ]


def get_sheet_by_id(sheet_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name_of_process, source_page_name, source_page_area,
               hash, last_scan, last_update,
               scan_quantity, update_quantity,
               scan_failures, update_failures,
               scan_interval
        FROM SheetsInfo
        WHERE id = ?
    """, (sheet_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return {
            "name": row[0],
            "source": f"{row[1]}!{row[2]}",
            "hash": row[3],
            "last_scan": row[4] or "—",
            "last_update": row[5] or "—",
            "scan_quantity": row[6],
            "update_quantity": row[7],
            "scan_failures": row[8],
            "update_failures": row[9],
            "scan_interval": row[10]
        }
    return None


def get_all_tracked_tables():
    from bot.utils_bot import get_connection

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM TrackedTables")
    result = cursor.fetchall()
    conn.close()
    return result

def get_sheets_stats():
    from bot.utils_bot import get_connection

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM SheetsInfo")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM SheetsInfo WHERE scan_failures > 0")
    errors = cursor.fetchone()[0]

    conn.close()
    return {"total": total, "errors": errors}

def get_top_error_tasks(source="rotations", limit=5):
    conn = get_connection()
    cursor = conn.cursor()

    table = "RotationsInfo" if source == "rotations" else "SheetsInfo"

    cursor.execute(f"""
        SELECT name_of_process, scan_quantity, scan_failures
        FROM {table}
        WHERE scan_failures > 0
        ORDER BY scan_failures DESC
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "name": row[0],
            "ok": row[1] - row[2],
            "fail": row[2]
        } for row in rows
    ]
