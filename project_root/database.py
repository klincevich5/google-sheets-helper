import sqlite3
from typing import List, Dict, Any, Optional

from logger import log_info, log_error
from config import db_path, SHEETS_LOG_FILE, ROTATIONS_LOG_FILE
from notifier import send_telegram_message


def get_connection() -> sqlite3.Connection:
    """Создает подключение к базе данных."""
    return sqlite3.connect(db_path)


def check_db_integrity() -> None:
    """Проверяет наличие базы данных и всех таблиц."""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Создание таблиц
        cursor.executescript("""
        CREATE TABLE IF NOT EXISTS TrackedTables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_type TEXT NOT NULL,
            label TEXT,
            spreadsheet_id TEXT NOT NULL,
            valid_from TEXT NOT NULL,
            valid_to TEXT
        );

        CREATE TABLE IF NOT EXISTS SheetsInfo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_of_process TEXT,
            source_table_type TEXT NOT NULL,
            source_page_name TEXT NOT NULL,
            source_page_area TEXT NOT NULL,
            scan_group TEXT,
            last_scan TEXT,
            scan_interval INTEGER DEFAULT 1800,
            scan_quantity INTEGER DEFAULT 0,
            scan_failures INTEGER DEFAULT 0,
            hash TEXT,
            process_data_method TEXT DEFAULT 'process_default',
            values_json TEXT,
            target_table_type TEXT NOT NULL,
            target_page_name TEXT NOT NULL,
            target_page_area TEXT NOT NULL,
            update_group TEXT,
            last_update TEXT,
            update_quantity INTEGER DEFAULT 0,
            update_failures INTEGER DEFAULT 0,
            need_update BOOLEAN DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS RotationsInfo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_of_process TEXT,
            source_table_type TEXT NOT NULL,
            source_page_name TEXT NOT NULL,
            source_page_area TEXT NOT NULL,
            scan_group TEXT,
            last_scan TEXT,
            scan_interval INTEGER DEFAULT 60,
            scan_quantity INTEGER DEFAULT 0,
            scan_failures INTEGER DEFAULT 0,
            hash TEXT,
            process_data_method TEXT DEFAULT 'process_default',
            values_json TEXT,
            target_table_type TEXT NOT NULL,
            target_page_name TEXT NOT NULL,
            target_page_area TEXT NOT NULL,
            update_group TEXT,
            last_update TEXT,
            update_quantity INTEGER DEFAULT 0,
            update_failures INTEGER DEFAULT 0,
            need_update BOOLEAN DEFAULT 0
        );
        """)

        conn.commit()
        conn.close()

        log_info(SHEETS_LOG_FILE, "✅ База данных проверена или создана.")
        log_info(ROTATIONS_LOG_FILE, "✅ База данных проверена или создана.")

    except Exception as e:
        message = f"❌ Ошибка создания базы данных: {e}"
        log_error(SHEETS_LOG_FILE, message)
        log_error(ROTATIONS_LOG_FILE, message)
        send_telegram_message(message)
        raise


def list_tracked_documents() -> List[tuple]:
    """Возвращает список документов из TrackedTables."""
    return _fetch_all(
        query="SELECT label, table_type, spreadsheet_id FROM TrackedTables",
        error_log_file=SHEETS_LOG_FILE,
        notify_error=True
    )


def get_sheets_tasks() -> List[Dict[str, Any]]:
    """Возвращает все задачи из SheetsInfo."""
    return _fetch_all_dict(
        table_name="SheetsInfo",
        error_log_file=SHEETS_LOG_FILE
    )


def get_rotations_tasks() -> List[Dict[str, Any]]:
    """Возвращает все задачи из RotationsInfo."""
    return _fetch_all_dict(
        table_name="RotationsInfo",
        error_log_file=ROTATIONS_LOG_FILE
    )


# ─────── Служебные функции ───────

def _fetch_all(query: str, error_log_file: str, notify_error: bool = False) -> List[tuple]:
    """Выполняет SELECT-запрос и возвращает все результаты."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        return results
    except Exception as e:
        message = f"❌ Ошибка выполнения запроса: {e}"
        log_error(error_log_file, message)
        if notify_error:
            send_telegram_message(message)
        return []


def _fetch_all_dict(table_name: str, error_log_file: str) -> List[Dict[str, Any]]:
    """Возвращает все строки из таблицы в формате списка словарей."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        message = f"❌ Ошибка чтения таблицы {table_name}: {e}"
        log_error(error_log_file, message)
        send_telegram_message(message)
        return []
