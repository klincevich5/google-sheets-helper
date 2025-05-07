# database.py

import sqlite3
from config import DB_PATH
from datetime import datetime

def log_to_db(conn, scanner, phase, level, message):
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO Logs (timestamp, scanner, phase, level, message)
    VALUES (?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        scanner,
        phase,
        level,
        message
    ))
    conn.commit()


def ensure_logs_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        scanner TEXT,
        phase TEXT,
        level TEXT,
        message TEXT
    )
    """)
    conn.commit()

def connect_to_db(DB_PATH):
    """Подключение к базе данных SQLite с поддержкой многопоточности."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # Чтобы можно было обращаться по имени столбцов
    return conn


def create_scanner_logs_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ScannerLogs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            scanner TEXT NOT NULL,
            phase TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL 
        );
    """)
    conn.commit()
    conn.close()

#############################################################################
# Таблица: ApiUsage (использование API)
###############################################################################

def create_api_usage_table():
    conn = connect_to_db(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ApiUsage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            token TEXT NOT NULL,
            counter INTEGER DEFAULT 0,
            info_scan_group TEXT,
            info_update_group TEXT,
            success BOOLEAN DEFAULT 1
        )
    """)
    conn.commit()
    conn.close()

#############################################################################
# Функция для вставки данных в таблицу ApiUsage
###############################################################################

def insert_usage(token, count, scan_group=None, update_group=None, success=True):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO ApiUsage (date, token, counter, info_scan_group, info_update_group, success)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (timestamp, token, count, scan_group, update_group, int(success)))
    conn.commit()
    conn.close()