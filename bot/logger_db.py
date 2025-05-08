# logger_db.py
import sqlite3
from core.config import DB_PATH
from datetime import datetime

def log_to_db(scanner, phase, level, message):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()

    cursor.execute('''
        INSERT INTO ScannerLogs (timestamp, scanner, phase, level, message)
        VALUES (?, ?, ?, ?, ?)
    ''', (timestamp, scanner, phase, level, message))

    conn.commit()
    conn.close()
