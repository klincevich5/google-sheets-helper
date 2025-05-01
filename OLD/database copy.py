# database.py

import sqlite3

def connect_to_db(DB_PATH):
    """Подключение к базе данных SQLite с поддержкой многопоточности."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # Чтобы можно было обращаться по имени столбцов
    return conn
