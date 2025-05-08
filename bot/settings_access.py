from database.db_access import get_connection

def is_scanner_enabled(scanner_name: str) -> bool:
    """
    Проверяет, включён ли сканер. По умолчанию — включён, если записи нет.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM BotSettings WHERE key = ?", (scanner_name,))
    row = cursor.fetchone()
    conn.close()
    return row is None or row[0] == '1'

def set_scanner_enabled(scanner_name: str, enabled: bool):
    """
    Устанавливает флаг включения или отключения сканера.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "REPLACE INTO BotSettings (key, value) VALUES (?, ?)",
        (scanner_name, '1' if enabled else '0')
    )
    conn.commit()
    conn.close()

def ensure_bot_settings_table():
    """
    Создаёт таблицу BotSettings, если её нет.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS BotSettings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()
