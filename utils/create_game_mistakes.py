import sqlite3
from core.config import DB_PATH  # Убедись, что DB_PATH указывает на правильную БД

def recreate_game_mistakes_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("DROP TABLE IF EXISTS MistakeStorage")
        print("🗑️ Таблица MistakeStorage удалена.")

        cursor.execute("""
        CREATE TABLE MistakeStorage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            floor TEXT NOT NULL,
            table_name TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            game_id TEXT NOT NULL,
            mistake TEXT NOT NULL,
            type TEXT NOT NULL,
            is_cancel INTEGER NOT NULL DEFAULT 0,
            dealer TEXT,
            sm TEXT,
            last_row INTEGER
        )

        """)
        conn.commit()
        print("✅ Таблица MistakeStorage создана заново с полем 'floor'.")
    except Exception as e:
        print(f"❌ Ошибка при пересоздании таблицы MistakeStorage: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    recreate_game_mistakes_table()