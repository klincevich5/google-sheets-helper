import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "../scheduler.db")

def add_last_row_column():
    """
    Adds the last_row column to the GameMistakes table if it doesn't exist.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE GameMistakes ADD COLUMN last_row INTEGER")
        conn.commit()
        print("✅ Колонка last_row успешно добавлена в таблицу GameMistakes.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("ℹ️ Колонка last_row уже существует в таблице GameMistakes.")
        else:
            print(f"❌ Ошибка при добавлении колонки last_row: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    add_last_row_column()
