import sqlite3
import pandas as pd

# Путь к базе данных и к CSV-файлу
db_path = "scheduler.db"
csv_path = "TrackedTables_export.csv"


def get():
    # Подключение к базе данных
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # ───── Создание таблицы (если ещё не создана) ─────
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS TrackedTables (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_type TEXT NOT NULL,
        label TEXT,
        spreadsheet_id TEXT NOT NULL,
        valid_from TEXT NOT NULL,
        valid_to TEXT
    );
    """)
    conn.commit()

    # ───── Загрузка данных из таблицы ─────
    df = pd.read_sql_query("SELECT * FROM TrackedTables", conn)

    # ───── Сохранение в CSV-файл ─────
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")  # encoding для совместимости с Excel

    # Закрытие соединения
    conn.close()

    print(f"Данные успешно экспортированы в файл: {csv_path}")


def clean():

    # Подключение к базе данных
    conn = sqlite3.connect(db_path)  # укажи путь к своей базе, если он другой
    cursor = conn.cursor()

    # Очистка поля hash во всех строках таблицы RotationsInfo
    cursor.execute("UPDATE RotationsInfo SET hash = NULL;")
    conn.commit()

    print("✅ Все значения hash в таблице RotationsInfo очищены.")


