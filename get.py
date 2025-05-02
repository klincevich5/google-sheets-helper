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


def add_may():

    db_path = "scheduler.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    updates = {
        14: "1DzSJqySS2J9GvuNcJKvNT00kJ8QBLYnUPV4frAhe7wU",
        15: "1oUW76fxFzi4hhOV3xykve4Ulvg4E1WhpwKsSbWPQ1LY",
        16: "1_fZcCl1uDaFDmjORM5e7x4lEDDyJRJlOYzJS6UtDcrU",
        17: "1VsecS-VEjqJDsltkJRtWJynni93REi1AgzqGSnGpGcU",
        18: "10BCYZ-SMEoBRtfqDzZ6mYB8Wr7J68LI2lnkBRWuf4y8",
        19: "1QZi0tnOAusGaa9stIaL5m6VCaeq4Ts4rkt6P2e2pV1g",
        20: "1VxV8aCYSD3yULFalDEZNqYGBHX0yEIKNZnRjXeZyRvA",
        21: "1XKgKGjs9lILXSaWuwVaKBTNFydDCHTpk_4bubd22GDc",
        22: "1dJQAVqLn6v6Oo1ZbFfIHWSWupvDB_wFwJCUp_XhSD6o",
        23: "1nhYm71aSjDhSbTwt_mZqvYmbYxFjN2EsRSHihd3uuvI",
        24: "1wkJg7pio8KdNJOlOcl4BLuzF38AJlaQ0omNVZERPj",
        25: "1SJ-r29TDQpY2wUpkrHkoKzeHHcNcsSq4oL0oC2Plw94",
    }

    for row_id, spreadsheet_id in updates.items():
        cursor.execute("""
            UPDATE TrackedTables
            SET spreadsheet_id = ?
            WHERE id = ?;
        """, (spreadsheet_id, row_id))

    conn.commit()
    conn.close()

def fix():

    db_path = "scheduler.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE SheetsInfo
        SET source_page_name = 'Info'
        WHERE source_table_type = 'feedbacks';
    """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    # Пример использования функции
    fix()
    print("✅ Данные успешно обновлены.")