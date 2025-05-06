import sqlite3
from config import DB_PATH

import csv
def clear_db(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # cursor.execute(f"UPDATE {table_name} SET last_scan = ?, last_update = ?", ("NULL", "NULL",))
    cursor.execute(f"UPDATE {table_name} SET last_scan = ?, scan_quantity = ?, update_quantity = ?, scan_failures = ?, last_update = ?, update_failures = ?, hash = ?, values_json = ?", ("NULL", 0, 0, 0, "NULL", 0, "NULL", "NULL"))
    conn.commit()
    conn.close()

def remove():
    query = """
        UPDATE {DB_PATH} SET
            source_page_area = ?, target_page_area = ?
        WHERE id = ?
    """
    values = (
        "C1:C300", "A1:A300",  # Замените на нужные значения
        22,  # ID задачи, которую нужно обновить
    )
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()
    conn.close()


def add():
    import sqlite3

    db_path = "scheduler.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Добавляем колонку is_active в SheetsInfo, если она ещё не существует
    cursor.execute("PRAGMA table_info(SheetsInfo)")
    sheets_columns = [col[1] for col in cursor.fetchall()]
    if "is_active" not in sheets_columns:
        cursor.execute("ALTER TABLE SheetsInfo ADD COLUMN is_active INTEGER DEFAULT 1")

    # Добавляем колонку is_active в RotationsInfo, если она ещё не существует
    cursor.execute("PRAGMA table_info(RotationsInfo)")
    rotations_columns = [col[1] for col in cursor.fetchall()]
    if "is_active" not in rotations_columns:
        cursor.execute("ALTER TABLE RotationsInfo ADD COLUMN is_active INTEGER DEFAULT 1")

    # Сохраняем изменения и закрываем соединение
    conn.commit()
    conn.close()

    print("Колонки is_active успешно добавлены в таблицы SheetsInfo и RotationsInfo.")

def get_db_structure(db_path):
    """
    Получает структуру базы данных SQLite, включая таблицы и их столбцы.

    :param db_path: Путь к базе данных SQLite
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Получаем список всех таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    print("Структура базы данных:")
    for table in tables:
        table_name = table[0]
        print(f"\nТаблица: {table_name}")

        # Получаем информацию о столбцах таблицы
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        print("Столбцы:")
        for column in columns:
            col_id, col_name, col_type, not_null, default_value, pk = column
            print(f"  - {col_name} ({col_type}){' NOT NULL' if not_null else ''}{' PRIMARY KEY' if pk else ''}{f' DEFAULT {default_value}' if default_value else ''}")

    conn.close()


def create_scheduler2_db():
    # Создаем новую базу данных scheduler2.db
    new_db_path = "scheduler2.db"
    conn = sqlite3.connect(new_db_path)
    cursor = conn.cursor()

    # Создаем таблицы с измененной структурой
    cursor.execute("""
    CREATE TABLE TrackedTables (
        id INTEGER PRIMARY KEY,
        table_type TEXT NOT NULL,
        label TEXT,
        spreadsheet_id TEXT NOT NULL,
        valid_from TEXT NOT NULL,
        valid_to TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE SheetsInfo (
        id INTEGER PRIMARY KEY,
        is_active INTEGER DEFAULT 1,
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
        update_failures INTEGER DEFAULT 0
    )
    """)

    cursor.execute("""
    CREATE TABLE RotationsInfo (
        id INTEGER PRIMARY KEY,
        is_active INTEGER DEFAULT 1,
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
        update_failures INTEGER DEFAULT 0
    )
    """)

    cursor.execute("""
    CREATE TABLE BotSettings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE ScannerLogs (
        id INTEGER PRIMARY KEY,
        timestamp TEXT NOT NULL,
        scanner TEXT NOT NULL,
        phase TEXT NOT NULL,
        level TEXT NOT NULL,
        message TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()
def migrate_data():
    # Подключаемся к оригинальной базе данных
    old_db_path = "scheduler.db"
    new_db_path = "scheduler2.db"

    old_conn = sqlite3.connect(old_db_path)
    old_cursor = old_conn.cursor()

    new_conn = sqlite3.connect(new_db_path)
    new_cursor = new_conn.cursor()

    # Перенос данных из TrackedTables
    old_cursor.execute("SELECT * FROM TrackedTables")
    rows = old_cursor.fetchall()
    new_cursor.executemany("INSERT INTO TrackedTables VALUES (?, ?, ?, ?, ?, ?)", rows)

    # Перенос данных из SheetsInfo
    old_cursor.execute("""
    SELECT 
        id, is_active, name_of_process, source_table_type, source_page_name, source_page_area, 
        scan_group, last_scan, scan_interval, scan_quantity, scan_failures, hash, 
        process_data_method, values_json, target_table_type, target_page_name, target_page_area, 
        update_group, last_update, update_quantity, update_failures
    FROM SheetsInfo
    """)
    rows = old_cursor.fetchall()
    new_cursor.executemany("""
    INSERT INTO SheetsInfo (
        id, is_active, name_of_process, source_table_type, source_page_name, source_page_area, 
        scan_group, last_scan, scan_interval, scan_quantity, scan_failures, hash, 
        process_data_method, values_json, target_table_type, target_page_name, target_page_area, 
        update_group, last_update, update_quantity, update_failures
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    # Перенос данных из RotationsInfo
    old_cursor.execute("""
    SELECT 
        id, is_active, name_of_process, source_table_type, source_page_name, source_page_area, 
        scan_group, last_scan, scan_interval, scan_quantity, scan_failures, hash, 
        process_data_method, values_json, target_table_type, target_page_name, target_page_area, 
        update_group, last_update, update_quantity, update_failures
    FROM RotationsInfo
    """)
    rows = old_cursor.fetchall()
    new_cursor.executemany("""
    INSERT INTO RotationsInfo (
        id, is_active, name_of_process, source_table_type, source_page_name, source_page_area, 
        scan_group, last_scan, scan_interval, scan_quantity, scan_failures, hash, 
        process_data_method, values_json, target_table_type, target_page_name, target_page_area, 
        update_group, last_update, update_quantity, update_failures
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    # Перенос данных из BotSettings
    old_cursor.execute("SELECT * FROM BotSettings")
    rows = old_cursor.fetchall()
    new_cursor.executemany("INSERT INTO BotSettings VALUES (?, ?)", rows)

    # Перенос данных из ScannerLogs
    old_cursor.execute("SELECT * FROM ScannerLogs")
    rows = old_cursor.fetchall()
    new_cursor.executemany("INSERT INTO ScannerLogs VALUES (?, ?, ?, ?, ?, ?)", rows)

    # Сохраняем изменения и закрываем соединения
    new_conn.commit()
    old_conn.close()
    new_conn.close()

def qqqq():

    # Путь к базе данных
    db_path = "scheduler.db"
    # Путь к файлу для сохранения
    csv_path = "TrackedTables_export.csv"

    # Подключение к БД
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Извлечение данных
    cursor.execute("SELECT * FROM TrackedTables")
    rows = cursor.fetchall()

    # Получение названий столбцов
    column_names = [description[0] for description in cursor.description]

    # Запись в CSV
    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_names)  # заголовки
        writer.writerows(rows)         # строки

    conn.close()
    print(f"✅ Таблица TrackedTables успешно экспортирована в {csv_path}")

def set():

    db_path = "scheduler.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Обновляем source_page_area для всех записей с source_table_type = 'qa_list'
    cursor.execute("""
    UPDATE TrackedTables
    SET spreadsheet_id = '1wkJg7pio8KdNJOlOcl4BLuzF38AJlaQ0omNVZERPj-Y'
    WHERE label = 'AquaRL Rotation May 2025';
    """)

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    conn.close()

if __name__ == "__main__":
    
    db_path = "scheduler.db" 
    # get_db_structure(db_path)
    # create_scheduler2_db()
    # migrate_data()
    # Пример использования функции
    clear_db("SheetsInfo")
    clear_db("RotationsInfo")
    # print("Last scan updated successfully.")
    # set()
    # qqqq()