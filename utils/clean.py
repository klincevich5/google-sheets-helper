import sqlite3
# from core.config import DB_PATH

import csv

DB_PATH = "scheduler.db"

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


def set():

    db_path = "scheduler.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Обновляем source_page_area для всех записей с source_table_type = 'qa_list'
    cursor.execute("""
    UPDATE TrackedTables
    SET spreadsheet_id = '14q-CItoITjj2L-VdyTkhPy2umyPz9oeUzNJzHyMJLho'
    WHERE label = 'Bonus April 2025';
    """)

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    conn.close()

def delete_trackedtables_records():
    """
    Удаляет записи из таблицы TrackedTables с id >= 26.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM TrackedTables WHERE id >= ?", (26,))
        conn.commit()
        print(f"✅ Успешно удалены записи из TrackedTables с id >= 26")
    except Exception as e:
        print(f"❌ Ошибка при удалении записей из TrackedTables: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    
    # clear_db("SheetsInfo")
    # clear_db("RotationsInfo")
    # remove()
    # set()
    delete_trackedtables_records()