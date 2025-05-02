import sqlite3
from config import DB_PATH

def clear_db(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # cursor.execute(f"UPDATE {table_name} SET last_scan = ?, last_update = ?", ("NULL", "NULL",))
    cursor.execute(f"UPDATE {table_name} SET last_scan = ?, hash = ?, values_json = ?, last_update = ?, update_failures = ?, update_quantity =?", ("NULL", "NULL", "NULL", "NULL", 0, 0,))
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
    UPDATE RotationsInfo
    SET source_page_area = 'G1:Y300'
    WHERE source_table_type = 'qa_list';
    """)

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    conn.close()


if __name__ == "__main__":
    # Пример использования функции
    clear_db("SheetsInfo")
    clear_db("RotationsInfo")
    print("Last scan updated successfully.")
    set()