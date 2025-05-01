import sqlite3
from config import DB_PATH

def clear_db(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE {table_name} SET last_scan = ?, hash = ?, values_json = ?, last_update = ?", ("NULL", "NULL", "NULL", "NULL",))
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

if __name__ == "__main__":
    # Пример использования функции
    clear_db(DB_PATH)
    print("Last scan updated successfully.")