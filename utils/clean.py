# utils/clean.py

import os
import sqlite3
from core.config import DB_PATH

def clear_db(table_name):
    print(f"Код запущен из директории: {os.getcwd()}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # cursor.execute(f"UPDATE {table_name} SET last_scan = ?, last_update = ?", ("NULL", "NULL",))
    cursor.execute(f"UPDATE {table_name} SET last_scan = ?, scan_quantity = ?, update_quantity = ?, scan_failures = ?, last_update = ?, update_failures = ?, hash = ?, values_json = ?", ("NULL", 0, 0, 0, "NULL", 0, "NULL", "NULL"))
    conn.commit()
    conn.close()

def set():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Обновляем source_page_area для всех записей с source_table_type = 'qa_list'
    cursor.execute("""
    UPDATE SheetsInfo
    SET is_active = 0
    WHERE process_data_method = 'process_default';
    """)

    conn.commit()
    conn.close()

def delete(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
    DELETE FROM {table_name}
                   
    WHERE name_of_process = 'live88_vBC2'
    """.format(table_name=table_name))
    conn.commit()
    conn.close()

def clear_feedback_storage():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM FeedbackStorage")
        conn.commit()
        print("🗑️ Таблица FeedbackStorage очищена.")
    except Exception as e:
        print(f"❌ Ошибка при очистке FeedbackStorage: {e}")
    finally:
        conn.close()

# Вызов

if __name__ == "__main__":
    
    # clear_db("SheetsInfo")
    # clear_db("RotationsInfo")
    # delete("SheetsInfo")
    clear_feedback_storage()
    # set()