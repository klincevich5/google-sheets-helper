import sqlite3
from config import DB_PATH

def update_last_scan(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE {table_name} SET last_scan = ?, hash = ?, values_json = ?", ("NULL", "NULL", "NULL",))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # Пример использования функции
    update_last_scan("SheetsInfo") 
    print("Last scan updated successfully.")