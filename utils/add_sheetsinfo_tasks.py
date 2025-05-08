import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "../scheduler.db")

def add_sheetsinfo_tasks():
    """
    Adds new tasks to the SheetsInfo table for live88 and legendz.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    live88_tables = [
        'gARL1', 'swRL1', 'swBC1', 'gsRL1', 'gRL1', 'gRL2', 'tBJ1', 'tBJ2', 'tRL1', 
        'vBJ2', 'vBJ3', 'vBJ4', 'vBC2', 'vBC3', 'vBC4', 'vDT1', 'vHSB1', 'gBJ1', 
        'gBJ3', 'gBJ4', 'gBJ5', 'gBC1', 'gBC2', 'gBC3', 'gBC4', 'gBC5', 'gBC6', 
        'gRL3', 'gsBJ1', 'gsBJ2', 'gsBJ3', 'gsBJ4', 'gsBJ5'
    ]
    legendz_tables = ['lBJ1', 'lBJ2', 'lBJ3']

    try:
        # Insert live88 tasks
        for table in live88_tables:
            cursor.execute("""
            INSERT INTO SheetsInfo (
                is_active, name_of_process, source_table_type, source_page_name, 
                source_page_area, scan_group, last_scan, scan_interval, scan_quantity, 
                scan_failures, hash, process_data_method, values_json,
                target_table_type, target_page_name, target_page_area, update_group, 
                last_update, update_quantity, update_failures
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """, (
                1, f"live88_{table}", "live88_bs_game", table, "A2:I500", 
                "mistake_getting", None, 1800, 0, 0, None, None, None,
                "nothing", "nothing", "nothing", None, 
                None, 0, 0
            ))

        # Insert legendz tasks
        for table in legendz_tables:
            cursor.execute("""
            INSERT INTO SheetsInfo (
                is_active, name_of_process, source_table_type, source_page_name, 
                source_page_area, scan_group, last_scan, scan_interval, scan_quantity, 
                scan_failures, hash, process_data_method, values_json,
                target_table_type, target_page_name, target_page_area, update_group, 
                last_update, update_quantity, update_failures
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """, (
                1, f"legendz_{table}", "legendz_bs_game", table, "A2:I500", 
                "mistake_getting", None, 1800, 0, 0, None, None, None,
                "nothing", "nothing", "nothing", None, 
                None, 0, 0
            ))

        conn.commit()
        print("✅ Новые задачи успешно добавлены в SheetsInfo.")
    except Exception as e:
        print(f"❌ Ошибка при добавлении задач в SheetsInfo: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    add_sheetsinfo_tasks()
