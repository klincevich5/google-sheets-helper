import sqlite3
import pandas as pd

# Путь к SQLite-базе
db_path = "scheduler.db"
conn = sqlite3.connect(db_path)

# Загружаем TrackedTables.csv
try:
    df_tracked = pd.read_csv("TrackedTables.csv")
    df_tracked.to_sql("TrackedTables", conn, if_exists="replace", index=False)
    print(f"✅ TrackedTables загружена: {len(df_tracked)} строк")
except Exception as e:
    print(f"⚠️ Ошибка при загрузке TrackedTables: {e}")

# Загружаем SheetsInfo.csv
try:
    df_sheets = pd.read_csv("SheetsInfo.csv")
    df_sheets.to_sql("SheetsInfo", conn, if_exists="replace", index=False)
    print(f"✅ SheetsInfo загружена: {len(df_sheets)} строк")
except Exception as e:
    print(f"⚠️ Ошибка при загрузке SheetsInfo: {e}")

# Загружаем RotationsInfo.csv
try:
    df_rotations = pd.read_csv("RotationsInfo.csv")
    df_rotations.to_sql("RotationsInfo", conn, if_exists="replace", index=False)
    print(f"✅ RotationsInfo загружена: {len(df_rotations)} строк")
except Exception as e:
    print(f"⚠️ Ошибка при загрузке RotationsInfo: {e}")

conn.commit()
conn.close()
print("🎉 Все данные успешно загружены в базу данных.")


# Таблица: TrackedTables
# Столбцы:
#   - id (INTEGER) PRIMARY KEY
#   - table_type (TEXT) NOT NULL
#   - label (TEXT)
#   - spreadsheet_id (TEXT) NOT NULL
#   - valid_from (TEXT) NOT NULL
#   - valid_to (TEXT)

# Таблица: sqlite_sequence
# Столбцы:
#   - name ()
#   - seq ()

# Таблица: SheetsInfo
# Столбцы:
#   - id (INTEGER) PRIMARY KEY
#   - name_of_process (TEXT)
#   - source_table_type (TEXT) NOT NULL
#   - source_page_name (TEXT) NOT NULL
#   - source_page_area (TEXT) NOT NULL
#   - scan_group (TEXT)
#   - last_scan (TEXT)
#   - scan_interval (INTEGER) DEFAULT 1800
#   - scan_quantity (INTEGER) DEFAULT 0
#   - scan_failures (INTEGER) DEFAULT 0
#   - hash (TEXT)
#   - process_data_method (TEXT) DEFAULT 'process_default'
#   - values_json (TEXT)
#   - target_table_type (TEXT) NOT NULL
#   - target_page_name (TEXT) NOT NULL
#   - target_page_area (TEXT) NOT NULL
#   - update_group (TEXT)
#   - last_update (TEXT)
#   - update_quantity (INTEGER) DEFAULT 0
#   - update_failures (INTEGER) DEFAULT 0
#   - need_update (BOOLEAN) DEFAULT 0
#   - is_active (INTEGER) DEFAULT 1

# Таблица: RotationsInfo
# Столбцы:
#   - id (INTEGER) PRIMARY KEY
#   - name_of_process (TEXT)
#   - source_table_type (TEXT) NOT NULL
#   - source_page_name (TEXT) NOT NULL
#   - source_page_area (TEXT) NOT NULL
#   - scan_group (TEXT)
#   - last_scan (TEXT)
#   - scan_interval (INTEGER) DEFAULT 60
#   - scan_quantity (INTEGER) DEFAULT 0
#   - scan_failures (INTEGER) DEFAULT 0
#   - hash (TEXT)
#   - process_data_method (TEXT) DEFAULT 'process_default'
#   - values_json (TEXT)
#   - target_table_type (TEXT) NOT NULL
#   - target_page_name (TEXT) NOT NULL
#   - target_page_area (TEXT) NOT NULL
#   - update_group (TEXT)
#   - last_update (TEXT)
#   - update_quantity (INTEGER) DEFAULT 0
#   - update_failures (INTEGER) DEFAULT 0
#   - need_update (BOOLEAN) DEFAULT 0
#   - is_active (INTEGER) DEFAULT 1

# Таблица: BotSettings
# Столбцы:
#   - key (TEXT) PRIMARY KEY
#   - value (TEXT)

# Таблица: ScannerLogs
# Столбцы:
#   - id (INTEGER) PRIMARY KEY
#   - timestamp (TEXT) NOT NULL
#   - scanner (TEXT) NOT NULL
#   - phase (TEXT) NOT NULL
#   - level (TEXT) NOT NULL
#   - message (TEXT) NOT NULL

