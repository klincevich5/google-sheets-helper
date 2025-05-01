import sqlite3

db_path = "scheduler.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# ─────── Пересоздание SheetsInfo ───────
cursor.execute("""
CREATE TABLE IF NOT EXISTS SheetsInfo_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    update_failures INTEGER DEFAULT 0,
    need_update BOOLEAN DEFAULT 0  -- добавлена колонка need_update
);
""")

cursor.execute("""
INSERT INTO SheetsInfo_new (
    id, name_of_process, source_table_type, source_page_name, source_page_area,
    scan_group, last_scan, scan_interval, scan_quantity, scan_failures, hash,
    process_data_method, values_json, target_table_type, target_page_name, target_page_area,
    update_group, last_update, update_quantity, update_failures
)
SELECT
    id, name_of_process, source_table_type, source_page_name, source_page_area,
    scan_group, last_scan, scan_interval, scan_quantity, scan_failures, hash,
    process_data_method, values_json, target_table_type, target_page_name, target_page_area,
    update_group, last_update, update_quantity, update_failures
FROM SheetsInfo;
""")

cursor.execute("DROP TABLE SheetsInfo;")
cursor.execute("ALTER TABLE SheetsInfo_new RENAME TO SheetsInfo;")

# ─────── Пересоздание RotationsInfo ───────
cursor.execute("""
CREATE TABLE IF NOT EXISTS RotationsInfo_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    update_failures INTEGER DEFAULT 0,
    need_update BOOLEAN DEFAULT 0  -- для совместимости (у тебя уже была)
);
""")

cursor.execute("""
INSERT INTO RotationsInfo_new (
    id, name_of_process, source_table_type, source_page_name, source_page_area,
    scan_group, last_scan, scan_interval, scan_quantity, scan_failures, hash,
    process_data_method, values_json, target_table_type, target_page_name, target_page_area,
    update_group, last_update, update_quantity, update_failures, need_update
)
SELECT
    id, name_of_process, source_table_type, source_page_name, source_page_area,
    scan_group, last_scan, scan_interval, scan_quantity, scan_failures, hash,
    process_data_method, values_json, target_table_type, target_page_name, target_page_area,
    update_group, last_update, update_quantity, update_failures, need_update
FROM RotationsInfo;
""")

cursor.execute("DROP TABLE RotationsInfo;")
cursor.execute("ALTER TABLE RotationsInfo_new RENAME TO RotationsInfo;")

# ─────── Сохраняем и закрываем ───────
conn.commit()
conn.close()

print("SheetsInfo и RotationsInfo успешно пересозданы без лишних колонок. В SheetsInfo добавлен need_update.")
