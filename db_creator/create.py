import sqlite3

db_path = "scheduler.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# ───────────── TrackedTables ─────────────
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

# ───────────── SheetsInfo (статические процессы) ─────────────
cursor.execute("""
CREATE TABLE IF NOT EXISTS SheetsInfo (
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
    need_update BOOLEAN DEFAULT 0
);
""")

# ───────────── RotationsInfo (динамические процессы) ─────────────
cursor.execute("""
CREATE TABLE IF NOT EXISTS RotationsInfo (
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
    need_update BOOLEAN DEFAULT 0
);
""")

# Сохранение и закрытие соединения
conn.commit()
conn.close()

print("База данных scheduler.db успешно создана.")