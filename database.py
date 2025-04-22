# database.py

import sqlite3
from datetime import datetime, timedelta
from config import DB_PATH, WARSAW_TZ

def get_doc_id_map():
    """
    Возвращает словарь {source_table_type: spreadsheet_id}
    Только для документов, актуальных по дате.
    """
    today = datetime.now(WARSAW_TZ).date()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT table_type, spreadsheet_id, valid_from, valid_to FROM TrackedTables")
    rows = cursor.fetchall()
    conn.close()

    result = {}
    for row in rows:
        start = parse_ddmmyyyy(row["valid_from"])
        end = parse_ddmmyyyy(row["valid_to"]) if row["valid_to"] else None
        if start and start <= today and (not end or today <= end):
            result[row["table_type"]] = row["spreadsheet_id"]
    return result

def get_pending_scans(table_name: str):
    now = datetime.now(WARSAW_TZ)  # текущее локальное время

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    rows = cursor.execute(f"SELECT * FROM {table_name}").fetchall()
    pending = []

    for row in rows:
        interval_raw = row["scan_interval"]
        if interval_raw is None or str(interval_raw).strip() == "":
            continue

        try:
            interval = int(interval_raw)
        except Exception:
            continue

        raw_last = row["last_scan"]
        if not raw_last:  # если даты нет — нужно отсканировать
            pending.append(dict(row))
            continue

        try:
            last = datetime.fromisoformat(raw_last).astimezone(WARSAW_TZ)
        except Exception:
            last = datetime.min.replace(tzinfo=WARSAW_TZ)

        if now - last >= timedelta(seconds=interval):
            pending.append(dict(row))

    conn.close()
    return pending

def parse_ddmmyyyy(date_str):
    try:
        return datetime.strptime(date_str, "%d.%m.%Y").date()
    except Exception:
        return None

def log_scan_groups(table_name: str, log_file: str, group_field: str = "scan_group"):
    now = datetime.now(WARSAW_TZ)
    now_str = now.strftime("[%Y-%m-%d %H:%M:%S]")
    today = now.date()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Получаем карту: source_table_type → spreadsheet_id (если актуален по дате)
    cursor.execute("SELECT table_type, spreadsheet_id, valid_from, valid_to FROM TrackedTables")
    tracked_rows = cursor.fetchall()
    id_map = {}
    for row in tracked_rows:
        start = parse_ddmmyyyy(row["valid_from"])
        end = parse_ddmmyyyy(row["valid_to"]) if row["valid_to"] else None
        if start and start <= today and (not end or today <= end):
            id_map[row["table_type"]] = row["spreadsheet_id"]

    # 2. Получаем все процессы с group_field
    cursor.execute(f"""
        SELECT id, {group_field}, source_table_type,
               source_page_name, source_page_area, get_data_method
        FROM {table_name}
        WHERE {group_field} IS NOT NULL AND TRIM({group_field}) != ''
    """)
    rows = cursor.fetchall()
    conn.close()

    # 3. Группировка
    groups = {}
    for row in rows:
        group = row[group_field]
        if group not in groups:
            groups[group] = []
        groups[group].append(row)

    # 4. Логирование
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{now_str} 🔍 Анализ групп {group_field} в {table_name}:\n")
        if not groups:
            f.write(f"{now_str} ❗ Нет ни одной группы ({group_field})\n\n")
            return

        for group, items in groups.items():
            f.write(f"{now_str} 📦 {group_field}: {group} ({len(items)} элементов)\n")
            missing = 0
            grouped_by_doc = {}

            for row in items:
                doc_id = id_map.get(row["source_table_type"])
                if not doc_id:
                    missing += 1
                range_str = f"{row['source_page_name']}!{row['source_page_area']}"
                grouped_by_doc.setdefault(doc_id, set()).add(range_str)

                f.write(
                    f"{now_str}   ├─ ID={row['id']} | 📄 {row['source_table_type']} | "
                    f"📑 {row['source_page_name']} | 🔲 {row['source_page_area']} | "
                    f"⚙ {row['get_data_method']} | 🆔 {doc_id or '—'}\n"
                )

            if missing:
                f.write(f"{now_str} ⚠️  Пропущено {missing} строк — не найден spreadsheet_id в TrackedTables\n")

            for doc_id, ranges in grouped_by_doc.items():
                if not doc_id:
                    continue
                f.write(f"{now_str} 🔄 Пример batchGet для ID={doc_id}:\n")
                f.write(f"{now_str}   ranges = [\n")
                for r in sorted(ranges):
                    f.write(f"{now_str}     '{r}',\n")
                f.write(f"{now_str}   ]\n")
            f.write("\n")

def check_db_integrity():
    required_tables = ["TrackedTables", "SheetsInfo", "RotationsInfo"]
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing = [row[0] for row in cursor.fetchall()]
    conn.close()

    missing = [tbl for tbl in required_tables if tbl not in existing]
    if missing:
        raise Exception(f"❌ Missing tables: {', '.join(missing)}")

def list_tracked_documents():
    today = datetime.now(WARSAW_TZ).date()  # правильно получаем польскую дату

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT table_type, label, spreadsheet_id, valid_from, valid_to FROM TrackedTables")
    rows = cursor.fetchall()
    conn.close()

    docs = []
    for row in rows:
        table_type, label, spreadsheet_id, valid_from_raw, valid_to_raw = row
        valid_from = parse_ddmmyyyy(valid_from_raw)
        valid_to = parse_ddmmyyyy(valid_to_raw) if valid_to_raw else None

        if valid_from and valid_from <= today and (not valid_to or today <= valid_to):
            docs.append((table_type, label, spreadsheet_id))

    return docs

def update_last_scan(table_name: str, process_id: int):
    now_local = datetime.now(WARSAW_TZ).isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE {table_name} SET last_scan = ? WHERE id = ?", (now_local, process_id))
    conn.commit()
    conn.close()