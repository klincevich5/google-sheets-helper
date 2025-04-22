import time
from collections import defaultdict
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from database import get_pending_scans, log_scan_groups, get_doc_id_map
from logger import log_to_file
from config import SHEETS_LOG_FILE, SCAN_INTERVAL_SECONDS, WARSAW_TZ, DB_PATH
from datetime import datetime
import json
import hashlib
import traceback
import sqlite3

TOKEN_PATH = "token.json"

# ────────────────────────────────
# AUTHENTICATION
# ────────────────────────────────

def load_credentials(token_path: str):
    creds = Credentials.from_authorized_user_file(token_path)
    service = build("sheets", "v4", credentials=creds)
    return service.spreadsheets()

# ────────────────────────────────
# ЗАДАЧИ НА СКАНИРОВАНИЕ
# ────────────────────────────────

def filter_valid_tasks(tasks, doc_id_map):
    updated_tasks = []
    for task in tasks:
        source_type = task.get("source_table_type")
        source_doc_id = doc_id_map.get(source_type)
        if not source_doc_id:
            log_to_file(SHEETS_LOG_FILE, f"⚠️ Пропуск задачи без source_doc_id: ID={task.get('id')}, type={source_type}")
            continue

        target_type = task.get("target_table_type")
        target_doc_id = doc_id_map.get(target_type)
        if not target_doc_id:
            log_to_file(SHEETS_LOG_FILE, f"⚠️ Пропуск задачи без target_doc_id: ID={task.get('id')}, type={target_type}")
            continue

        task["source_doc_id"] = source_doc_id
        task["target_doc_id"] = target_doc_id
        updated_tasks.append(task)
    return updated_tasks

# ────────────────────────────────
# ОБРАБОТКА ДАННЫХ
# ────────────────────────────────

def process_data_by_method(method: str, values: list) -> tuple[str, str]:
    values_json = json.dumps(values, ensure_ascii=False)
    new_hash = hashlib.md5(values_json.encode()).hexdigest()
    return values_json, new_hash

# ────────────────────────────────
# ОБНОВЛЕНИЕ TASK В БАЗЕ
# ────────────────────────────────
def update_sheet_task_data(task):
    query = """
        UPDATE SheetsInfo SET
            last_scan = ?,
            scan_quantity = ?,
            scan_failures = ?,
            hash = ?,
            values_json = ?
        WHERE id = ?
    """
    now_local = datetime.now(WARSAW_TZ).isoformat()
    values = (
        task.get("last_scan", now_local),
        task.get("scan_quantity", 0),
        task.get("scan_failures", 0),
        task.get("hash"),
        task.get("values_json"),
        task.get("id")
    )
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()
    conn.close()

def update_sheet_import_data(task):
    query = """
        UPDATE SheetsInfo SET
            last_update = ?,
            update_quantity = ?,
            update_failures = ?
        WHERE id = ?
    """
    now_local = datetime.now(WARSAW_TZ).isoformat()
    values = (
        task.get("last_update", now_local),
        task.get("update_quantity", 0),
        task.get("update_failures", 0),
        task.get("id")
    )
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()
    conn.close()

# ────────────────────────────────
# ПОЛУЧЕНИЕ ДАННЫХ ИЗ SHEETS
# ────────────────────────────────

def get_group_ranges(group_tasks):
    range_to_tasks = defaultdict(list)
    for task in group_tasks:
        a1_range = f"'{task['source_page_name']}'!{task['source_page_area']}"
        range_to_tasks[a1_range].append(task)
    return range_to_tasks

def fetch_data_from_sheet(sheet, doc_id, unique_ranges):
    try:
        response = sheet.values().batchGet(spreadsheetId=doc_id, ranges=unique_ranges).execute()
        return response.get("valueRanges", [])
    except HttpError as e:
        log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка получения данных из документа id: {doc_id}: {str(e)}")
        return None

# ────────────────────────────────
# ОБРАБОТКА ПОЛУЧЕННЫХ ДАННЫХ
# ────────────────────────────────

def handle_fetched_data(value_ranges, range_to_tasks, changed_update_groups):
    for requested_range, value_range in zip(range_to_tasks.keys(), value_ranges):
        values = value_range.get("values", [])
        linked_tasks = range_to_tasks[requested_range]

        for task in linked_tasks:
            try:
                now_str = datetime.now(WARSAW_TZ).isoformat()
                if not values:
                    task["scan_failures"] = task.get("scan_failures", 0) + 1
                    log_to_file(SHEETS_LOG_FILE, f"  ❌ ID={task['id']} | Нет данных в диапазоне {requested_range}")
                else:
                    task["fetched_values"] = values
                    log_to_file(SHEETS_LOG_FILE, f"  ✅ ID={task['id']} | Диапазон: {requested_range} | Строк: {len(values)}")

                    method = task.get("process_data_method", "process_default")
                    values_json, new_hash = process_data_by_method(method, values)

                    old_hash = task.get("hash")
                    task["last_scan"] = now_str
                    task["scan_quantity"] = task.get("scan_quantity", 0) + 1

                    if new_hash == old_hash:
                        log_to_file(SHEETS_LOG_FILE, f"  ↪️ ID={task['id']} | Хэш не изменился")
                    else:
                        task["hash"] = new_hash
                        task["values_json"] = values_json

                        update_group = task.get("update_group")
                        if update_group:
                            changed_update_groups.append(update_group)
                            log_to_file(SHEETS_LOG_FILE, f"  🔁 ID={task['id']} | Изменение данных | update_group={update_group}")

                update_sheet_task_data(task)

            except Exception as task_err:
                log_to_file(SHEETS_LOG_FILE, f"  ⚠️ Ошибка при обработке задачи ID={task.get('id')}: {str(task_err)}\n{traceback.format_exc()}")

# ────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ────────────────────────────────

def SheetsInfo_scanner():
    log_to_file(SHEETS_LOG_FILE, "🟢 SheetsInfo_scanner запущен.")
    log_scan_groups("SheetsInfo", SHEETS_LOG_FILE, group_field="scan_group")

    while True:
        doc_id_map = get_doc_id_map()
        log_to_file(SHEETS_LOG_FILE, f"📊 Получено {len(doc_id_map)} актуальных документов для сканирования.")
        if not doc_id_map:
            log_to_file(SHEETS_LOG_FILE, "⚠️ Нет актуальных документов для сканирования.")
            return

        tasks = get_pending_scans("SheetsInfo")
        if not tasks:
            log_to_file(SHEETS_LOG_FILE, f"⏳ Нет задач на {datetime.now(WARSAW_TZ).strftime('%H:%M:%S')}")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue

        tasks_for_scan = filter_valid_tasks(tasks, doc_id_map)
        scan_groups = set(task["scan_group"] for task in tasks_for_scan if task.get("scan_group"))

        sheet = load_credentials(TOKEN_PATH)
        changed_update_groups = []

        for group in scan_groups:
            group_tasks = [task for task in tasks_for_scan if task.get("scan_group") == group]
            if not group_tasks:
                continue

            print(f"\n🔷 Обработка группы: {group} | Задач: {len(group_tasks)}")
            doc_id = group_tasks[0].get("source_doc_id")
            if not doc_id:
                print(f"❌ Нет source_doc_id в группе: {group}")
                continue

            range_to_tasks = get_group_ranges(group_tasks)
            unique_ranges = list(range_to_tasks.keys())
            print(f"  📊 Уникальных диапазонов: {len(unique_ranges)}")
            value_ranges = fetch_data_from_sheet(sheet, doc_id, unique_ranges)
            if value_ranges is None:
                continue

            handle_fetched_data(value_ranges, range_to_tasks, changed_update_groups)

        changed_update_groups = list(set(changed_update_groups))
        for group in changed_update_groups:
            print(f"🔁 Изменённая группа обновления: {group}")

        print("\n📋 Задачи по изменённым группам:")
        for group in changed_update_groups:
            group_tasks = [task for task in tasks_for_scan if task.get("update_group") == group]
            print(f"\n🔹 update_group: {group} ({len(group_tasks)} задач)")
            for row in group_tasks:
                print(f"  ➤ ID={row['id']} | {row['name_of_process']} → {row['target_page_name']}")

            try:
                # ────────────────────────────────
                # Подготовка batchUpdate
                print("  📥 Подготовка batchUpdate:")
                target_doc_id = group_tasks[0]["target_doc_id"]
                data_to_import = []
                for task in group_tasks:
                    import_method = task.get("import_data_method", "import_default")
                    values = json.loads(task.get("values_json") or "[]")
                    data_to_import.append({
                        "range": f"'{task['target_page_name']}'!{task['target_page_area']}",
                        "values": values
                    })

                if not data_to_import:
                    continue

                sheet.values().batchUpdate(
                    spreadsheetId=target_doc_id,
                    body={"valueInputOption": "RAW", "data": data_to_import}
                ).execute()

                # ────────────────────────────────
                # Успешный импорт — обновляем задачи
                now_str = datetime.now(WARSAW_TZ).isoformat()
                for task in group_tasks:
                    task["last_update"] = now_str
                    task["update_quantity"] = task.get("update_quantity", 0) + 1
                    update_sheet_import_data(task)

                print("  ✅ Импорт успешно выполнен")

            except Exception as err:
                log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка batchUpdate в группе {group}: {str(err)}\n{traceback.format_exc()}")
                for task in group_tasks:
                    task["update_failures"] = task.get("update_failures", 0) + 1
                    update_sheet_import_data(task)
                print("  ❌ Ошибка при выполнении импорта")

        break

if __name__ == "__main__":
    SheetsInfo_scanner()
    log_to_file(SHEETS_LOG_FILE, "🔴 SheetsInfo_scanner завершен.")
