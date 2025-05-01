import time
import json
import hashlib
from datetime import datetime

from config import WARSAW_TZ, SHEETS_LOG_FILE, SCAN_INTERVAL_SECONDS
from db import get_ready_tasks, update_task_scan, update_task_import
from logger import log_to_file
from sheets import load_sheet_api, batch_get_ranges, batch_update_ranges
from processors.registry import get_processor


def run_scanner():
    log_to_file(SHEETS_LOG_FILE, "🟢 Сканер запущен.")

    while True:
        # 1. Получаем задачи на сканирование
        tasks = get_ready_tasks()
        if not tasks:
            log_to_file(SHEETS_LOG_FILE, "⏳ Нет задач для сканирования.")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue

        # 2. Загружаем клиент API
        sheet = load_sheet_api()
        if not sheet:
            log_to_file(SHEETS_LOG_FILE, "❌ Не удалось подключиться к Google Sheets.")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue

        log_to_file(SHEETS_LOG_FILE, f"📄 Задач на скан: {len(tasks)}")

        for task in tasks:
            try:
                doc_id = task["source_doc_id"]
                sheet_range = f"'{task['source_page_name']}'!{task['source_page_area']}"
                values = batch_get_ranges(sheet, doc_id, [sheet_range])[0]
                now_str = datetime.now(WARSAW_TZ).isoformat()

                # Обработка через process_data_method
                processor = get_processor(task["process_data_method"])
                processed = processor(values)
                values_json = json.dumps(processed, ensure_ascii=False)
                values_hash = hashlib.md5(values_json.encode("utf-8")).hexdigest()

                # Сравнение с предыдущим хэшем
                if values_hash != task.get("hash"):
                    log_to_file(SHEETS_LOG_FILE, f"🔁 ID={task['id']} | Обнаружены изменения.")
                    task["values_json"] = values_json
                    task["hash"] = values_hash
                    task["needs_update"] = True
                else:
                    log_to_file(SHEETS_LOG_FILE, f"✅ ID={task['id']} | Без изменений.")
                    task["needs_update"] = False

                task["last_scan"] = now_str
                task["scan_quantity"] = task.get("scan_quantity", 0) + 1
                update_task_scan(task)

            except Exception as e:
                log_to_file(SHEETS_LOG_FILE, f"⚠️ Ошибка в задаче ID={task['id']}: {str(e)}")

        # Импорт изменений
        tasks_to_import = [t for t in tasks if t.get("needs_update")]
        if tasks_to_import:
            run_import(sheet, tasks_to_import)

        time.sleep(SCAN_INTERVAL_SECONDS)


def run_import(sheet, tasks):
    log_to_file(SHEETS_LOG_FILE, f"⬇️ Импорт {len(tasks)} изменённых задач...")
    for task in tasks:
        try:
            values = json.loads(task["values_json"])
            batch_update_ranges(sheet, task["target_doc_id"], [{
                "range": f"'{task['target_page_name']}'!{task['target_page_area']}",
                "values": values
            }])
            task["last_update"] = datetime.now(WARSAW_TZ).isoformat()
            task["update_quantity"] = task.get("update_quantity", 0) + 1
            update_task_import(task)
            log_to_file(SHEETS_LOG_FILE, f"✅ ID={task['id']} | Импорт выполнен.")
        except Exception as e:
            log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка импорта ID={task['id']}: {str(e)}")
