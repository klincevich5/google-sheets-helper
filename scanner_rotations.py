import time
from database import get_pending_scans, update_last_scan, log_scan_groups, get_doc_id_map
from logger import log_to_file
from utils import build_batchget_ranges, fetch_batch_values
from methods import get_range_by_method
from config import ROTATIONS_LOG_FILE, SCAN_INTERVAL_SECONDS, WARSAW_TZ
from datetime import datetime

def scan_process(process):
    log_to_file(ROTATIONS_LOG_FILE, f"🔍 Скан: ID={process['id']} — {process['name_of_process']}")
    time.sleep(1)  # заглушка
    update_last_scan("RotationsInfo", process["id"])
    log_to_file(ROTATIONS_LOG_FILE, f"✅ Завершено: ID={process['id']}")

def RotationsInfo_scanner():
    log_to_file(ROTATIONS_LOG_FILE, "🟢 RotationsInfo_scanner запущен.")
    log_scan_groups("RotationsInfo", ROTATIONS_LOG_FILE, group_field="scan_group")
    
    doc_id_map = get_doc_id_map()
    log_to_file(ROTATIONS_LOG_FILE, f"📊 Получено {len(doc_id_map)} актуальных документов для сканирования.")
    if not doc_id_map:
        log_to_file(ROTATIONS_LOG_FILE, "⚠️ Нет актуальных документов для сканирования.")
        return
    
    while True:
        log_to_file(ROTATIONS_LOG_FILE, "🔄 Проверка задач...")
        tasks = get_pending_scans("SheetsInfo")

        if not tasks:
            log_to_file(ROTATIONS_LOG_FILE, f"⏳ Нет задач на {datetime.now(WARSAW_TZ).strftime('%H:%M:%S')}")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue

        # 1. Группируем задачи по spreadsheet_id
        doc_to_tasks = {}
        for task in tasks:
            doc_id = doc_id_map.get(task["source_table_type"])
            if not doc_id:
                log_to_file(ROTATIONS_LOG_FILE, f"⚠️ Пропуск задачи без актуального doc_id: ID={task['id']}")
                continue

            log_to_file(ROTATIONS_LOG_FILE, f"📦 ID={task['id']} | 🆔 {doc_id} | 📑 {task['source_page_name']} | ⚙ {task['get_data_method']}")
            if doc_id:
                doc_to_tasks.setdefault(doc_id, []).append(task)

        # 2. Обрабатываем каждый документ отдельно
        for doc_id, task_list in doc_to_tasks.items():
            log_to_file(ROTATIONS_LOG_FILE, f"🔄 Обработка ID={doc_id} с {len(task_list)} задачами"    )
            batch_ranges = build_batchget_ranges(task_list, ROTATIONS_LOG_FILE).get(doc_id, set())
            if not batch_ranges:
                log_to_file(ROTATIONS_LOG_FILE, f"⚠️ Пропуск ID={doc_id} — нет ranges")
                continue

            log_to_file(ROTATIONS_LOG_FILE, f"🔄 Выполняется batchGet для ID={doc_id}, диапазонов: {len(batch_ranges)}")
            values_map = fetch_batch_values(doc_id, list(batch_ranges))
            log_to_file(ROTATIONS_LOG_FILE, f"📊 Получено {len(values_map)} страниц от ID={doc_id}")

            for row in task_list:
                log_to_file(ROTATIONS_LOG_FILE, f"📦 ID={row['id']} | 🆔 {doc_id} | 📑 {row['source_page_name']} | ⚙ {row['get_data_method']}")
                page = row["source_page_name"]
                area = row["source_page_area"]
                method = row["get_data_method"]
                process_id = row["id"]

                values = values_map.get(page, [])
                actual_range, error = get_range_by_method(method, page, area, values)

                if actual_range:
                    log_to_file(ROTATIONS_LOG_FILE, f"📥 ID={process_id} → зона сканирования: {actual_range}")
                    scan_process(row)
                    update_last_scan("SheetsInfo", process_id)
                else:
                    log_to_file(ROTATIONS_LOG_FILE, f"⚠️ ID={process_id} ошибка диапазона: {error}")

        time.sleep(SCAN_INTERVAL_SECONDS)
