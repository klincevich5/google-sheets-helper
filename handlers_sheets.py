import time
from datetime import datetime
import traceback
import json

from config import WARSAW_TZ, SHEETS_LOG_FILE
from database import update_sheet_task_data, update_sheet_import_data
from logger import log_to_file
from utils import process_data_by_method


def handle_fetched_data(value_ranges, range_to_tasks, changed_update_groups):
    """
    Обработка полученных данных от batchGet.
    Присваивает хэш, сравнивает с предыдущим, определяет, были ли изменения.
    Обновляет информацию о сканировании и добавляет задачи в список для импорта.
    """
    for requested_range, value_range in zip(range_to_tasks.keys(), value_ranges):
        values = value_range.get("values", [])

        for task in range_to_tasks[requested_range]:
            try:
                log_to_file(SHEETS_LOG_FILE, f"🟢 ID={task['id']} | Запрос диапазона {requested_range}")

                now_str = datetime.now(WARSAW_TZ).isoformat()
                task["last_scan"] = now_str
                task["scan_quantity"] = task.get("scan_quantity", 0) + 1

                # Если данные отсутствуют — отмечаем как неудачное сканирование
                if not values:
                    task["scan_failures"] = task.get("scan_failures", 0) + 1
                    log_to_file(SHEETS_LOG_FILE, f"  ❌ ID={task['id']} | Пустой диапазон {requested_range}")
                else:
                    # Обрабатываем и сравниваем хэш
                    process_data_method = task.get("process_data_method")

                    values_json, new_hash = process_data_by_method(process_data_method, values)

                    if new_hash != task.get("hash"):
                        log_to_file(SHEETS_LOG_FILE, f"  🔁 ID={task['id']} | Данные изменены")
                        task["hash"] = new_hash
                        task["values_json"] = values_json
                        if task.get("update_group"):
                            changed_update_groups.append(task["update_group"])

                # Обновляем задачу в БД после сканирования
                for key in ["last_scan", "scan_quantity", "scan_failures", "hash", "values_json"]:
                    log_to_file(SHEETS_LOG_FILE, "Содержимое задачи:")
                    log_to_file(SHEETS_LOG_FILE, f"  🔁 {key}={task[key]}")
                    task[key] = task.get(key, None)
                update_sheet_task_data(task)

            except Exception as e:
                log_to_file(SHEETS_LOG_FILE, f"⚠️ Ошибка в задаче {task['id']}: {str(e)}\n{traceback.format_exc(limit=1)}")


def perform_group_import(sheet, group_tasks):
    """
    Пошаговый импорт данных группы через batchUpdate.
    При ошибке — исключаем некорректную задачу и пробуем снова.
    Максимум 1 минута повторов, пауза 1 секунда между итерациями.
    """
    from time import time as now_unix
    start = now_unix()
    max_time = 60  # секунд
    delay = 1      # пауза между попытками
    tasks_remaining = list(group_tasks)  # копия задач
    imported = set()

    while tasks_remaining and now_unix() - start < max_time:
        log_to_file(SHEETS_LOG_FILE, "===========================")
        log_to_file(SHEETS_LOG_FILE, f"🟢 Импорт группы {tasks_remaining[0]['update_group']}...")
        log_to_file(SHEETS_LOG_FILE, f"🟢 Осталось задач для импорта: {len(tasks_remaining)}")

        # Логируем задачи перед попыткой импорта
        for task in tasks_remaining:
            log_to_file(SHEETS_LOG_FILE, f"🔁 source_page_area={task['source_page_area']}")
            log_to_file(SHEETS_LOG_FILE, f"🔁 process_data_method={task['process_data_method']}")
            log_to_file(SHEETS_LOG_FILE, f"после обработки{len(task['values_json'][0])} столбцов, {len(task['values_json'])} строк")
            log_to_file(SHEETS_LOG_FILE, f" 🔁 target_page_area={task['target_page_area']}")
            log_to_file(SHEETS_LOG_FILE, "============================")
            log_to_file(SHEETS_LOG_FILE, f"  🟢 ID={task['id']} | Импорт диапазона {task['target_page_name']}!{task['target_page_area']}")

        # Подготовка данных для batchUpdate
        data_to_import = []
        for task in tasks_remaining:  # Копия списка — чтобы безопасно удалять элементы
            try:
                values = json.loads(task.get("values_json") or "[]")
                data_to_import.append({
                    "range": f"'{task['target_page_name']}'!{task['target_page_area']}",
                    "values": values
                })
            except json.JSONDecodeError as e:
                # Ошибка при распарсивании JSON-строки
                task["update_failures"] = task.get("update_failures", 0) + 1
                update_sheet_import_data(task)
                log_to_file(SHEETS_LOG_FILE, f"⚠️ Ошибка JSON в задаче ID={task['id']}: {str(e)}")
                tasks_remaining.remove(task)
            except Exception as e:
                # Другие неожиданные ошибки — логируем явно
                task["update_failures"] = task.get("update_failures", 0) + 1
                update_sheet_import_data(task)
                log_to_file(SHEETS_LOG_FILE, f"⚠️ Неизвестная ошибка в задаче ID={task['id']}: {str(e)}\n{traceback.format_exc(limit=1)}")
                tasks_remaining.remove(task)
        if not data_to_import:
            log_to_file(SHEETS_LOG_FILE, "⚠️ Все задачи исключены из группы — нечего импортировать.")
            break

        # Отправляем данные в Google Sheets
        try:
            sheet.values().batchUpdate(
                spreadsheetId=tasks_remaining[0]["target_doc_id"],
                body={"valueInputOption": "RAW", "data": data_to_import}
            ).execute()

            # Обновляем каждую задачу как успешно загруженную
            now_str = datetime.now(WARSAW_TZ).isoformat()
            for task in tasks_remaining:
                task["last_update"] = now_str
                task["update_quantity"] = task.get("update_quantity", 0) + 1
                update_sheet_import_data(task)
                imported.add(task["id"])
                log_to_file(SHEETS_LOG_FILE, f"✅ ID={task['id']} | Импорт успешен")

            break  # выходим — все задачи успешно загружены

        except Exception as err:
            text = str(err)
            log_to_file(SHEETS_LOG_FILE, "")
            log_to_file(SHEETS_LOG_FILE, f"❌ batchUpdate ошибка:\n{text}\n{traceback.format_exc(limit=1)}")
            log_to_file(SHEETS_LOG_FILE, "")

            # Ищем задачу, по которой могла быть ошибка (по имени листа)
            failed = None
            for task in tasks_remaining:
                if task["target_page_name"] in text:
                    failed = task
                    break

            # Если не нашли — исключаем первую задачу
            failed = failed or tasks_remaining[0]

            # Отмечаем ошибку в БД и исключаем задачу
            failed["update_failures"] = failed.get("update_failures", 0) + 1
            log_to_file(SHEETS_LOG_FILE, f"⚠️ ID={failed['id']} | Ошибка в задаче: {text}")
            update_sheet_import_data(failed)
            log_to_file(SHEETS_LOG_FILE, f"  ❌ ID={failed['id']} | Исключён из группы")
            tasks_remaining.remove(failed)

            time.sleep(delay)
