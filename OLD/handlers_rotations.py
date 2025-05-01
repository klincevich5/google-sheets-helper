# import time
# from datetime import datetime
# import traceback
# import json

# from config import WARSAW_TZ, SHEETS_LOG_FILE
# from database import update_sheet_task_data, update_sheet_import_data
# from logger import log_to_file
# from utils import process_data_by_method


# def handle_fetched_data(value_ranges, range_to_tasks, changed_update_groups, table_name, log_file):
#     """
#     Обработка данных от Google Sheets: вычисление хэша, сравнение, запись в БД.
#     """
#     for sheet_range, value_range in zip(range_to_tasks.keys(), value_ranges):
#         values = value_range.get("values", [])

#         for task in range_to_tasks[sheet_range]:
#             try:
#                 process_single_task(task, values, sheet_range, changed_update_groups, table_name, log_file)
#             except Exception as e:
#                 log_to_file(SHEETS_LOG_FILE, f"⚠️ Ошибка в задаче ID={task['id']}: {str(e)}\n{traceback.format_exc(limit=1)}")

# def process_single_task(task, values, sheet_range, changed_update_groups):
#     """
#     Обрабатывает одну задачу:
#     - Проверяет, есть ли значения
#     - Вызывает нужный метод обработки
#     - Вычисляет хэш
#     - Если хэш изменился — сохраняет изменения и добавляет группу к импорту
#     - Логирует результат
#     """
#     task_id = task["id"]
#     log_to_file(SHEETS_LOG_FILE, f"🟢 ID={task_id} | Обработка диапазона: {sheet_range}")

#     # Обновляем технические поля
#     now_str = datetime.now(WARSAW_TZ).isoformat()
#     task["last_scan"] = now_str
#     task["scan_quantity"] = task.get("scan_quantity", 0) + 1

#     if not values:
#         task["scan_failures"] = task.get("scan_failures", 0) + 1
#         log_to_file(SHEETS_LOG_FILE, "  ❌ Пустой диапазон.")
#         update_sheet_task_data(task)
#         return

#     # Получение метода обработки
#     method = task.get("process_data_method") or "process_default"
#     values_json, new_hash = process_data_by_method(method, values)

#     # Сравнение хэшей
#     old_hash = task.get("hash")
#     if new_hash == old_hash:
#         log_to_file(SHEETS_LOG_FILE, "  ✅ Данные не изменились (hash совпадает).")
#     else:
#         task["hash"] = new_hash
#         task["values_json"] = values_json
#         log_to_file(SHEETS_LOG_FILE, "  🔁 Данные изменены (hash обновлён).")

#         update_group = task.get("update_group")
#         if update_group and update_group not in changed_update_groups:
#             changed_update_groups.append(update_group)

#     # Финальный лог и сохранение в БД
#     log_task_summary(task)
#     update_sheet_task_data(task)


# def log_task_summary(task):
#     """
#     Логирует ключевую информацию о задаче после обработки.
#     """
#     log_to_file(SHEETS_LOG_FILE, f"📦 ID={task['id']} | Итог:")
#     for key in ["last_scan", "scan_quantity", "scan_failures", "hash"]:
#         value = task.get(key)
#         log_to_file(SHEETS_LOG_FILE, f"   {key} = {value}")


# def perform_group_import(sheet, group_tasks):
#     """
#     Пакетный импорт задач группы. Повторяет попытки при ошибках, исключая проблемные задачи.
#     """
#     max_time = 60  # секунд
#     retry_delay = 1
#     start_time = time.time()

#     tasks = list(group_tasks)
#     update_group = tasks[0].get("update_group", "без имени")
#     log_to_file(SHEETS_LOG_FILE, "===========================")

#     log_to_file(SHEETS_LOG_FILE, f"🟡 Запуск импорта группы: {update_group} ({len(tasks)} задач)")

#     while tasks and (time.time() - start_time < max_time):

#         data = prepare_batch_data(tasks)

#         if not data:
#             log_to_file(SHEETS_LOG_FILE, "⚠️ Все задачи исключены — нечего импортировать.")
#             break

#         log_task_details(tasks)

#         try:
#             log_to_file(SHEETS_LOG_FILE, f"🧪 Отправляем данные:\n{json.dumps(data, ensure_ascii=False)[:500]}")

#             spreadsheet_id = tasks[0]["target_doc_id"]
#             sheet.values().batchUpdate(
#                 spreadsheetId=spreadsheet_id,
#                 body={"valueInputOption": "RAW", "data": data}
#             ).execute()

#             now_str = datetime.now(WARSAW_TZ).isoformat()
#             for task in tasks:
#                 task["last_update"] = now_str
#                 task["update_quantity"] = task.get("update_quantity", 0) + 1
#                 update_sheet_import_data(task)
#                 log_to_file(SHEETS_LOG_FILE, f"✅ ID={task['id']} | Импорт успешен")
#                 log_to_file(SHEETS_LOG_FILE, "===========================")

#             break  # импорт прошел — выходим

#         except Exception as err:
#             error_text = str(err)
#             log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка batchUpdate:\n{error_text}\n{traceback.format_exc(limit=1)}")

#             failed_task = find_failed_task(tasks, error_text)
#             if failed_task:
#                 failed_task["update_failures"] = failed_task.get("update_failures", 0) + 1
#                 update_sheet_import_data(failed_task)
#                 log_to_file(SHEETS_LOG_FILE, f"⚠️ ID={failed_task['id']} | Исключён из-за ошибки")
#                 tasks.remove(failed_task)
#                 log_to_file(SHEETS_LOG_FILE, "===========================")

#             time.sleep(retry_delay)


# def prepare_batch_data(tasks):
#     prepared = []
#     to_exclude = []

#     for task in tasks:
#         try:
#             raw = task.get("values_json") or "[]"
#             values = json.loads(raw)

#             # Защита: должны быть списки списков
#             if not isinstance(values, list) or any(not isinstance(row, list) for row in values):
#                 raise ValueError("Данные не являются списками строк (2D-массив)")

#             prepared.append({
#                 "range": f"'{task['target_page_name']}'!{task['target_page_area']}",
#                 "values": values
#             })

#         except Exception as e:
#             task["update_failures"] = task.get("update_failures", 0) + 1
#             update_sheet_import_data(task)
#             log_to_file(SHEETS_LOG_FILE, f"⚠️ Ошибка JSON или данных в ID={task['id']}: {str(e)}")
#             to_exclude.append(task)

#     for t in to_exclude:
#         tasks.remove(t)

#     return prepared



# def find_failed_task(tasks, error_text):
#     """
#     Находит задачу, по которой могла произойти ошибка batchUpdate.
#     Иначе возвращает первую.
#     """
#     for task in tasks:
#         if task["target_page_name"] in error_text:
#             return task
#     return tasks[0] if tasks else None


# def log_task_details(tasks):
#     """
#     Логирует короткую сводку по каждой задаче перед импортом
#     """
#     for task in tasks:
#         log_to_file(SHEETS_LOG_FILE, f"🔁 ID={task['id']}")
#         log_to_file(SHEETS_LOG_FILE, f"   ➤ name_of_process: {task['name_of_process']}")
#         log_to_file(SHEETS_LOG_FILE, f"   ➤ Зона откуда:  {task['source_table_type']}!{task['source_page_name']}!{task['source_page_area']}")
#         log_to_file(SHEETS_LOG_FILE, f"   ➤ Метод: {task['process_data_method']}")
#         log_to_file(SHEETS_LOG_FILE, f"   ➤ Зона куда:  {task['target_table_type']}!{task['target_page_name']}!{task['target_page_area']}")
#         try:
#             data = json.loads(task["values_json"])
#             log_to_file(SHEETS_LOG_FILE, f"   ➤ Размер: {len(data)}x{len(data[0]) if data else 0}")
#         except Exception:
#             log_to_file(SHEETS_LOG_FILE, "   ⚠️ Ошибка при анализе данных JSON")
#     return True