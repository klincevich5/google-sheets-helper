import time
from datetime import datetime
import traceback
import json

from config import WARSAW_TZ
from database import update_sheet_task_data, update_sheet_import_data, update_rotation_row_data, set_need_update_true_below
from logger import log_to_file
from utils import process_data_by_method


def handle_fetched_data(value_ranges, range_to_tasks, changed_update_groups, table_name, log_file):
    """
    Обработка данных от Google Sheets: вычисление хэша, сравнение, запись в БД.
    """
    for sheet_range, value_range in zip(range_to_tasks.keys(), value_ranges):
        values = value_range.get("values", [])

        for task in range_to_tasks[sheet_range]:
            try:
                process_single_task(task, values, sheet_range, changed_update_groups, table_name, log_file)
            except Exception as e:
                log_to_file(log_file, f"⚠️ Ошибка в задаче ID={task['id']}: {str(e)}\n{traceback.format_exc(limit=1)}")

def process_single_task(task, values, sheet_range, changed_update_groups, table_name, log_file):
    """
    Обрабатывает одну задачу:
    - Проверяет, есть ли значения
    - Вызывает нужный метод обработки
    - Вычисляет хэш
    - Если хэш изменился — сохраняет изменения и добавляет группу к импорту
    - Логирует результат
    """
    task_id = task["id"]
    log_to_file(log_file, f"🟢 ID={task_id} | Обработка диапазона: {sheet_range}")

    # Обновляем технические поля
    now_str = datetime.now(WARSAW_TZ).isoformat()
    task["last_scan"] = now_str
    task["scan_quantity"] = task.get("scan_quantity", 0) + 1

    if not values:
        task["scan_failures"] = task.get("scan_failures", 0) + 1
        log_to_file(log_file, "  ❌ Пустой диапазон.")
        update_sheet_task_data(task, table_name, log_file)
        return

    # Получение метода обработки
    method = task.get("process_data_method") or "process_default"
    values_json, new_hash = process_data_by_method(method, values, log_file)

    # Сравнение хэшей
    old_hash = task.get("hash")
    if new_hash == old_hash:
        log_to_file(log_file, "  ✅ Данные не изменились (hash совпадает).")
    else:
        task["hash"] = new_hash
        task["values_json"] = values_json
        log_to_file(log_file, "  🔁 Данные изменены (hash обновлён).")

        update_group = task.get("update_group")
        if update_group and update_group not in changed_update_groups:
            changed_update_groups.append(update_group)

    # Финальный лог и сохранение в БД
    log_task_summary(task, log_file)
    update_sheet_task_data(task, table_name, log_file)


def log_task_summary(task, log_file):
    """
    Логирует ключевую информацию о задаче после обработки.
    """
    log_to_file(log_file, f"📦 ID={task['id']} | Итог:")
    for key in ["last_scan", "scan_quantity", "scan_failures", "hash"]:
        value = task.get(key)
        log_to_file(log_file, f"   {key} = {value}")


def perform_group_import(sheet, group_tasks, table_name, log_file):
    """
    Пакетный импорт задач группы. Повторяет попытки при ошибках, исключая проблемные задачи.
    """
    max_time = 60  # секунд
    retry_delay = 1
    start_time = time.time()

    tasks = list(group_tasks)
    update_group = tasks[0].get("update_group", "без имени")
    log_to_file(log_file, "=" * 30 + "\n")

    log_to_file(log_file, f"🟡 Запуск импорта группы: {update_group} ({len(tasks)} задач)")

    while tasks and (time.time() - start_time < max_time):

        data = prepare_batch_data(tasks, table_name, log_file)

        if not data:
            log_to_file(log_file, "⚠️ Все задачи исключены — нечего импортировать.")
            break

        log_task_details(tasks, log_file)

        try:
            log_to_file(log_file, f"🧪 Отправляем данные:\n{json.dumps(data, ensure_ascii=False)[:500]}")

            spreadsheet_id = tasks[0]["target_doc_id"]
            sheet.values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "RAW", "data": data}
            ).execute()

            now_str = datetime.now(WARSAW_TZ).isoformat()
            for task in tasks:
                task["last_update"] = now_str
                task["update_quantity"] = task.get("update_quantity", 0) + 1
                update_sheet_import_data(task, table_name, log_file)
                log_to_file(log_file, f"✅ ID={task['id']} | Импорт успешен")
                log_to_file(log_file, "===========================")

            break  # импорт прошел — выходим

        except Exception as err:
            error_text = str(err)
            log_to_file(log_file, f"❌ Ошибка batchUpdate:\n{error_text}\n{traceback.format_exc(limit=1)}")

            failed_task = find_failed_task(tasks, error_text, log_file)
            if failed_task:
                failed_task["update_failures"] = failed_task.get("update_failures", 0) + 1
                update_sheet_import_data(failed_task, table_name, log_file)
                log_to_file(log_file, f"⚠️ ID={failed_task['id']} | Исключён из-за ошибки")
                tasks.remove(failed_task)
                log_to_file(log_file, "===========================")

            time.sleep(retry_delay)


def prepare_batch_data(tasks, table_name, log_file):
    prepared = []
    to_exclude = []

    for task in tasks:
        try:
            raw = task.get("values_json") or "[]"
            values = json.loads(raw)

            # Защита: должны быть списки списков
            if not isinstance(values, list) or any(not isinstance(row, list) for row in values):
                raise ValueError("Данные не являются списками строк (2D-массив)")

            prepared.append({
                "range": f"'{task['target_page_name']}'!{task['target_page_area']}",
                "values": values
            })

        except Exception as e:
            task["update_failures"] = task.get("update_failures", 0) + 1
            update_sheet_import_data(task, table_name, log_file)
            log_to_file(log_file, f"⚠️ Ошибка JSON или данных в ID={task['id']}: {str(e)}")
            to_exclude.append(task)

    for t in to_exclude:
        tasks.remove(t)

    return prepared



def find_failed_task(tasks, error_text, log_file):
    """
    Находит задачу, по которой могла произойти ошибка batchUpdate.
    Иначе возвращает первую.
    """
    for task in tasks:
        if task["target_page_name"] in error_text:
            return task
    return tasks[0] if tasks else None


def log_task_details(tasks, log_file):
    """
    Логирует короткую сводку по каждой задаче перед импортом
    """
    for task in tasks:
        log_to_file(log_file, f"🔁 ID={task['id']}")
        log_to_file(log_file, f"   ➤ name_of_process: {task['name_of_process']}")
        log_to_file(log_file, f"   ➤ Зона откуда:  {task['source_table_type']}!{task['source_page_name']}!{task['source_page_area']}")
        log_to_file(log_file, f"   ➤ Метод: {task['process_data_method']}")
        log_to_file(log_file, f"   ➤ Зона куда:  {task['target_table_type']}!{task['target_page_name']}!{task['target_page_area']}")
        try:
            data = json.loads(task["values_json"])
            log_to_file(log_file, f"   ➤ Размер: {len(data)}x{len(data[0]) if data else 0}")
        except Exception:
            log_to_file(log_file, "   ⚠️ Ошибка при анализе данных JSON")
        log_to_file(log_file, "-" * 30)
    log_to_file(log_file, "=" * 30)
    return True

def handle_main_rotations_group(sheet, tasks, table_name, log_file):
    """
    Обрабатывает update_group='update_main': 8 ротаций, одна цель, строгий порядок.
    """

    ROTATION_ORDER = [
        "SHUFFLE Main",
        "VIP Main",
        "TURKISH Main",
        "GENERIC Main",
        "GSBJ Main",
        "LEGENDZ Main",
        "TRI-STAR Main",
        "TritonRL Main",
    ]

    # Сортируем задачи строго по порядку названий
    tasks_by_name = {t["name_of_process"]: t for t in tasks}
    sorted_tasks = [tasks_by_name[name] for name in ROTATION_ORDER if name in tasks_by_name]

    prev_end = 1
    for idx, task in enumerate(sorted_tasks):
        task_id = task["id"]
        log_to_file(log_file, f"\n🌀 Обработка ротации: {task['name_of_process']} (ID={task_id})")

        # Попытка получить значения из JSON
        try:
            values = json.loads(task["values_json"])
        except Exception:
            log_to_file(log_file, "⚠️ Повреждён JSON. Пропуск.")
            continue

        if not values:
            log_to_file(log_file, "⚠️ Пустые значения. Пропуск.")
            continue

        row_count = len(values)
        old_start = task.get("start_row")
        old_end = task.get("end_row")
        old_hash = task.get("hash")
        need_update = task.get("need_update")

        # Вычисляем новый диапазон
        new_start = prev_end
        new_end = new_start + row_count - 1

        # Первое обновление (нет данных вообще)
        if not old_start or not old_end or need_update is None:
            log_to_file(log_file, "🆕 Первая инициализация ротации")
            insert_rotation(sheet, task, new_start, new_end, values, table_name, log_file)
            update_rotation_row_data(task_id, new_start, new_end, task["hash"], values, False, table_name, log_file)
            set_need_update_true_below(task_id, table_name, log_file)

        # Если need_update=True — обновляем в новый диапазон
        elif need_update:
            insert_rotation(sheet, task, new_start, new_end, values, table_name, log_file)
            clear_range(sheet, task["target_doc_id"], task["target_page_name"], new_end + 1, new_end + 1, table_name, log_file)
            update_rotation_row_data(task_id, new_start, new_end, task["hash"], values, False, table_name, log_file)
            set_need_update_true_below(task_id, table_name, log_file)

        # Если хэш и длина изменились — обновляем и активируем нижние
        elif task["hash"] != old_hash and (old_end - old_start + 1 != row_count):
            insert_rotation(sheet, task, new_start, new_end, values, table_name, log_file)
            if old_end > new_end:
                clear_range(sheet, task["target_doc_id"], task["target_page_name"], new_end + 1, old_end, table_name, log_file)
            else:
                clear_range(sheet, task["target_doc_id"], task["target_page_name"], new_end + 1, new_end + 1, table_name, log_file)
            update_rotation_row_data(task_id, new_start, new_end, task["hash"], values, True, table_name, log_file)
            set_need_update_true_below(task_id, table_name, log_file)

        # Если хэш изменился, а длина совпадает — просто обновляем на месте
        elif task["hash"] != old_hash:
            insert_rotation(sheet, task, old_start, old_end, values, table_name, log_file)
            update_rotation_row_data(task_id, old_start, old_end, task["hash"], values, False, table_name, log_file)

        else:
            log_to_file(log_file, "⏭️ Пропуск: изменений нет.")

        # Обновляем prev_end для следующей ротации
        prev_end = new_end + 2


def insert_rotation(sheet, task, start_row, end_row, values, table_name, log_file):
    """
    Вставляет values в зону 'target_page_name'!D{start_row}:AC{end_row}
    """
    target_range = f"'{task['target_page_name']}'!D{start_row}:AC{end_row}"
    log_to_file(log_file, f"⬇️ Вставка ротации: {target_range} (строк: {len(values)})")

    sheet.values().batchUpdate(
        spreadsheetId=task["target_doc_id"],
        body={
            "valueInputOption": "RAW",
            "data": [{
                "range": target_range,
                "values": values
            }]
        }
    ).execute()

def clear_range(sheet, spreadsheet_id, sheet_name, start_row, end_row, table_name, log_file):
    """
    Очищает диапазон D{start_row}:AC{end_row} в указанном листе
    """
    if start_row > end_row:
        return  # Нечего чистить

    clear_range = f"'{sheet_name}'!D{start_row}:AC{end_row}"
    log_to_file(log_file, f"🧹 Очистка диапазона: {clear_range}")

    sheet.values().batchClear(
        spreadsheetId=spreadsheet_id,
        body={"ranges": [clear_range]}
    ).execute()
