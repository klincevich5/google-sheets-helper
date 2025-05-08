# core/logic.py

import hashlib
from datetime import datetime, timedelta
import time
from typing import List
from core.data import return_raw_tasks, return_tracked_tables

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from core.methods import PROCESSORS

def log_to_file(path, text):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} — {text}\n")

log_file = "scanner.log"

TOKEN_PATH = "token.json"  # путь к токену

def load_credentials():
    creds = Credentials.from_authorized_user_file(TOKEN_PATH)
    service = build("sheets", "v4", credentials=creds)
    return service


def batch_get(service, spreadsheet_id, ranges, log_file):
    """ Получение данных из Google Sheets с помощью batchGet """
    try:
        result = service.spreadsheets().values().batchGet(  # <-- добавляем .spreadsheets()!
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            majorDimension="ROWS"
        ).execute()

        value_ranges = result.get("valueRanges", [])
        data = {}
        for value_range in value_ranges:
            range_name = value_range.get("range", "")
            values = value_range.get("values", [])
            data[range_name] = values
        return data
    except Exception as e:
        log_to_file(log_file, f"❌ Ошибка при batchGet: {e}")
        return {}
    
def batch_update(service, spreadsheet_id, batch_data, log_file):
    """ Отправка batchUpdate запроса с несколькими диапазонами """
    try:
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": batch_data
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

        return True, None

    except Exception as e:
        log_to_file(log_file, f"❌ Ошибка при batchUpdate: {e}")
        return False, str(e)
    
# def RotationsInfo_scanner(self):
#     log_file = "scanner_rotationsinfo.log"

#     if not self.rotationsinfo_tasks:
#         log_to_file(log_file, "⚪ Нет задач для обновления RotationsInfo.")
#         return

#     update_main_tasks = [task for task in self.rotationsinfo_tasks if task.update_group == "update_main"]
#     update_shuffle_tasks = [task for task in self.rotationsinfo_tasks if task.update_group in ("update_generic_shuffle", "update_legendz_shuffle", "update_gsbj_shuffle")]

#     if update_main_tasks:
#         self._handle_main_updates(update_main_tasks, log_file)

#     if update_shuffle_tasks:
#         self._handle_shuffle_updates(update_shuffle_tasks, log_file)

# def SheetsInfo_scanner(self):
#     log_file = "scanner_sheetsinfo.log"

#     if not self.sheetsinfo_tasks:
#         log_to_file(log_file, "⚪ Нет задач для обновления SheetsInfo.")
#         return

#     self._handle_other_updates(self.sheetsinfo_tasks, log_file)


class Task:
    def __init__(self, data):
        self.id = data.get("id")
        self.name_of_process = data.get("name_of_process")
        self.source_table_type = data.get("source_table_type")
        self.source_page_name = data.get("source_page_name")
        self.source_page_area = data.get("source_page_area")
        self.scan_group = data.get("scan_group")
        self.last_scan = data.get("last_scan")
        self.scan_interval = data.get("scan_interval")
        self.scan_quantity = data.get("scan_quantity", 0)
        self.scan_failures = data.get("scan_failures", 0)
        self.hash = data.get("hash")
        self.process_data_method = data.get("process_data_method")
        self.values_json = data.get("values_json")
        self.target_table_type = data.get("target_table_type")
        self.target_page_name = data.get("target_page_name")
        self.target_page_area = data.get("target_page_area")
        self.update_group = data.get("update_group")
        self.last_update = data.get("last_update")
        self.update_quantity = data.get("update_quantity", 0)
        self.update_failures = data.get("update_failures", 0)
        self.start_row = data.get("start_row")
        self.end_row = data.get("end_row")
        self.need_update = data.get("need_update", 0)

        self.source_doc_id = None
        self.target_doc_id = None

        self.raw_values_json = None  # <=== Новое поле для сырых данных

    def is_ready_to_scan(self):
        if not self.last_scan:
            log_to_file(log_file, f"📅 [Task {self.id} {self.name_of_process}] Нет last_scan — готово к сканированию.")
            return True
        next_scan_time = self.last_scan + timedelta(seconds=self.scan_interval)
        ready = datetime.now() >= next_scan_time
        log_to_file(log_file, f"📅 [Task {self.id} {self.name_of_process}] Last scan: {self.last_scan}, Next scan time: {next_scan_time}, Ready: {ready}")
        return ready

    def assign_doc_ids(self, doc_id_map):
        self.source_doc_id = doc_id_map.get(self.source_table_type)
        self.target_doc_id = doc_id_map.get(self.target_table_type)
        if self.source_doc_id and self.target_doc_id:
            log_to_file(log_file, f"🔗 [Task {self.name_of_process}] Привязаны doc_id: source_doc_id={self.source_doc_id}, target_doc_id={self.target_doc_id}")
            return True
        else:
            log_to_file(log_file, f"❌ [Task {self.name_of_process}] Не удалось привязать doc_id: source={self.source_table_type}, target={self.target_table_type}")
            return False

    def update_after_scan(self, success):
        previous_scan_quantity = self.scan_quantity
        previous_scan_failures = self.scan_failures
        if success:
            self.last_scan = datetime.now()
            self.scan_quantity += 1
            log_to_file(log_file, f"✅ [Task {self.name_of_process}] Успешный скан: scan_quantity {previous_scan_quantity} → {self.scan_quantity}")
        else:
            self.scan_failures += 1
            log_to_file(log_file, f"⚠️ [Task {self.name_of_process}] Ошибка сканирования: scan_failures {previous_scan_failures} → {self.scan_failures}")

    def process_raw_value(self):
        if not self.raw_values_json:
            log_to_file(log_file, f"⚪ [Task {self.name_of_process}] Нет данных для обработки (raw_values_json пуст).")
            return

        method_name = self.process_data_method or "process_default"
        process_func = PROCESSORS.get(method_name)

        if not process_func:
            log_to_file(log_file, f"❌ [Task {self.name_of_process}] Неизвестный метод обработки: {method_name}")
            return

        try:
            processed_values = process_func(self.raw_values_json)
            self.values_json = processed_values
            log_to_file(log_file, f"🛠️ [Task {self.name_of_process}] Данные успешно обработаны через метод {method_name}.")
        except Exception as e:
            log_to_file(log_file, f"❌ [Task {self.name_of_process}] Ошибка при обработке методом {method_name}: {e}")

    def process_values(self):
        if not self.values_json:
            log_to_file(log_file, f"⚪ [Task {self.name_of_process}] Нет данных в values_json для хэширования.")
            return None
        processed = str(self.values_json).encode("utf-8")
        new_hash = hashlib.md5(processed).hexdigest()
        log_to_file(log_file, f"📌 [Task {self.name_of_process}] Посчитан новый хэш: {new_hash}")
        return new_hash

    def check_for_update(self):
        new_hash = self.process_values()
        if new_hash and new_hash != self.hash:
            old_hash = self.hash
            self.hash = new_hash
            self.need_update = 1
            log_to_file(log_file, f"♻️ [Task {self.name_of_process}] Хэш изменился: {old_hash} → {new_hash}, нужна выгрузка.\n")
        else:
            self.need_update = 0
            log_to_file(log_file, f"✅ [Task {self.name_of_process}] Хэш не изменился, выгрузка не требуется.\n")

    def update_after_upload(self, success):
        previous_update_quantity = self.update_quantity
        previous_update_failures = self.update_failures
        if success:
            self.last_update = datetime.now()
            self.update_quantity += 1
            log_to_file(log_file, f"📤 [Task {self.name_of_process}] Успешный апдейт: update_quantity {previous_update_quantity} → {self.update_quantity}")
        else:
            self.update_failures += 1
            log_to_file(log_file, f"❌ [Task {self.name_of_process}] Ошибка апдейта: update_failures {previous_update_failures} → {self.update_failures}")

class TaskManager:
    def __init__(self):
        self.tasks = []
        self.doc_id_map = self.get_current_month_id()
        self.service = load_credentials()

    def get_active_tabs(self):
        now = datetime.now()
        hour = now.hour
        tab_list = []

        if 9 <= hour < 19:
            tab_list.append(f"DAY {now.day}")
        elif 19 <= hour < 21:
            tab_list.append(f"DAY {now.day}")
            tab_list.append(f"NIGHT {now.day}")
        elif 21 <= hour <= 23:
            tab_list.append(f"NIGHT {now.day}")
        elif 0 <= hour < 7:
            yesterday = now - timedelta(days=1)
            tab_list.append(f"NIGHT {yesterday.day}")
        elif 7 <= hour < 9:
            yesterday = now - timedelta(days=1)
            tab_list.append(f"DAY {now.day}")
            tab_list.append(f"NIGHT {yesterday.day}")

        # Для тестов
        tab_list = ["DAY 1"]
        return tab_list

    def load_active_tasks(self, active_tabs):
        raw_tasks = return_raw_tasks()
        # return [Task(data) for data in raw_tasks if data.get("source_page_name") in active_tabs]
        return [Task(data) for data in raw_tasks]

    def get_current_month_id(self):
        today = datetime.now().date()
        tracked_tables = return_tracked_tables()
        result = {}
        for table in tracked_tables:
            valid_from = datetime.strptime(table["valid_from"], "%d.%m.%Y").date()
            valid_to = datetime.strptime(table["valid_to"], "%d.%m.%Y").date()
            if valid_from <= today <= valid_to:
                result[table["table_type"]] = table["spreadsheet_id"]
        return result

############################################################################################################################################################
#         log_to_file(log_file, "=== 🛠️ Старт фазы сканирования ==="
############################################################################################################################################################

    def scan_phase(self):
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "=== 🚀 Старт фазы сканирования ===")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")

        active_tabs = self.get_active_tabs()
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, f"📄 Определены активные вкладки: {active_tabs}")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")
        self.tasks = self.load_active_tasks(active_tabs)

        if not self.tasks:
            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, "⚠️ Нет активных задач для текущих вкладок.")
            log_to_file(log_file, "")
            log_to_file(log_file, "=" * 100)
            return
        
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, f"🟢 Найдено активных задач: {len(self.tasks)}")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")

        ready_tasks = [task for task in self.tasks if task.is_ready_to_scan()]
        if not ready_tasks:
            log_to_file(log_file, "")
            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, "⚠️ Нет задач, готовых к сканированию.")
            log_to_file(log_file, "")
            log_to_file(log_file, "=" * 100)
            self.tasks = []
            return
        else:
            log_to_file(log_file, "")
            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, f"🟢 Найдено {len(ready_tasks)} задач, готовых к сканированию")
            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, "")

        scan_groups = {}
        for task in ready_tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                continue
            scan_groups.setdefault(task.scan_group, []).append(task)
        log_to_file(log_file, "")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, f"🟢 Найдено {len(scan_groups)} групп для сканирования:")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")

        self.tasks = []  # Очищаем, чтобы оставить только отсканированные

        for group, tasks in scan_groups.items():
            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, f"🔍 [Группа {group}] Начало сканирования {len(tasks)} задач")
            log_to_file(log_file, "")

            try:
                unique_ranges = list({f"{task.source_page_name}!{task.source_page_area}" for task in tasks})
                spreadsheet_id = tasks[0].source_doc_id

                log_to_file(log_file, f"🔄 Запрос данных из {spreadsheet_id} для диапазонов: {unique_ranges}")

                response_data = batch_get(self.service, spreadsheet_id, unique_ranges, log_file)

                # Нормализуем ответ
                normalized_response = {}
                for k, v in response_data.items():
                    clean_key = k.replace("'", "")
                    sheet_name, cells_range = clean_key.split("!")
                    normalized_response[(sheet_name, cells_range)] = v

                log_to_file(log_file, f"🔄 Получены данные: {list(normalized_response.keys())}")
                log_to_file(log_file, "")

                successful_tasks = []

                for task in tasks:
                    expected_sheet = task.source_page_name
                    expected_area = task.source_page_area.split(":")[0]  # берем начало диапазона, например "D"

                    matched = None
                    for (sheet_name, cells_range), values in normalized_response.items():
                        if sheet_name == expected_sheet and cells_range.startswith(expected_area):
                            matched = values
                            break

                    if matched:
                        task.raw_values_json = matched
                        task.update_after_scan(success=True)
                        successful_tasks.append(task)
                    else:
                        task.update_after_scan(success=False)

                self.tasks.extend(successful_tasks)

                if successful_tasks:
                    log_to_file(log_file, "")
                    log_to_file(log_file, f"✅ [Группа {group}] Успешно сканировано {len(successful_tasks)} задач")
                else:
                    log_to_file(log_file, "")
                    log_to_file(log_file, f"❌ [Группа {group}] Все задачи завершились ошибкой.")

            except Exception as e:
                for task in tasks:
                    task.update_after_scan(success=False)
                log_to_file(log_file, "")
                log_to_file(log_file, f"❌ Ошибка сканирования в группе {group}: {e}")

            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, "")


############################################################################################################################################################
#         log_to_file(log_file, "=== 🛠️ Старт фазы преобразования ==="
############################################################################################################################################################

    def process_phase(self):
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "=== 🛠️ Старт фазы обработки ===")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")

        if not self.tasks:
            log_to_file(log_file, "⚪ Нет задач для обработки.")
            log_to_file(log_file, "")
            return

        success_count = 0
        failure_count = 0

        for task in self.tasks:
            try:
                task.process_raw_value()
                try:
                    task.check_for_update()
                    success_count += 1
                except Exception as e:
                    failure_count += 1
                    log_to_file(log_file, f"❌ [Task {task.name_of_process}] Ошибка при проверке обновления: {e}")
            except Exception as e:
                failure_count += 1
                log_to_file(log_file, f"❌ [Task {task.name_of_process}] Ошибка обработки данных: {e}")

        log_to_file(log_file, "")
        log_to_file(log_file, f"✅ Фаза обработки завершена. Успешно: {success_count}, Ошибок: {failure_count}")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")

############################################################################################################################################################
#         log_to_file(log_file, "=== 🛠️ Старт фазы обновления ==="
############################################################################################################################################################

    def update_phase(self):
        log_to_file("scanner.log", "=" * 100)
        log_to_file("scanner.log", "=== 🔄 Старт фазы обновления ===")
        log_to_file("scanner.log", "=" * 100)
        log_to_file("scanner.log", "")

        if not self.tasks:
            log_to_file("scanner.log", "⚪ Нет задач для обновления.")
            log_to_file("scanner.log", "=" * 100)
            log_to_file("scanner.log", "")
            return

        self.rotationsinfo_tasks = []
        self.sheetsinfo_tasks = []

        for task in self.tasks:
            if task.source_table == "RotationsInfo":
                self.rotationsinfo_tasks.append(task)
            elif task.source_table == "SheetsInfo":
                self.sheetsinfo_tasks.append(task)
            else:
                log_to_file("scanner.log", f"⚠️ [Task {task.name_of_process}] Неизвестный тип таблицы: {task.source_table}")

        import threading

        thread_rotationsinfo = threading.Thread(target=self.RotationsInfo_scanner)
        thread_sheetsinfo = threading.Thread(target=self.SheetsInfo_scanner)

        thread_rotationsinfo.start()
        thread_sheetsinfo.start()

        thread_rotationsinfo.join()
        thread_sheetsinfo.join()

        log_to_file("scanner.log", "=" * 100)
        log_to_file("scanner.log", "✅ Фаза обновления завершена.")
        log_to_file("scanner.log", "=" * 100)
        log_to_file("scanner.log", "")

    def _handle_main_updates(self, tasks, log_file):
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "🔄 Начало обновления: update_main")
        log_to_file(log_file, "")

        try:
            # Здесь твоя логика импорта для MAIN
            self.import_main_data(tasks)

            for task in tasks:
                task.update_after_upload(success=True)

            log_to_file(log_file, "")
            log_to_file(log_file, f"✅ Успешно обновлено {len(tasks)} задач update_main")
            time.sleep(5)  # Задержка для предотвращения превышения лимитов API

        except Exception as e:
            for task in tasks:
                task.update_after_upload(success=False)

            log_to_file(log_file, "")
            log_to_file(log_file, f"❌ Ошибка при обновлении update_main: {e}")

        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")


    def _handle_shuffle_updates(self, tasks, log_file):
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "🔄 Начало обновления: update_shuffle")
        log_to_file(log_file, "")

        try:
            # Здесь твоя логика импорта для SHUFFLE
            self.import_shuffle_data(tasks)

            for task in tasks:
                task.update_after_upload(success=True)

            log_to_file(log_file, "")
            log_to_file(log_file, f"✅ Успешно обновлено {len(tasks)} задач update_shuffle")
            time.sleep(5)  # Задержка для предотвращения превышения лимитов API

        except Exception as e:
            for task in tasks:
                task.update_after_upload(success=False)

            log_to_file(log_file, "")
            log_to_file(log_file, f"❌ Ошибка при обновлении update_shuffle: {e}")

        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")


    def _handle_other_updates(self, tasks, log_file):
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "🔄 Начало обновления: другие группы")
        log_to_file(log_file, "")

        try:
            # Здесь твоя логика импорта для прочих задач
            self.import_other_data(tasks)

            for task in tasks:
                task.update_after_upload(success=True)

            log_to_file(log_file, "")
            log_to_file(log_file, f"✅ Успешно обновлено {len(tasks)} задач (другие группы)")
            time.sleep(5)  # Задержка для предотвращения превышения лимитов API

        except Exception as e:
            for task in tasks:
                task.update_after_upload(success=False)

            log_to_file(log_file, "")
            log_to_file(log_file, f"❌ Ошибка при обновлении других групп: {e}")

        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")

    # === Заглушки для реальных импортов (пока можно пустыми сделать) ===


    def import_main_data(self, tasks):
        
        """Импорт данных для update_main"""
        
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

        log_to_file(log_file, "=" * 50)
        log_to_file(log_file, "")
        log_to_file(log_file, "🔄 Старт импорта данных для группы 'update_main'.")
        log_to_file(log_file, "")

        if not tasks:
            log_to_file(log_file, "⚠️ Нет задач для update_main.")
            return

        # Проверяем, есть ли задачи, требующие обновления
        need_update_tasks = [task for task in tasks if task.need_update == 1]
        if not need_update_tasks:
            log_to_file(log_file, "⚪ Нет задач для обновления в группе update_main.")
            return

        # Сортируем задачи в правильном порядке
        task_map = {task.name_of_process: task for task in tasks}
        sorted_tasks = []

        for name in ROTATION_ORDER:
            if name in task_map:
                sorted_tasks.append(task_map[name])
            else:
                log_to_file(log_file, f"⚠️ Задача {name} не найдена среди задач.")

        # Собираем все значения
        all_values = []
        for task in sorted_tasks:
            if task.values_json:
                all_values.extend(task.values_json)
                all_values.append(["" * 23])  # Пустая строка между ротациями
            else:
                log_to_file(log_file, f"⚠️ [Task {task.name_of_process}] Нет данных для вставки.")

        if all_values and all_values[-1] == []:
            all_values.pop()  # Убираем последнюю пустую строку, если она осталась в конце

        # Проверяем длину
        if len(all_values) < 100:
            empty_rows = 100 - len(all_values)
            all_values.extend([[] for _ in range(empty_rows)])
            log_to_file(log_file, f"➕ Добавлены {empty_rows} пустых строк до 100.")
        elif len(all_values) > 100:
            all_values = all_values[:100]
            log_to_file(log_file, "⚠️ Обрезано до 100 строк.")

        spreadsheet_id = sorted_tasks[0].target_doc_id
        target_page_name = sorted_tasks[0].target_page_name
        target_page_area = sorted_tasks[0].target_page_area
        insert_range = f"{target_page_name}!{target_page_area}"

        batch_data = [{
            "range": insert_range,
            "values": all_values
        }]

        success, error = batch_update(self.service, spreadsheet_id, batch_data, log_file)

        if success:
            log_to_file(log_file, "✅ Успешная вставка всех ротаций в update_main.")
            for task in need_update_tasks:
                task.update_after_upload(success=True)
        else:
            log_to_file(log_file, f"❌ Ошибка при вставке update_main: {error}")
            for task in need_update_tasks:
                task.update_after_upload(success=False)

        log_to_file(log_file, "=" * 50)
        log_to_file(log_file, "")


    def import_shuffle_data(self, tasks):
        """Импорт данных для shuffle групп с разделением по update_group."""
        if not tasks:
            log_to_file(log_file, "⚪ Нет задач для импорта shuffle.")
            return

        log_to_file(log_file, "=" * 50)
        log_to_file(log_file, "🔄 Старт импорта данных для группы 'shuffle_rotation'.")
        log_to_file(log_file, "")

        # Группируем задачи по update_group
        update_groups = {}
        for task in tasks:
            update_groups.setdefault(task.update_group, []).append(task)

        # Собираем все данные для одной batchUpdate
        batch_data = []

        for group_name, group_tasks in update_groups.items():
            log_to_file(log_file, "=" * 50)
            log_to_file(log_file, "")
            log_to_file(log_file, f"🔄 Обработка подгруппы: {group_name} ({len(group_tasks)} задач)")
            log_to_file(log_file, "")

            if not group_tasks:
                log_to_file(log_file, f"⚠️ Нет задач в подгруппе {group_name}.")
                continue

            spreadsheet_id = group_tasks[0].target_doc_id
            page_name = group_tasks[0].target_page_name
            page_area = group_tasks[0].target_page_area

            all_page_values = batch_get(self.service, spreadsheet_id, [page_area], log_file)
            sheet_values = next(iter(all_page_values.values()), [])

            shift_row_index = None
            for idx, row in enumerate(sheet_values):
                if row and isinstance(row[0], str) and "shift:" in row[0].lower():
                    shift_row_index = idx + 1
                    break

            if shift_row_index is None:
                log_to_file(log_file, f"❌ Строка 'shift:' не найдена на листе {page_name}.")
                continue

            start_row = shift_row_index
            end_row = start_row + 5
            insert_range = f"{page_name}!D{start_row + 1}:AC{end_row + 1}"  # +1 чтобы попасть в строки для вставки

            log_to_file(log_file, f"📍 Найдена строка 'shift:' на {shift_row_index + 1}-й строке. Вставка группы {group_name} в диапазон {insert_range}.")
            log_to_file(log_file, f"json: {len(group_tasks[0].values_json)} строк")
            log_to_file(log_file, "")

            # Собираем все values из задач
            all_values = []
            for task in group_tasks:
                if task.values_json:
                    all_values.extend(task.values_json)
                else:
                    log_to_file(log_file, f"⚠️ [Task {task.name_of_process}] Нет данных для вставки.")

            if not all_values:
                log_to_file(log_file, f"⚠️ Нет данных для вставки в подгруппе {group_name}.")
                continue

            # Отправляем отдельный batchUpdate на каждую подгруппу
            batch_data = [{
                "range": insert_range,
                "values": all_values
            }]

            success, error = batch_update(self.service, spreadsheet_id, batch_data, log_file)

            if success:
                log_to_file(log_file, f"✅ Успешная вставка данных подгруппы {group_name}.")
                for task in group_tasks:
                    task.update_after_upload(success=True)
            else:
                log_to_file(log_file, f"❌ Ошибка при вставке подгруппы {group_name}: {error}")
                for task in group_tasks:
                    task.update_after_upload(success=False)

            log_to_file(log_file, "=" * 50)
            log_to_file(log_file, "")

        log_to_file(log_file, "✅ Импорт shuffle_rotation завершён.")
        log_to_file(log_file, "=" * 50)
        log_to_file(log_file, "")


    def import_other_data(self, other_tasks: List[Task]):
        """ Импорт других задач с учетом группировки и ошибок """

        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "🔄 Начало обновления: другие группы")
        log_to_file(log_file, "")

        if not other_tasks:
            log_to_file(log_file, "⚪ Нет задач для обновления в других группах.")
            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, "")
            return

        # Группируем по update_group
        update_groups = {}
        for task in other_tasks:
            update_groups.setdefault(task.update_group, []).append(task)

        log_to_file(log_file, "🔄 Старт batchUpdate для группы 'другие задачи'.")

        for group_name, group_tasks in update_groups.items():
            log_to_file(log_file, f"🔄 Обработка группы: {group_name} ({len(group_tasks)} задач)")

            spreadsheet_id = group_tasks[0].target_doc_id

            # Собираем данные для batchUpdate
            batch_data = []
            for task in group_tasks:
                if task.values_json:
                    batch_data.append({
                        "range": f"{task.target_page_name}!{task.target_page_area}",
                        "values": task.values_json
                    })
                else:
                    log_to_file(log_file, f"⚠️ [Task {task.name_of_process}] Нет данных для импорта.")

            if not batch_data:
                log_to_file(log_file, f"⚠️ Нет данных для отправки в группе {group_name}.")
                continue

            # Пытаемся отправить общий запрос
            success, error = batch_update(self.service, spreadsheet_id, batch_data, log_file)

            if success:
                log_to_file(log_file, f"✅ Успешно обновлены данные в группе {group_name} одним запросом.")
            else:
                log_to_file(log_file, f"❌ Ошибка при общем обновлении группы {group_name}: {error}")
                log_to_file(log_file, f"🔄 Переходим к поштучному обновлению задач группы {group_name}.")

                # Переходим к индивидуальной отправке по одной задаче
                for task in group_tasks:
                    if not task.values_json:
                        continue

                    single_range = f"{task.target_page_name}!{task.target_page_area}"
                    single_data = [{
                        "range": single_range,
                        "values": task.values_json
                    }]

                    single_success, single_error = batch_update(self.service, task.target_doc_id, single_data, log_file)

                    if single_success:
                        log_to_file(log_file, f"✅ Успешно обновлена задача [Task {task.name_of_process}] отдельно.")
                    else:
                        log_to_file(log_file, f"❌ Ошибка при обновлении [Task {task.name_of_process}]: {single_error}")

            log_to_file(log_file, "")

        log_to_file(log_file, "✅ Импорт других задач завершён.")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")


    def start(self):
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "=== ▶️ Старт основного цикла сканера ===")
        log_to_file(log_file, "=" * 100)
        log_to_file(log_file, "")
        while True:
            self.scan_phase()
            self.process_phase()
            self.update_phase()
            log_to_file(log_file, "")
            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, "⏳ Ожидание следующего цикла...")
            log_to_file(log_file, "=" * 100)
            log_to_file(log_file, "")
            time.sleep(60)


if __name__ == "__main__":
    manager = TaskManager()
    manager.start()
