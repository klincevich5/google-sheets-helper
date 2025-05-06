# sheetsinfo_scanner.py

import os
import time
from collections import defaultdict
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from settings_access import is_scanner_enabled
from config import SHEETSINFO_LOG, TOKEN_PATH
from data import load_sheetsinfo_tasks
from logger import log_to_file, log_separator, log_section

class SheetsInfoScanner:
    def __init__(self, conn, service, doc_id_map):
        self.conn = conn
        self.service = service
        self.doc_id_map = doc_id_map
        self.tasks = []
        self.log_file = SHEETSINFO_LOG
        self.keep_running = True

    def run(self):

        while True:
            try:
                
                if not is_scanner_enabled("sheets_scanner"):
                    log_to_file(self.log_file, "⏸ Сканер отключён (rotations_scanner). Ожидание...")
                    time.sleep(10)
                    continue
                log_to_file(self.log_file, "🔄 Новый цикл сканирования RotationsInfo")

                try:
                    self.check_and_refresh_token()
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка на этапе обновления токена: {e}")
                    raise

                try:
                    self.load_tasks()
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка на этапе загрузки задач: {e}")
                    raise

                try:
                    self.scan_phase()
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка на этапе сканирования: {e}")
                    raise

                try:
                    self.process_phase()
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка на этапе обработки: {e}")
                    raise

                try:
                    self.update_phase()
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка на этапе обновления: {e}")
                    raise

                time.sleep(60)

            except Exception as e:
                log_separator(self.log_file)
                log_to_file(self.log_file, f"❌ Критическая ошибка в основном цикле: {e}")
                time.sleep(10)

############################################################################################
# проверка токена и обновление
############################################################################################

    def check_and_refresh_token(self):
        log_section("🔐 Проверка работоспособности Google API токена", self.log_file)

        if not os.path.exists(TOKEN_PATH):
            log_to_file(self.log_file, f"❌ Файл {TOKEN_PATH} не найден. Авторизация невозможна.")
            raise FileNotFoundError(f"{TOKEN_PATH} не найден")

        creds = Credentials.from_authorized_user_file(TOKEN_PATH)
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(TOKEN_PATH, 'w') as token_file:
                    token_file.write(creds.to_json())
                log_to_file(self.log_file, f"🔄 Токен успешно обновлён и сохранён в {TOKEN_PATH}")
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка обновления токена: {e}")
                raise
        else:
            log_to_file(self.log_file, "✅ Токен действителен.")

#############################################################################################
# загрузка задач из БД
#############################################################################################

    def load_tasks(self):
        log_section("🧩 📥 Загрузка задач из SheetsInfo", self.log_file)
        self.tasks = load_sheetsinfo_tasks(self.conn)

        if not self.tasks:
            log_section(self.log_file, "⚪ Нет задач для загрузки из SheetsInfo.")
            return

        log_section(self.log_file, f"🔄 Загружено {len(self.tasks)} задач.")
        for task in self.tasks:
            log_to_file(self.log_file, f"   • [Task] {task.source_table_type} | Страница: {task.source_page_name} | Диапазон: {task.source_page_area}")
            task.assign_doc_ids(self.doc_id_map)

#############################################################################################
# Фаза сканирования
#############################################################################################

    def scan_phase(self):
        log_section("🔍 Фаза сканирования", self.log_file)

        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для сканирования.")
            return

        ready_tasks = [task for task in self.tasks if task.is_ready_to_scan()]
        if not ready_tasks:
            log_to_file(self.log_file, "⚪ Нет задач, готовых к сканированию.")
            self.metrics_scan = {"ready": 0, "success": 0, "failed": 0}
            return

        log_to_file(self.log_file, f"🔎 Найдено {len(ready_tasks)} задач, готовых к сканированию:")
        # for task in ready_tasks:
        #     log_to_file(self.log_file, f"   • [Task] {task.name_of_process} | Страница: {task.source_page_name} | Диапазон: {task.source_page_area}")

        scan_groups = defaultdict(list)
        for task in ready_tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process}] Не удалось сопоставить doc_id. Пропуск.")
                continue
            scan_groups[task.scan_group].append(task)

        for scan_group, group_tasks in scan_groups.items():
            log_separator(self.log_file)
            log_to_file(self.log_file, f"📘 Обработка scan_group: {scan_group} ({len(group_tasks)} задач)")

            if not group_tasks:
                log_to_file(self.log_file, "⚪ В группе нет задач.")
                continue

            doc_id = group_tasks[0].source_doc_id
            unique_sheet_names = set(task.source_page_name for task in group_tasks)
            log_to_file(self.log_file, f"Уникальные названия листов: {unique_sheet_names}")

            exists_map = {}
            for sheet_name in unique_sheet_names:
                exists_map[sheet_name] = self.check_sheet_exists(doc_id, sheet_name)
                log_to_file(self.log_file, f"{'✅' if exists_map[sheet_name] else '⚠️'} Лист '{sheet_name}' {'существует' if exists_map[sheet_name] else 'не найден'}.")

            valid_tasks = []
            for task in group_tasks:
                sheet_name = task.source_page_name
                if exists_map.get(sheet_name):
                    log_to_file(self.log_file, f"➡️ Используем '{sheet_name}' для задачи {task.name_of_process}.")
                    valid_tasks.append(task)
                else:
                    log_to_file(self.log_file, f"⛔ Пропуск задачи {task.name_of_process}: лист '{sheet_name}' не найден.")
                    task.update_after_scan(success=False)
                    self.update_task_scan_fields(task)

            if not valid_tasks:
                log_to_file(self.log_file, f"⚪ Все задачи группы {scan_group} отфильтрованы. Пропуск batchGet.")
                continue

            range_to_tasks = defaultdict(list)
            for task in valid_tasks:
                range_str = f"{task.source_page_name}!{task.source_page_area}"
                range_to_tasks[range_str].append(task)

            ranges = list(range_to_tasks.keys())

            log_to_file(self.log_file, "")

            log_to_file(self.log_file, f"📤 Отправка batchGet на документ {task.source_table_type} с {len(ranges)} уникальными диапазонами:")
            for r in ranges:
                log_to_file(self.log_file, f"   • {r}")

            response_data = self.batch_get(self.service, doc_id, ranges, self.log_file)

            if not response_data:
                log_to_file(self.log_file, "❌ Пустой ответ от batchGet. Все задачи будут отмечены как неудачные.")
                for task in valid_tasks:
                    task.update_after_scan(success=False)
                    self.update_task_scan_fields(task)
                continue

            normalized_response = {}
            for k, v in response_data.items():
                clean_key = k.replace("'", "")
                if "!" in clean_key:
                    sheet_name, cells_range = clean_key.split("!", 1)
                    normalized_response[(sheet_name.strip(), cells_range.strip())] = v

            log_to_file(self.log_file, "")
            log_to_file(self.log_file, f"📥 Получены диапазоны: {list(normalized_response.keys())}")

            for task in valid_tasks:
                expected_sheet = task.source_page_name.strip()
                expected_area_start = task.source_page_area.split(":")[0].strip()

                matched_values = None
                for (sheet_name, cells_range), values in normalized_response.items():
                    if sheet_name == expected_sheet and cells_range.startswith(expected_area_start):
                        matched_values = values
                        break

                if matched_values:
                    task.raw_values_json = matched_values
                    task.update_after_scan(success=True)
                    self.update_task_scan_fields(task)
                    log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Найден диапазон {sheet_name}!{cells_range}, строк: {len(matched_values)}")
                else:
                    task.update_after_scan(success=False)
                    self.update_task_scan_fields(task)
                    log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process}] Диапазон {expected_sheet}!{task.source_page_area} не найден или пуст.")

    def check_sheet_exists(self, spreadsheet_id, sheet_name):
        try:
            metadata = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheets = metadata.get('sheets', [])
            for sheet in sheets:
                title = sheet.get('properties', {}).get('title')
                if title == sheet_name:
                    return True
            return False
        except Exception as e:
            log_to_file(self.log_file, f"❌ Ошибка при проверке листа в {spreadsheet_id}: {e}")
            return False

    def update_task_scan_fields(self, task):
        cursor = self.conn.cursor()
        table_name = "SheetsInfo"

        cursor.execute(f"""
            UPDATE {table_name}
            SET
                last_scan = ?,
                scan_quantity = ?,
                scan_failures = ?
            WHERE id = ?
        """, (
            task.last_scan.isoformat() if task.last_scan else None,
            task.scan_quantity,
            task.scan_failures,
            task.id
        ))

        log_to_file(self.log_file, f"💾 Сохраняю в БД [Task {task.name_of_process}] → proceed_and_changed={task.proceed_and_changed}, hash={task.hash}")
        self.conn.commit()

    def batch_get(self, service, spreadsheet_id, ranges, log_file, retries=5, delay_seconds=5):
        attempt = 0
        while attempt < retries:
            try:
                log_to_file(log_file, f"📡 Пытаюсь выполнить batchGet (попытка {attempt+1}/{retries}) для документа {spreadsheet_id}")
                result = service.spreadsheets().values().batchGet(
                    spreadsheetId=spreadsheet_id,
                    ranges=ranges,
                    majorDimension="ROWS"
                ).execute()
                value_ranges = result.get("valueRanges", [])
                data = {vr.get("range", ""): vr.get("values", []) for vr in value_ranges}
                log_to_file(log_file, f"✅ Успешный batchGet. Получено {len(data)} диапазонов.")
                return data
            except HttpError as e:
                status_code = e.resp.status
                log_to_file(log_file, f"❌ HttpError {status_code} при batchGet: {e}")
                if status_code in (429, 500, 503):
                    attempt += 1
                    log_to_file(log_file, f"⏳ Повтор через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                elif status_code == 401:
                    log_to_file(log_file, "🔒 Ошибка авторизации (401). Прерываю batchGet.")
                    return {}
                else:
                    return {}
            except Exception as e:
                if any(x in str(e) for x in ["SSL", "handshake", "decryption", "timed out"]):
                    attempt += 1
                    log_to_file(log_file, f"⏳ Сетевая ошибка '{e}', повтор через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                else:
                    log_to_file(log_file, f"❌ Непредвиденная ошибка batchGet: {e}")
                    return {}
        log_to_file(log_file, "❌ Превышено количество попыток batchGet.")
        return {}

#############################################################################################
# Фаза обработки
#############################################################################################

    def process_phase(self):
        log_section("🛠️ Фаза обработки", self.log_file)

        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обработки.")
            return
        
        for task in self.tasks:
            if task.scanned == 0:
                log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Задача не была отсканирована. Пропуск.")
                continue

            try:
                log_to_file(self.log_file, f"🔧 Обработка задачи [Task {task.name_of_process}]...")

                try:
                    task.process_raw_value()
                    log_to_file(self.log_file, f"📦 [Task {task.name_of_process}] После обработки: {len(task.values_json)} строк.")
                    for i, row in enumerate(task.values_json[:5]):
                        log_to_file(self.log_file, f"      [{i+1}] {row}")
                    if len(task.values_json) > 5:
                        log_to_file(self.log_file, f"      ...ещё {len(task.values_json) - 5} строк скрыто")
                except Exception as e:
                    log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка в process_raw_value: {e}")
                    continue

                try:
                    old_hash = task.hash
                    task.check_for_update()
                    new_hash = task.hash

                    log_to_file(self.log_file, "")
                    log_to_file(self.log_file, f"🧮 Сравнение хеша [Task {task.name_of_process}]:")
                    log_to_file(self.log_file, f"     • Старый хеш: {old_hash}")
                    log_to_file(self.log_file, f"     • Новый хеш : {new_hash}")
                    log_to_file(self.log_file, "")

                    if task.proceed_and_changed:
                        log_to_file(self.log_file, "🔁 Изменения обнаружены — задача будет обновлена.")
                        self.update_task_process_fields(task)
                        log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Успешно обработана и записана в БД.")
                    else:
                        log_to_file(self.log_file, "⚪ Изменений нет — обновление не требуется.")
                except Exception as e:
                    log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка в check_for_update: {e}")
                    continue

            except Exception as e:
                log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка обработки: {e}")

    def update_task_process_fields(self, task):
        cursor = self.conn.cursor()
        table = "SheetsInfo"
        cursor.execute(f"""
            UPDATE {table}
            SET
                hash = ?,
                values_json = ?
            WHERE id = ?
        """, (
            task.hash,
            str(task.values_json) if task.values_json else None,
            task.id
        ))
        log_to_file(self.log_file, f"💾 Обновлён values_json и hash для задачи {task.name_of_process}")
        self.conn.commit()

#############################################################################################
# Фаза обновления
#############################################################################################

    def update_phase(self):
        log_section("🔼 Фаза обновления", self.log_file)

        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обновления.")
            return

        tasks_to_update = [task for task in self.tasks if task.proceed_and_changed == 1]

        if not tasks_to_update:
            log_to_file(self.log_file, "⚪ Нет задач, требующих обновления в Google Sheets.")
            return

        log_to_file(self.log_file, f"🔄 Начало фазы обновления. Задач для выгрузки: {len(tasks_to_update)}.")
        log_to_file(self.log_file, "=" * 100)

        tasks_by_update_group = defaultdict(list)
        for task in tasks_to_update:
            tasks_by_update_group[task.update_group].append(task)

        for update_group, group_tasks in tasks_by_update_group.items():
            log_section(f"🔄 Обработка группы обновления: {update_group} ({len(group_tasks)} задач).", self.log_file)

            tasks_by_doc = defaultdict(list)
            for task in group_tasks:
                tasks_by_doc[task.target_doc_id].append(task)

            for doc_id, tasks in tasks_by_doc.items():
                log_to_file(self.log_file, f"📄 Работаем с документом {task.source_table_type} ({len(tasks)} задач).")

                batch_data = []
                for task in tasks:
                    if not task.values_json:
                        log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет данных для отправки, пропуск.")
                        continue
                    batch_data.append({
                        "range": f"{task.target_page_name}!{task.target_page_area}",
                        "values": task.values_json
                    })

                if not batch_data:
                    log_to_file(self.log_file, "⚪ Нет данных для batchUpdate.")
                    continue

                success, error = self.batch_update(self.service, doc_id, batch_data, self.log_file)

                if success:
                    for task in tasks:
                        task.update_after_upload(success=True)
                        self.update_task_update_fields(task)
                    log_to_file(self.log_file, f"✅ Успешное обновление группы ({len(tasks)} задач) одним запросом.")
                else:
                    log_to_file(self.log_file, f"❌ Ошибка batchUpdate: {error}")
                    log_to_file(self.log_file, "🔄 Переходим на поштучную отправку задач.")

                    for task in tasks:
                        if not task.values_json:
                            continue

                        single_data = [{
                            "range": f"{task.target_page_name}!{task.target_page_area}",
                            "values": task.values_json
                        }]
                        single_success, single_error = self.batch_update(self.service, doc_id, single_data, self.log_file)

                        if single_success:
                            task.update_after_upload(success=True)
                            log_to_file(self.log_file, f"✅ Успешно обновлена задача [Task {task.name_of_process}] отдельно.")
                        else:
                            task.update_after_upload(success=False)
                            log_to_file(self.log_file, f"❌ Ошибка обновления [Task {task.name_of_process}] отдельно: {single_error}")

                        self.update_task_update_fields(task)

                time.sleep(2)  # Пауза между группами

    def batch_update(self, service, spreadsheet_id, batch_data, log_file, retries=3, delay_seconds=10):
        for attempt in range(retries):
            try:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        "valueInputOption": "USER_ENTERED",
                        "data": batch_data
                    }
                ).execute()
                return True, None
            except HttpError as e:
                status = e.resp.status
                log_to_file(log_file, f"❌ HTTP {status}: {e}")
                if status in [429, 500, 503]:
                    log_to_file(log_file, f"⏳ Повтор через {delay_seconds} сек...")
                    time.sleep(delay_seconds)
                else:
                    return False, str(e)
            except Exception as e:
                log_to_file(log_file, f"❌ Ошибка: {e}")
                return False, str(e)
        return False, "Превышено число попыток"

    def update_task_update_fields(self, task):
        cursor = self.conn.cursor()
        table = "SheetsInfo"
        cursor.execute(f"""
            UPDATE {table}
            SET
                last_update = ?,
                update_quantity = ?,
                update_failures = ?,
            WHERE id = ?
        """, (
            task.last_update.isoformat() if task.last_update else None,
            task.update_quantity,
            task.update_failures,
            task.id
        ))
        log_to_file(self.log_file, f"💾 Обновлён статус обновления для задачи {task.name_of_process}")
        self.conn.commit()
