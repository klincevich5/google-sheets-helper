# scanners/sheetsinfo_scanner.py

import os
import time
import sqlite3
import json
from collections import defaultdict
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from bot.settings_access import is_scanner_enabled
from core.config import SHEETSINFO_LOG, SHEETSINFO_TOKEN, SHEETINFO_INTERVAL, DB_PATH
from core.data import load_sheetsinfo_tasks
from database.database import insert_usage
from utils.logger import log_to_file, log_separator, log_section

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
                    log_to_file(self.log_file, "⏸ Сканер отключён (sheets_scanner). Ожидание...")
                    time.sleep(10)
                    continue
                log_to_file(self.log_file, "🔄 Новый цикл сканирования SheetsInfo")

                try:
                    self.check_and_refresh_token(SHEETSINFO_TOKEN)
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

    def check_and_refresh_token(self, token_path):
        log_section("🔐 Проверка токена", self.log_file)

        if not os.path.exists(token_path):
            log_to_file(self.log_file, f"❌ Файл токена {token_path} не найден.")
            raise FileNotFoundError(f"{token_path} не найден")

        creds = Credentials.from_authorized_user_file(token_path)

        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_path, "w", encoding="utf-8") as f:
                    f.write(creds.to_json())

                log_to_file(self.log_file, f"🔄 Токен обновлён: {token_path}")

                # ✅ Логируем факт обновления токена
                insert_usage(
                    token=SHEETSINFO_TOKEN,
                    count=1,
                    scan_group="token_refresh",
                    success=True
                )

            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка обновления токена {token_path}: {e}")

                # ❌ Логируем как неудачную попытку обновления токена
                insert_usage(
                    token=SHEETSINFO_TOKEN,
                    count=1,
                    scan_group="token_refresh",
                    success=False
                )
                raise
        else:
            log_to_file(self.log_file, f"✅ Токен {token_path} действителен.")

#############################################################################################
# загрузка задач из БД
#############################################################################################

    def load_tasks(self):
        log_section("🧩 📥 Загрузка задач из SheetsInfo", self.log_file)
        self.tasks = load_sheetsinfo_tasks(self.conn)

        if not self.tasks:
            log_section("⚪ Нет задач для загрузки из SheetsInfo.", self.log_file)
            return

        log_section(f"🔄 Загружено {len(self.tasks)} задач.", self.log_file)
        for task in self.tasks:
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
            return

        log_to_file(self.log_file, f"🔎 Найдено {len(ready_tasks)} задач, готовых к сканированию:")

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
                    task.update_after_scan(success=False) #Обновление в Классе
                    self.update_task_scan_fields(task) #Обновление в БД

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

            response_data = self.batch_get(self.service, doc_id, ranges, scan_group, self.log_file)
            if not response_data:
                log_to_file(self.log_file, "❌ Пустой ответ от batchGet. Все задачи будут отмечены как неудачные.")
                for task in valid_tasks:
                    task.update_after_scan(success=False) #Обновление в Классе
                    self.update_task_scan_fields(task) #Обновление в БД
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
                    task.raw_values_json = matched_values #Сохранение необработанных данных в raw_values_json
                    task.update_after_scan(success=True) #Обновление в Классе
                    self.update_task_scan_fields(task) #Обновление в БД
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

        log_to_file(self.log_file, f"💾 Сохраняю в БД [Task {task.name_of_process}] → proceed={task.proceed} → changed={task.changed}, hash={task.hash}")
        self.conn.commit()

    def batch_get(self, service, spreadsheet_id, ranges, scan_group, log_file, retries=5, delay_seconds=5):
        attempt = 0
        success = False

        while attempt < retries:
            try:
                log_to_file(log_file, f"📡 Пытаюсь выполнить batchGet (попытка {attempt + 1}/{retries}) для документа {spreadsheet_id}")

                result = service.spreadsheets().values().batchGet(
                    spreadsheetId=spreadsheet_id,
                    ranges=ranges,
                    majorDimension="ROWS"
                ).execute()

                value_ranges = result.get("valueRanges", [])
                data = {vr.get("range", ""): vr.get("values", []) for vr in value_ranges}

                log_to_file(log_file, f"✅ Успешный batchGet. Получено {len(data)} диапазонов.")
                success = True
                break  # Выход из цикла после успешного запроса

            except HttpError as e:
                status_code = e.resp.status
                log_to_file(log_file, f"❌ HttpError {status_code} при batchGet: {e}")

                if status_code in (429, 500, 503):
                    attempt += 1
                    log_to_file(log_file, f"⏳ Повтор через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                elif status_code == 401:
                    log_to_file(log_file, "🔒 Ошибка авторизации (401). Прерываю batchGet.")
                    break
                else:
                    break

            except Exception as e:
                if any(x in str(e).lower() for x in ["ssl", "handshake", "decryption", "timed out"]):
                    attempt += 1
                    log_to_file(log_file, f"⏳ Сетевая ошибка '{e}', повтор через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                else:
                    log_to_file(log_file, f"❌ Непредвиденная ошибка batchGet: {e}")
                    break

        # ✅ Универсальное логирование использования токена
        insert_usage(
            token=SHEETSINFO_TOKEN,
            count=attempt + 1,           # total попыток, включая финальную
            scan_group=scan_group,
            success=success
        )

        if success:
            return data
        else:
            log_to_file(log_file, "❌ batchGet завершён неудачно.")
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
                log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Задача не была успешно отсканирована. Пропуск.")
                continue

            try:
                log_to_file(self.log_file, f"🔧 Обработка задачи [Task {task.name_of_process}]...")

                try:
                    task.process_raw_value() # Обработка данных и сохранение в values_json
                    
                    log_to_file(self.log_file, f"📦 [Task {task.name_of_process}] После обработки: {len(task.values_json)} строк.")
                    # for i, row in enumerate(task.values_json[:5]):
                    #     log_to_file(self.log_file, f"      [{i+1}] {row}")
                    # if len(task.values_json) > 5:
                    #     log_to_file(self.log_file, f"      ...ещё {len(task.values_json) - 5} строк скрыто")
                except Exception as e:
                    log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка в process_raw_value: {e}")
                    continue

                try:
                    task.check_for_update()

                    if task.changed:
                        log_to_file(self.log_file, "🔁 Изменения обнаружены — задача будет обновлена.")
                        self.update_task_process_fields(task) # Обновление в БД
                        log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Успешно обработана и записана в БД.\n")
                    else:
                        log_to_file(self.log_file, "⚪ Изменений нет — обновление не требуется.\n")
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
            json.dumps(task.values_json) if task.values_json else None,
            task.id
        ))
        log_to_file(self.log_file, f"💾 Обновлён values_json и hash для задачи {task.name_of_process}")
        self.conn.commit()

#############################################################################################
# Фаза обновления
#############################################################################################

    def update_phase(self):
        log_section("🔼 Фаза обновления", self.log_file)
        # time.sleep(SHEETINFO_INTERVAL)
        # return  # Закомментировано для тестирования

        has_tasks_changes = any(task.changed for task in self.tasks if task.update_group != "update_mistakes_in_db")
        log_to_file(self.log_file, f"🔼 Обнаружены изменения в задачах: {has_tasks_changes}")
        tasks_to_update = [task for task in self.tasks if task.values_json and task.update_group != "update_mistakes_in_db" and has_tasks_changes]
        log_to_file(self.log_file, f"🔼 Задач для обновления: {len(tasks_to_update)}")


        has_mistakes_changes = any(task.changed for task in self.tasks if task.update_group == "update_mistakes_in_db")
        log_to_file(self.log_file, f"🔼 Обнаружены изменения в ошибках: {has_mistakes_changes}")
        mistakes_to_update = [task for task in self.tasks if task.values_json and task.update_group == "update_mistakes_in_db" and has_mistakes_changes]
        log_to_file(self.log_file, f"🔼 Ошибок для обновления: {len(mistakes_to_update)}")

        # if tasks_to_update:
        #         try:
        #             self.import_tasks_to_update(tasks_to_update)
        #         except Exception as e:
        #             log_to_file(self.log_file, f"❌ Ошибка при обновлении tasks_to_update: {e}")

        if mistakes_to_update:
            try:
                self.import_mistakes_to_update(mistakes_to_update)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении mistakes_to_update: {e}")
        if not tasks_to_update and not mistakes_to_update:
            log_to_file(self.log_file, "⚪ Нет задач для обновления. Пропуск.")
            return
        else:
            log_to_file(self.log_file, "🔼 Обновление завершено.")
        
        time.sleep(SHEETINFO_INTERVAL)

##############################################################################################
# Импорт Обычных задач 
##############################################################################################

    def import_tasks_to_update(self, tasks_to_update):

        log_to_file(self.log_file, f"🔄 Начало фазы tasks_to_update. Задач для выгрузки: {len(tasks_to_update)}.")

        tasks_by_update_group = defaultdict(list)
        for task in tasks_to_update:
            tasks_by_update_group[task.update_group].append(task)

        for update_group, group_tasks in tasks_by_update_group.items():
            log_section(f"🔄 Обработка группы обновления: {update_group} ({len(group_tasks)} задач).", self.log_file)

            doc_id = group_tasks[0].target_doc_id

            batch_data = []
            for task in group_tasks:
                if not task.values_json:
                    log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет данных для отправки, пропуск.")
                    continue

                batch_data.append({
                    "range": f"{task.target_page_name}!{task.target_page_area}",
                    "values": task.values_json
                })

            if not batch_data:
                log_to_file(self.log_file, "⚪ Нет данных для batchUpdate в этой группе.")
                continue

            success, error = self.batch_update(self.service, doc_id, batch_data, update_group, self.log_file)

            if success:
                for task in group_tasks:
                    task.update_after_upload(success=True)
                    self.update_task_update_fields(task)
                    insert_usage(
                        token=SHEETSINFO_TOKEN,
                        count=1,
                        scan_group=update_group,
                        success=True
                    )
                log_to_file(self.log_file, f"✅ Успешное обновление группы {update_group} ({len(group_tasks)} задач).")
            else:
                log_to_file(self.log_file, f"❌ Ошибка batchUpdate: {error}")
                log_to_file(self.log_file, "🔄 Переходим на поштучную отправку задач.")

                for task in group_tasks:
                    if not task.values_json:
                        continue

                    single_data = [{
                        "range": f"{task.target_page_name}!{task.target_page_area}",
                        "values": task.values_json
                    }]
                    single_success, single_error = self.batch_update(self.service, doc_id, single_data, update_group, self.log_file)

                    insert_usage(
                        token=SHEETSINFO_TOKEN,
                        count=1,
                        scan_group=update_group,
                        success=single_success
                    )

                    if single_success:
                        task.update_after_upload(success=True)
                        log_to_file(self.log_file, f"✅ Успешно обновлена задача [Task {task.name_of_process}] отдельно.")
                        log_separator(self.log_file)
                        log_to_file(self.log_file, "" * 100)
                    else:
                        task.update_after_upload(success=False)
                        log_to_file(self.log_file, f"❌ Ошибка обновления [Task {task.name_of_process}] отдельно: {single_error}")
                        log_separator(self.log_file)
                        log_to_file(self.log_file, "" * 100)

                    self.update_task_update_fields(task)

                    log_to_file(self.log_file, f"💾 Обновлён values_json и hash для задачи {task.name_of_process}")

###############################################################################################
# Импорт Ошибок в БД
###############################################################################################
    @staticmethod
    def get_floor_by_table_name(table_name: str, floor_map: dict) -> str:
        for floor, tables in floor_map.items():
            if table_name in tables:
                return floor
        return "UNKNOWN"

    @staticmethod
    def get_max_last_row(conn, table_name):
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(last_row) FROM MistakeStorage WHERE table_name = ?", (table_name,))
        result = cursor.fetchone()
        return result[0] if result and result[0] is not None else 0

    def import_mistakes_to_update(self, mistakes_to_update):
        log_to_file(self.log_file, f"🔄 Начало фазы mistakes_to_update. Задач для выгрузки: {len(mistakes_to_update)}.")

        floor_list = {
            "VIP": ["vBJ2", "vBJ3", "gBC1", "vBC3", "vBC4", "vHSB1", "vDT1", "gsRL1", "swBC1", "swRL1"],
            "TURKISH": ["tBJ1", "tBJ2", "tRL1"],
            "GENERIC": ["gBJ1", "gBJ3", "gBJ4", "gBJ5", "gBC2", "gBC3", "gBC4", "gBC5", "gBC6", "gRL1", "gRL2"],
            "GSBJ": ["gsBJ1", "gsBJ2", "gsBJ3", "gsBJ4", "gsBJ5", "gRL3"],
            "LEGENDZ": ["lBJ1", "lBJ2", "lBJ3"]
        }

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        try:
            for task in mistakes_to_update:
                sheet = task.raw_values_json
                page_name = task.source_page_name
                floor = self.get_floor_by_table_name(page_name, floor_list)
                max_row_in_db = self.get_max_last_row(conn, page_name)

                for row_index, row in enumerate(sheet[1:], start=2):  # Пропускаем заголовок
                    if row_index <= max_row_in_db:
                        continue

                    # Защита от пустых строк
                    if not row or len(row) < 8:
                        log_to_file(self.log_file, f"⚠️ Пропущена пустая или неполная строка {row_index} из {page_name}: {row}")
                        continue

                    try:
                        is_cancel = 1 if str(row[5]).strip().lower() == "cancel" else 0

                        cursor.execute("""
                            INSERT INTO MistakeStorage (
                                floor, table_name, date, time, game_id, mistake, type,
                                is_cancel, dealer, sm, last_row
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            floor,
                            page_name,
                            row[0],  # date
                            row[1],  # time
                            row[2],  # game_id
                            row[3],  # mistake
                            row[4],  # type
                            is_cancel,
                            row[6],  # dealer
                            row[7],  # sm
                            row_index
                        ))

                    except Exception as row_err:
                        log_to_file(self.log_file, f"❌ Ошибка при добавлении строки {row_index} из {page_name}: {row_err}. Строка: {row}")
                        continue  # просто пропускаем строку и едем дальше

            conn.commit()
            log_to_file(self.log_file, "✅ Все ошибки успешно импортированы.")

        except Exception as task_err:
            log_to_file(self.log_file, f"❌ Ошибка при обработке mistakes_to_update: {task_err}")

        finally:
            conn.close()
            log_to_file(self.log_file, "🔄 Завершение фазы mistakes_to_update.")


###############################################################################################
# batchUpdate для обновления данных в Google Sheets
###############################################################################################

    def batch_update(self, service, spreadsheet_id, batch_data, update_group, log_file, retries=3, delay_seconds=10):
        success = False
        attempt = 0

        while attempt < retries:
            try:
                log_to_file(log_file, f"📤 Пытаюсь выполнить batchUpdate (попытка {attempt + 1}/{retries}) для документа {spreadsheet_id}")

                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        "valueInputOption": "USER_ENTERED",
                        "data": batch_data
                    }
                ).execute()

                log_to_file(log_file, "✅ Успешный batchUpdate.")
                success = True
                break  # завершили успешно

            except HttpError as e:
                status = e.resp.status
                log_to_file(log_file, f"❌ HTTP {status}: {e}")

                if status in [429, 500, 503]:
                    attempt += 1
                    log_to_file(log_file, f"⏳ Повтор через {delay_seconds} сек...")
                    time.sleep(delay_seconds)
                else:
                    break

            except Exception as e:
                log_to_file(log_file, f"❌ Ошибка: {e}")
                break

        # ✅ Логируем использование токена
        insert_usage(
            token=SHEETSINFO_TOKEN,
            count=attempt + 1,
            update_group=update_group,
            success=success
        )

        if success:
            return True, None
        else:
            return False, "Превышено число попыток" if attempt == retries else "Ошибка запроса"

    def update_task_update_fields(self, task):
        cursor = self.conn.cursor()
        table = "SheetsInfo"
        cursor.execute(f"""
            UPDATE {table}
            SET
                last_update = ?,
                update_quantity = ?,
                update_failures = ?
            WHERE id = ?
        """, (
            task.last_update.isoformat() if task.last_update else None,
            task.update_quantity,
            task.update_failures,
            task.id
        ))
        self.conn.commit()