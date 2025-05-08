# rotationsinfo_scanner.py

import os
import time
import json
from collections import defaultdict
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from utils.formatting_utils import build_formatting_requests
from bot.settings_access import is_scanner_enabled
from core.config import ROTATIONSINFO_LOG, ROTATIONSINFO_INTERVAL
from core.data import load_rotationsinfo_tasks
from database.database import insert_usage
from utils.logger import log_to_file, log_separator, log_section
from core.token_manager import TokenManager
from utils.utils import load_credentials

class RotationsInfoScanner:
    def __init__(self, conn, token_map, doc_id_map):
        self.conn = conn
        self.token_map = token_map  # передаётся из main.py
        self.doc_id_map = doc_id_map
        self.log_file = ROTATIONSINFO_LOG
        self.tasks = []

    def run(self):
        manager = TokenManager(self.token_map)

        while True:
            try:
                if not is_scanner_enabled("rotations_scanner"):
                    log_to_file(self.log_file, "⏸ Сканер отключён (rotations_scanner). Ожидание...")
                    time.sleep(10)
                    continue
                else:
                    log_to_file(self.log_file, "▶️ Новый цикл сканирования RotationsInfo")

                # 🔁 Выбор токена каждый цикл
                self.token_name, token_path = manager.select_best_token(self.log_file)
                self.service = load_credentials(token_path, self.log_file)
                log_to_file(self.log_file, f"🔐 Используется токен: {self.token_name}")

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
                    token=self.token_name,
                    count=1,
                    scan_group="token_refresh",
                    success=True
                )

            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка обновления токена {token_path}: {e}")

                # ❌ Логируем как неудачную попытку обновления токена
                insert_usage(
                    token=self.token_name,
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
        log_section("🧩 📥 Загрузка задач из RotationsInfo", self.log_file)
        self.tasks = load_rotationsinfo_tasks(self.conn)

        if not self.tasks:
            log_section("⚪ Нет задач для загрузки из RotationsInfo.", self.log_file)
            return

        log_section(f"🔄 Загружено {len(self.tasks)} задач.", self.log_file)
        for task in self.tasks:
            # log_to_file(self.log_file, f"   • [Task] {task.source_table_type} | Страница: {task.source_page_name} | Диапазон: {task.source_page_area}")
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
        table_name = "RotationsInfo"

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
            token=self.token_name,
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
        table = "RotationsInfo"
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

        has_main_changes = any(task.changed for task in self.tasks if task.update_group == "update_main")
        has_shuffle_changes = any(task.changed for task in self.tasks if "shuffle" in task.update_group)

        main_tasks = [task for task in self.tasks if task.update_group == "update_main" and task.values_json and has_main_changes]
        shuffle_tasks = [task for task in self.tasks if "shuffle" in task.update_group and task.values_json and has_shuffle_changes]

        if main_tasks:
                try:
                    self.import_main_data(main_tasks)
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка при обновлении update_main: {e}")

        if shuffle_tasks:
            try:
                self.import_shuffle_data(shuffle_tasks)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении update_shuffle: {e}")
        if not main_tasks and not shuffle_tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обновления. Пропуск.")
            return
        else:
            log_to_file(self.log_file, "🔼 Обновление завершено.")

##############################################################################################
# Импорт данных в Main
##############################################################################################

    def import_main_data(self, all_main_tasks):
        log_section("📥 Импорт данных для update_main", self.log_file)

        grouped_by_page = defaultdict(list)
        
        for task in all_main_tasks:
            grouped_by_page[task.target_page_name].append(task)

        for page_name, tasks in grouped_by_page.items():
            log_to_file(self.log_file, f"📄 Лист: {page_name} ({len(tasks)} задач)")

            ROTATION_ORDER = [
                "SHUFFLE Main", "VIP Main", "TURKISH Main", "GENERIC Main",
                "GSBJ Main", "LEGENDZ Main", "TRI-STAR Main", "TritonRL Main"
            ]

            task_map = {task.name_of_process: task for task in tasks}
            sorted_tasks = []
            all_values = []

            for name in ROTATION_ORDER:
                task = task_map.get(name)
                if not task:
                    log_to_file(self.log_file, f"⚠️ Задача '{name}' не найдена.")
                    continue

                values = task.values_json
                if not values or not isinstance(values, list):
                    log_to_file(self.log_file, f"⚪ [Task {name}] нет корректных данных. Пропуск.")
                    task.update_after_upload(False)
                    self.update_task_update_fields(task)
                    continue

                flat = [str(cell).strip().upper() for row in values for cell in row if cell is not None]
                if flat == ["NULL"]:
                    log_to_file(self.log_file, f"⚪ [Task {name}] содержит 'NULL'. Пропуск.")
                    task.update_after_upload(False)
                    self.update_task_update_fields(task)
                    continue

                log_to_file(self.log_file, f"📦 [Task {name}] — {len(values)} строк (🔄 новые данные)")
                sorted_tasks.append(task)
                all_values.extend(values)
                all_values.append([""] * 26)

            if not sorted_tasks:
                log_to_file(self.log_file, f"⚪ Нет задач с данными для вставки. Пропуск смены {page_name}.")
                continue

            if all_values[-1] == [""] * 26:
                all_values.pop()

            if len(all_values) < 100:
                padding = 100 - len(all_values)
                all_values.extend([[""] * 26 for _ in range(padding)])
                log_to_file(self.log_file, f"➕ Добавлены {padding} пустых строк до 100.")
            elif len(all_values) > 100:
                all_values = all_values[:100]
                log_to_file(self.log_file, "⚠️ Обрезано до 100 строк.")

            reference_task = sorted_tasks[0]
            spreadsheet_id = reference_task.target_doc_id
            target_page_area = reference_task.target_page_area
            insert_range = f"{page_name}!{target_page_area}"

            log_to_file(self.log_file, f"📤 Вставка итогового блока из {len(all_values)} строк в диапазон {insert_range}.")
            batch_data = [{
                "range": insert_range,
                "values": all_values
            }]

            # Вставка данных
            success, error = self.batch_update(self.service, spreadsheet_id, batch_data, reference_task.update_group, self.log_file)

            # ✅ Логируем API-вызов вставки данных
            insert_usage(
                token=self.token_name,
                count=1,
                scan_group=reference_task.update_group,
                success=success
            )

            # Применение цветового форматирования
            try:
                sheet_metadata = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                sheet_id = next(
                    (s["properties"]["sheetId"] for s in sheet_metadata["sheets"] if s["properties"]["title"] == page_name),
                    None
                )

                if sheet_id is None:
                    raise ValueError(f"Sheet '{page_name}' not found in spreadsheet")

                formatting_requests = build_formatting_requests(all_values, sheet_id)

                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": formatting_requests}
                ).execute()

                log_to_file(self.log_file, f"🎨 Применено {len(formatting_requests)} форматирований для листа '{page_name}'.")

                # ✅ Логируем API-вызов раскраски
                insert_usage(
                    token=self.token_name,
                    count=1,
                    scan_group=reference_task.update_group,
                    success=True
                )

            except Exception as e:
                log_to_file(self.log_file, f"⚠️ Ошибка при применении форматирования: {e}")
                insert_usage(
                    token=self.token_name,
                    count=1,
                    scan_group=reference_task.update_group,
                    success=False
                )

            for task in sorted_tasks:
                task.update_after_upload(success)
                self.update_task_update_fields(task)

            if success:
                log_to_file(self.log_file, f"✅ Вставка смены {page_name} завершена успешно ({len(sorted_tasks)} задач).\n")
            else:
                log_to_file(self.log_file, f"❌ Ошибка при вставке смены {page_name}: {error}\n")

##############################################################################################
# Импорт Shuffle в ротации
##############################################################################################
    
    def import_shuffle_data(self, tasks):
        log_section("📥 Импорт данных для update_shuffle", self.log_file)

        shuffle_groups = defaultdict(list)
        for task in tasks:
            shuffle_groups[task.update_group].append(task)

        for update_group, group_tasks in shuffle_groups.items():
            # 🔄 Группируем внутри группы задачи по target_page_name (DAY 1, NIGHT 1 и т.п.)
            pages = defaultdict(list)
            for task in group_tasks:
                pages[task.target_page_name].append(task)

            for page_name, page_tasks in pages.items():
                reference_task = page_tasks[0]
                spreadsheet_id = page_tasks[0].target_doc_id

                log_to_file(self.log_file, f"📄 Документ: {page_tasks[0].name_of_process}, страница: {page_name}\n")

                try:
                    raw = self.batch_get(
                        self.service,
                        spreadsheet_id,
                        [f"{page_name}!D1:AC200"],
                        reference_task.update_group,
                        self.log_file
                    )
                    sheet_values = list(raw.values())[0] if raw else []

                    # 🔍 Поиск строки с "shift:"
                    shift_row_index = None
                    for idx, row in enumerate(sheet_values):
                        if row and isinstance(row[0], str) and "shift:" in row[0].lower():
                            shift_row_index = idx + 1
                            break

                    if shift_row_index is None:
                        log_to_file(self.log_file, f"\n❌ Строка с 'shift:' не найдена. Пропуск страницы {page_name}.")
                        for task in page_tasks:
                            task.update_after_upload(False)
                            self.update_task_update_fields(task)
                        continue

                    all_values = []
                    tasks_with_data = []

                    for task in page_tasks:
                        if not task.values_json or not isinstance(task.values_json, list):
                            log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет данных. Пропуск.")
                            task.update_after_upload(False)
                            self.update_task_update_fields(task)
                            continue

                        log_to_file(self.log_file, f"📦 [Task {task.name_of_process}] — {len(task.values_json)} строк (🔄 новые данные)")

                        flat = [str(cell).strip().upper() for row in task.values_json for cell in row]
                        if flat == ["NULL"]:
                            log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Содержит 'NULL'. Пропуск.")
                            task.update_after_upload(False)
                            self.update_task_update_fields(task)
                            continue

                        all_values.extend(task.values_json)
                        tasks_with_data.append(task)

                    if not tasks_with_data:
                        log_to_file(self.log_file, f"⚪ Нет допустимых данных на странице {page_name}. Пропуск.")
                        continue

                    start_row = shift_row_index + 1
                    end_row = start_row + len(all_values) - 1
                    insert_range = f"{page_name}!D{start_row}:AC{end_row}"

                    log_to_file(self.log_file, f"\n📍 Строка 'shift:' найдена на {shift_row_index + 1}, вставка в {insert_range}")

                    batch_data = [{
                        "range": insert_range,
                        "values": all_values
                    }]

                    success, error = self.batch_update(
                        self.service,
                        spreadsheet_id,
                        batch_data,
                        update_group,
                        self.log_file
                    )

                    for task in page_tasks:
                        if task in tasks_with_data:
                            task.update_after_upload(success)
                            self.update_task_update_fields(task)

                    if success:
                        log_to_file(self.log_file, f"✅ Успешная вставка данных страницы {page_name}.")
                    else:
                        log_to_file(self.log_file, f"❌ Ошибка вставки страницы {page_name}: {error}")

                    log_separator(self.log_file)

                except Exception as e:
                    log_to_file(self.log_file, f"❌ Критическая ошибка страницы {page_name}: {e}")
                    for task in page_tasks:
                        task.update_after_upload(False)
                        self.update_task_update_fields(task)

                time.sleep(ROTATIONSINFO_INTERVAL)


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
            token=self.token_name,
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
        table = "RotationsInfo"
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