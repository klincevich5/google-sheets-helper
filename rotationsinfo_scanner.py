# rotationsinfo_scanner.py

import os
import threading
import time
from collections import defaultdict
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from config import ROTATIONSINFO_LOG, TOKEN_PATH
from data import load_rotationsinfo_tasks
from logger import log_to_file, log_separator, log_section

class RotationsInfoScanner:
    def __init__(self, conn, service, doc_id_map):
        self.conn = conn
        self.service = service
        self.doc_id_map = doc_id_map
        self.tasks = []
        self.log_file = ROTATIONSINFO_LOG
        self.keep_running = True

    def run(self):

        while True:
            try:
                log_section("🔄 Новый цикл сканирования RotationsInfo", self.log_file)

                self.check_and_refresh_token()

                self.load_tasks()

                self.scan_phase()

                self.process_phase()

                self.update_phase()
                
                self.summary_report()

                time.sleep(60)

            except Exception as e:
                log_separator(self.log_file)
                log_to_file(self.log_file, f"❌ Критическая ошибка в основном цикле: {e}")
                time.sleep(10)

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

    def load_tasks(self):
        log_section("🧩 📥 Загрузка задач из RotationsInfo", self.log_file)
        self.tasks = load_rotationsinfo_tasks(self.conn)

        if not self.tasks:
            log_section(self.log_file, "⚪ Нет задач для загрузки из RotationsInfo.")
            return

        log_section(self.log_file, f"🔄 Загружено {len(self.tasks)} задач.")
        for task in self.tasks:
            log_to_file(self.log_file, f"   • [Task] {task.source_table_type} | Страница: {task.source_page_name} | Диапазон: {task.source_page_area}")
            task.assign_doc_ids(self.doc_id_map)

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

        log_to_file(self.log_file, f"💾 Сохраняю в БД [Task {task.name_of_process}] → need_update={task.need_update}, hash={task.hash}")
        self.conn.commit()

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
        
    def summary_report(self):
        log_section("📈 Итоги текущего цикла сканирования", self.log_file)

        scan = getattr(self, "metrics_scan", {"ready": 0, "success": 0, "failed": 0})
        process = getattr(self, "metrics_process", {"success": 0, "skipped": 0, "failed": 0})
        update = getattr(self, "metrics_update", {"updated": 0, "skipped": 0, "failed": 0})

        log_to_file(self.log_file, "🔍 Сканирование:")
        log_to_file(self.log_file, f"   • Готово к сканированию: {scan['ready']}")
        log_to_file(self.log_file, f"   • Успешно отсканировано: {scan['success']}")
        log_to_file(self.log_file, f"   • Ошибок/пропусков: {scan['failed']}")

        log_to_file(self.log_file, "🛠️ Обработка:")
        log_to_file(self.log_file, f"   • Успешно обработано: {process['success']}")
        log_to_file(self.log_file, f"   • Пропущено: {process['skipped']}")
        log_to_file(self.log_file, f"   • Ошибок: {process['failed']}")

        log_to_file(self.log_file, "🔼 Обновление:")
        log_to_file(self.log_file, f"   • Успешно обновлено: {update['updated']}")
        log_to_file(self.log_file, f"   • Пропущено: {update['skipped']}")
        log_to_file(self.log_file, f"   • Ошибок обновления: {update['failed']}")
        log_to_file(self.log_file, "=" * 100)

    def scan_phase(self):
        log_section("🔍 Фаза сканирования", self.log_file)

        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для сканирования.")
            self.metrics_scan = {"ready": 0, "success": 0, "failed": 0}
            return

        ready_tasks = [task for task in self.tasks if task.is_ready_to_scan()]
        if not ready_tasks:
            log_to_file(self.log_file, "⚪ Нет задач, готовых к сканированию.")
            self.metrics_scan = {"ready": 0, "success": 0, "failed": 0}
            return

        log_to_file(self.log_file, f"🔎 Найдено {len(ready_tasks)} задач, готовых к сканированию:")
        for task in ready_tasks:
            log_to_file(self.log_file, f"   • [Task] {task.name_of_process} | Страница: {task.source_page_name} | Диапазон: {task.source_page_area}")

        scan_groups = defaultdict(list)
        for task in ready_tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process}] Не удалось сопоставить doc_id. Пропуск.")
                continue
            scan_groups[task.scan_group].append(task)

        total_success = 0
        total_failed = 0

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
                    total_failed += 1

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
                    total_failed += 1
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
                    total_success += 1
                else:
                    task.update_after_scan(success=False)
                    self.update_task_scan_fields(task)
                    log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process}] Диапазон {expected_sheet}!{task.source_page_area} не найден или пуст.")
                    total_failed += 1

        log_separator(self.log_file)
        log_to_file(self.log_file, "📊 Результаты фазы сканирования:")
        log_to_file(self.log_file, f"   • ✅ Успешно отсканировано: {total_success}")
        log_to_file(self.log_file, f"   • ❌ Ошибок/пропусков: {total_failed}")
        log_to_file(self.log_file, f"   • 🟡 Всего готовых к сканированию: {len(ready_tasks)}")

        self.metrics_scan = {
            "ready": len(ready_tasks),
            "success": total_success,
            "failed": total_failed
        }

    def process_phase(self):
        log_section("🛠️ Фаза обработки", self.log_file)

        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обработки.")
            self.metrics_process = {"success": 0, "skipped": 0, "failed": 0}
            return

        processed = 0
        skipped = 0
        failed = 0

        for task in self.tasks:
            if not task.raw_values_json:
                log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет сырых данных для обработки.")
                skipped += 1
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
                    failed += 1
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

                    if task.need_update:
                        log_to_file(self.log_file, "🔁 Изменения обнаружены — задача будет обновлена.")
                        self.update_task_process_fields(task)
                        log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Успешно обработана и записана в БД.")
                    else:
                        log_to_file(self.log_file, "⚪ Изменений нет — обновление не требуется.")
                except Exception as e:
                    log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка в check_for_update: {e}")
                    failed += 1
                    continue

                log_separator(self.log_file)
                processed += 1

            except Exception as e:
                log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка обработки: {e}")
                failed += 1

        log_to_file(self.log_file, "📊 Результаты фазы обработки:")
        log_to_file(self.log_file, f"   • ✅ Успешно обработано: {processed}")
        log_to_file(self.log_file, f"   • ⚪ Пропущено (нет данных): {skipped}")
        log_to_file(self.log_file, f"   • ❌ С ошибками: {failed}")

        self.metrics_process = {
            "success": processed,
            "skipped": skipped,
            "failed": failed
        }

    def update_phase(self):
        """
        🔼 ФАЗА ОБНОВЛЕНИЯ

        Обрабатываются только две группы задач:
        • update_main — вставка на главный экран (строго по ROTATION_ORDER) по каждой смене (DAY 1, NIGHT 1 и т.д.)
        • update_shuffle — вставка в динамический диапазон (поиск строки 'shift:')
        """

        log_section("🔼 Фаза обновления", self.log_file)

        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обновления.")
            self.metrics_update = {"updated": 0, "skipped": 0, "failed": 0}
            return

        updated_count = 0
        failed_count = 0
        skipped_count = 0

        main_tasks = [task for task in self.tasks if task.update_group == "update_main"]
        shuffle_tasks = [task for task in self.tasks if "shuffle" in task.update_group]

        if main_tasks:
            grouped_by_tab = defaultdict(list)
            for task in main_tasks:
                grouped_by_tab[task.target_page_name].append(task)

            for tab, group in grouped_by_tab.items():
                if any(t.need_update == 1 for t in group):
                    log_section(f"🔄 Обработка группы обновления: update_main / {tab} ({len(group)} задач)", self.log_file)
                    try:
                        u, f, s = self.import_main_data(group)
                        updated_count += u
                        failed_count += f
                        skipped_count += s
                    except Exception as e:
                        log_to_file(self.log_file, f"❌ Ошибка import_main_data: {e}")
                        for task in group:
                            task.update_after_upload(success=False)
                            self.update_task_update_fields(task)
                            failed_count += 1

        shuffle_groups = defaultdict(list)
        for task in shuffle_tasks:
            shuffle_groups[task.update_group].append(task)

        for update_group, group in shuffle_groups.items():
            log_section(f"🔄 Обработка группы обновления: {update_group} ({len(group)} задач)", self.log_file)
            try:
                u, f, s = self.import_shuffle_data(group)
                updated_count += u
                failed_count += f
                skipped_count += s
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка import_shuffle_data: {e}")
                for task in group:
                    task.update_after_upload(success=False)
                    self.update_task_update_fields(task)
                    failed_count += 1

        log_to_file(self.log_file, "📊 Результаты фазы обновления:")
        log_to_file(self.log_file, f"   • ✅ Успешно обновлено: {updated_count}")
        log_to_file(self.log_file, f"   • ❌ Неудачных обновлений: {failed_count}")
        log_to_file(self.log_file, f"   • ⚪ Пропущено (нет данных или неизвестная группа): {skipped_count}")
        log_to_file(self.log_file, f"   • 🔁 Всего задач в очереди обновления: {len(self.tasks)}")

        self.metrics_update = {
            "updated": updated_count,
            "failed": failed_count,
            "skipped": skipped_count
        }

    def import_main_data(self, all_main_tasks):
        log_section("📥 Импорт данных для update_main", self.log_file)
        grouped_by_page = defaultdict(list)
        for task in all_main_tasks:
            grouped_by_page[task.target_page_name].append(task)

        updated = 0
        failed = 0
        skipped = 0

        for page_name, tasks in grouped_by_page.items():
            log_to_file(self.log_file, f"📄 Обработка смены: {page_name} ({len(tasks)} задач)")

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
                    skipped += 1
                    continue

                flat = [str(cell).strip().upper() for row in values for cell in row if cell is not None]
                if flat == ["NULL"]:
                    log_to_file(self.log_file, f"⚪ [Task {name}] содержит 'NULL'. Пропуск.")
                    task.update_after_upload(False)
                    self.update_task_update_fields(task)
                    skipped += 1
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
                log_to_file(self.log_file, f"⚠️ Обрезано до 100 строк.")

            reference_task = sorted_tasks[0]
            spreadsheet_id = reference_task.target_doc_id
            target_page_area = reference_task.target_page_area
            insert_range = f"{page_name}!{target_page_area}"

            log_to_file(self.log_file, f"📤 Вставка итогового блока из {len(all_values)} строк в диапазон {insert_range}.")
            batch_data = [{
                "range": insert_range,
                "values": all_values
            }]

            success, error = self.batch_update(self.service, spreadsheet_id, batch_data, self.log_file)

            for task in sorted_tasks:
                task.update_after_upload(success)
                self.update_task_update_fields(task)
                if success:
                    updated += 1
                else:
                    failed += 1

            if success:
                log_to_file(self.log_file, f"✅ Вставка смены {page_name} завершена успешно ({len(sorted_tasks)} задач).")
            else:
                log_to_file(self.log_file, f"❌ Ошибка при вставке смены {page_name}: {error}")

        return updated, failed, skipped
    
    def import_shuffle_data(self, tasks):
        updated = 0
        failed = 0
        skipped = 0

        shuffle_groups = defaultdict(list)
        for task in tasks:
            shuffle_groups[task.update_group].append(task)

        for update_group, group_tasks in shuffle_groups.items():
            spreadsheet_id = group_tasks[0].target_doc_id
            target_page_name = group_tasks[0].target_page_name

            log_to_file(self.log_file, f"📄 Документ: {spreadsheet_id}, Лист: {target_page_name}")

            try:
                raw = self.batch_get(
                    self.service,
                    spreadsheet_id,
                    [f"{target_page_name}!D1:AC200"],
                    self.log_file
                )
                sheet_values = list(raw.values())[0] if raw else []

                shift_row_index = None
                for idx, row in enumerate(sheet_values):
                    if row and isinstance(row[0], str) and "shift:" in row[0].lower():
                        shift_row_index = idx + 1
                        break

                if shift_row_index is None:
                    log_to_file(self.log_file, f"❌ Строка с 'shift:' не найдена на листе {target_page_name}. Пропуск всей группы.")
                    for task in group_tasks:
                        task.update_after_upload(False)
                        self.update_task_update_fields(task)
                        failed += 1
                    continue

                start_row = shift_row_index + 1
                end_row = start_row + 5
                insert_range = f"{target_page_name}!D{start_row}:AC{end_row}"

                log_to_file(self.log_file, f"📍 Найдена строка 'shift:' на строке {shift_row_index + 1}, вставка в диапазон {insert_range}")

                all_values = []
                tasks_with_data = []

                for task in group_tasks:
                    if not task.values_json or not isinstance(task.values_json, list):
                        log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет корректных данных для вставки. Пропуск.")
                        task.update_after_upload(False)
                        self.update_task_update_fields(task)
                        skipped += 1
                        continue

                    flat = [str(cell).strip().upper() for row in task.values_json for cell in row]
                    if flat == ["NULL"]:
                        log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Данные содержат 'NULL'. Пропуск.")
                        task.update_after_upload(False)
                        self.update_task_update_fields(task)
                        skipped += 1
                        continue

                    all_values.extend(task.values_json)
                    tasks_with_data.append(task)

                if not tasks_with_data:
                    log_to_file(self.log_file, f"⚪ Нет допустимых данных для вставки в группе {update_group}. Пропуск.")
                    continue

                batch_data = [{
                    "range": insert_range,
                    "values": all_values
                }]

                success, error = self.batch_update(self.service, spreadsheet_id, batch_data, self.log_file)

                for task in group_tasks:
                    if task in tasks_with_data:
                        task.update_after_upload(success)
                        if success:
                            updated += 1
                        else:
                            failed += 1
                    else:
                        # уже учтены как skipped выше
                        continue
                    self.update_task_update_fields(task)

                if success:
                    log_to_file(self.log_file, f"✅ Успешная вставка данных группы {update_group}.")
                else:
                    log_to_file(self.log_file, f"❌ Ошибка вставки группы {update_group}: {error}")

            except Exception as e:
                log_to_file(self.log_file, f"❌ Критическая ошибка обработки группы {update_group}: {e}")
                for task in group_tasks:
                    task.update_after_upload(False)
                    self.update_task_update_fields(task)
                    failed += 1

            time.sleep(2)

        return updated, failed, skipped


    def update_tasks_batch(self, spreadsheet_id, tasks):
        batch_data = []
        for task in tasks:
            if not task.values_json:
                log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет данных, пропуск.")
                continue

            batch_data.append({
                "range": f"{task.target_page_name}!{task.target_page_area}",
                "values": task.values_json
            })

        if not batch_data:
            log_to_file(self.log_file, "⚪ Нет данных для batchUpdate.")
            return

        success, error = self.batch_update(self.service, spreadsheet_id, batch_data, self.log_file)

        if success:
            for task in tasks:
                task.update_after_upload(True)
                self.update_task_update_fields(task)
            log_to_file(self.log_file, f"✅ Обновлено пакетно: {len(tasks)} задач.")
        else:
            log_to_file(self.log_file, f"❌ Ошибка batchUpdate: {error}. Попробуем по одной.")

            for task in tasks:
                if not task.values_json:
                    continue

                data = [{
                    "range": f"{task.target_page_name}!{task.target_page_area}",
                    "values": task.values_json
                }]
                ok, err = self.batch_update(self.service, spreadsheet_id, data, self.log_file)

                if ok:
                    task.update_after_upload(True)
                    log_to_file(self.log_file, f"✅ Обновлена [Task {task.name_of_process}] отдельно.")
                else:
                    task.update_after_upload(False)
                    log_to_file(self.log_file, f"❌ Ошибка при обновлении [Task {task.name_of_process}]: {err}")

                self.update_task_update_fields(task)

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

    def update_task_process_fields(self, task):
        cursor = self.conn.cursor()
        table = "RotationsInfo"
        cursor.execute(f"""
            UPDATE {table}
            SET
                hash = ?,
                values_json = ?,
                need_update = ?
            WHERE id = ?
        """, (
            task.hash,
            str(task.values_json) if task.values_json else None,
            task.need_update,
            task.id
        ))
        log_to_file(self.log_file, f"💾 Обновлён values_json и hash для задачи {task.name_of_process}")
        self.conn.commit()

    def update_task_update_fields(self, task):
        cursor = self.conn.cursor()
        table = "RotationsInfo"
        cursor.execute(f"""
            UPDATE {table}
            SET
                last_update = ?,
                update_quantity = ?,
                update_failures = ?,
                need_update = ?
            WHERE id = ?
        """, (
            task.last_update.isoformat() if task.last_update else None,
            task.update_quantity,
            task.update_failures,
            task.need_update,
            task.id
        ))
        log_to_file(self.log_file, f"💾 Обновлён статус обновления для задачи {task.name_of_process}")
        self.conn.commit()
