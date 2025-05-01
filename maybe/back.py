# rotationsinfo_scanner.py

import os
import threading
import time
import json
from collections import defaultdict
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from config import ROTATIONSINFO_LOG
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
        def heartbeat():
            while self.keep_running:
                log_to_file(self.log_file, "⏳ Ожидание следующего цикла сканирования...")
                log_to_file(self.log_file, "=" * 100)
                time.sleep(10)

        threading.Thread(target=heartbeat, daemon=True).start()

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

    # def check_and_refresh_token(self):
    #     log_section("🔐 Проверка работоспособности Google API токена", self.log_file)

    #     token_path = "token.json"
    #     if not os.path.exists(token_path):
    #         log_to_file(self.log_file, f"❌ Файл {token_path} не найден. Авторизация невозможна.")
    #         raise FileNotFoundError("token.json не найден")

    #     creds = Credentials.from_authorized_user_file(token_path)
    #     if creds.expired and creds.refresh_token:
    #         try:
    #             creds.refresh(Request())
    #             with open(token_path, 'w') as token_file:
    #                 token_file.write(creds.to_json())
    #             log_to_file(self.log_file, "🔄 Токен успешно обновлён и сохранён в token.json.")
    #         except Exception as e:
    #             log_to_file(self.log_file, f"❌ Ошибка обновления токена: {e}")
    #             raise
    #     else:
    #         log_to_file(self.log_file, "✅ Токен действителен.")

    def check_and_refresh_token(self):
        log_section("🔐 Проверка работоспособности Google API токена", self.log_file)

        token_path = "token.json"
        creds = None

        if not os.path.exists(token_path):
            log_to_file(self.log_file, f"❌ Файл {token_path} не найден. Авторизация невозможна.")
            raise FileNotFoundError("token.json не найден")

        creds = Credentials.from_authorized_user_file(token_path)
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_path, 'w') as token_file:
                    token_file.write(creds.to_json())
                log_to_file(self.log_file, "🔄 Токен успешно обновлён и сохранён в token.json.")
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка обновления токена: {e}")
                raise
        else:
            log_to_file(self.log_file, "✅ Токен действителен и не требует обновления.")

#==================================================================================================
#  Фаза сканирования
#==================================================================================================

    def update_task_update_fields(self, task):
        """Обновить last_update, update_quantity, update_failures после выгрузки задачи."""
        cursor = self.conn.cursor()

        cursor.execute(f"""
            UPDATE {task.source_table}
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
            log_to_file(self.log_file, f"❌ Ошибка при проверке листа '{sheet_name}' в документе {spreadsheet_id}: {e}")
            return False

    def batch_get(self, service, spreadsheet_id, ranges, log_file, retries=5, delay_seconds=5):
        attempt = 0
        while attempt < retries:
            try:
                log_to_file(log_file, f"📡 batchGet (попытка {attempt+1}/{retries}) для {spreadsheet_id}")
                result = service.spreadsheets().values().batchGet(
                    spreadsheetId=spreadsheet_id,
                    ranges=ranges,
                    majorDimension="ROWS"
                ).execute()
                value_ranges = result.get("valueRanges", [])
                data = {vr.get("range", ""): vr.get("values", []) for vr in value_ranges}
                log_to_file(log_file, f"✅ batchGet успешен. Получено диапазонов: {len(data)}")
                return data
            except HttpError as e:
                status = e.resp.status
                log_to_file(log_file, f"❌ HttpError {status}: {e}")
                if status in (429, 500, 503):
                    attempt += 1
                    log_to_file(log_file, f"⏳ Повтор через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                elif status == 401:
                    log_to_file(log_file, "🔒 Ошибка авторизации (401). Завершение.")
                    return {}
                else:
                    return {}
            except Exception as e:
                if any(x in str(e) for x in ["SSL", "handshake", "timed out"]):
                    attempt += 1
                    log_to_file(log_file, f"⏳ Сетевая ошибка '{e}'. Повтор через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                else:
                    log_to_file(log_file, f"❌ Непредвиденная ошибка: {e}")
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
            log_to_file(self.log_file, f"   • [Task] {task.name_of_process} | source_page_name: {task.source_page_name} | Диапазон: {task.source_page_area}")

        # === Группировка по scan_group ===
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
            log_to_file(self.log_file, f"Уникальные листы (source_page_name): {unique_sheet_names}")

            # === Проверка наличия листов ===
            exists_map = {}
            for sheet_name in unique_sheet_names:
                exists_map[sheet_name] = self.check_sheet_exists(doc_id, sheet_name)
                log_to_file(
                    self.log_file,
                    f"{'✅' if exists_map[sheet_name] else '⚠️'} Лист '{sheet_name}' {'найден' if exists_map[sheet_name] else 'не найден'}"
                )

            valid_tasks = []
            for task in group_tasks:
                sheet_name = task.actual_tab
                if exists_map.get(sheet_name):
                    log_to_file(self.log_file, f"➡️ Лист '{sheet_name}' будет использоваться для задачи {task.name_of_process}.")
                    valid_tasks.append(task)
                else:
                    log_to_file(self.log_file, f"⛔ Пропуск задачи {task.name_of_process}: лист '{sheet_name}' не найден.")
                    task.update_after_scan(success=False)
                    self.update_task_scan_fields(task)
                    total_failed += 1

            if not valid_tasks:
                log_to_file(self.log_file, f"⚪ Все задачи группы {scan_group} отфильтрованы. Пропуск batchGet.")
                continue

            # === Группировка по диапазонам ===
            range_to_tasks = defaultdict(list)
            for task in valid_tasks:
                range_str = f"{task.source_page_name}!{task.source_page_area}"
                range_to_tasks[range_str].append(task)

            ranges = list(range_to_tasks.keys())

            log_to_file(self.log_file, f"📤 Отправка batchGet для документа {task.source_table_type}:")
            for r in ranges:
                log_to_file(self.log_file, f"   • {r}")

            # === Запрос данных ===
            response_data = self.batch_get(self.service, doc_id, ranges, self.log_file)

            if not response_data:
                log_to_file(self.log_file, "❌ Пустой ответ от batchGet. Отмечаем задачи как неудачные.")
                for task in valid_tasks:
                    task.update_after_scan(success=False)
                    self.update_task_scan_fields(task)
                    total_failed += 1
                continue

            # === Обработка ответа ===
            normalized_response = {}
            for k, v in response_data.items():
                clean_key = k.replace("'", "")
                if "!" in clean_key:
                    sheet_name, cells_range = clean_key.split("!", 1)
                    normalized_response[(sheet_name.strip(), cells_range.strip())] = v

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
                    for i, row in enumerate(matched_values[:5]):
                        log_to_file(self.log_file, f"      [{i+1}] {row}")
                    if len(matched_values) > 5:
                        log_to_file(self.log_file, f"      ...ещё {len(matched_values) - 5} строк скрыто")
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

    #==================================================================================================
    #  Фаза обработки
    #==================================================================================================

    def update_task_process_fields(self, task):
        """Обновить values_json, hash и need_update после обработки задачи."""
        cursor = self.conn.cursor()

        table_name = "SheetsInfo" if task.source_table_type == "SheetsInfo" else "RotationsInfo"

        cursor.execute(f"""
            UPDATE {table_name}
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
        
        log_to_file(self.log_file, f"💾 Сохраняю в БД [Task {task.name_of_process}] → need_update={task.need_update}, hash={task.hash}")

        self.conn.commit()

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
                        log_to_file(self.log_file, f"🔁 Изменения обнаружены — задача будет обновлена.")
                    else:
                        log_to_file(self.log_file, f"⚪ Изменений нет — обновление не требуется.")
                except Exception as e:
                    log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка в check_for_update: {e}")
                    failed += 1
                    continue

                self.update_task_process_fields(task)
                log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Успешно обработана и записана в БД.")
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


    #==================================================================================================
    #  Фаза обновления
    #==================================================================================================

    def update_phase(self):
        log_section("🔼 Фаза обновления", self.log_file)

        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обновления.")
            self.metrics_update = {"updated": 0, "skipped": 0, "failed": 0}
            return

        tasks_to_update = [task for task in self.tasks if task.need_update == 1]

        if not tasks_to_update:
            log_to_file(self.log_file, "⚪ Нет задач, требующих обновления в Google Sheets.")
            self.metrics_update = {"updated": 0, "skipped": 0, "failed": 0}
            return

        log_to_file(self.log_file, f"🔄 Начало фазы обновления. Задач для выгрузки: {len(tasks_to_update)}.")
        log_to_file(self.log_file, "=" * 100)

        total_updated = 0
        total_failed = 0
        total_skipped = 0

        tasks_by_update_group = defaultdict(list)
        for task in tasks_to_update:
            tasks_by_update_group[task.update_group].append(task)

        for update_group, group_tasks in tasks_by_update_group.items():
            log_to_file(self.log_file, f"🔄 Обработка группы обновления: {update_group} ({len(group_tasks)} задач).")
            log_separator(self.log_file)

            tasks_by_doc = defaultdict(list)
            for task in group_tasks:
                tasks_by_doc[task.target_doc_id].append(task)

            for doc_id, tasks in tasks_by_doc.items():
                log_to_file(self.log_file, f"📄 Работаем с документом {task.source_table_type} ({len(tasks)} задач).")

                batch_data = []
                for task in tasks:
                    if not task.values_json:
                        log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет данных для отправки, пропуск.")
                        total_skipped += 1
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
                    total_updated += len(tasks)
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
                            total_updated += 1
                        else:
                            task.update_after_upload(success=False)
                            log_to_file(self.log_file, f"❌ Ошибка обновления [Task {task.name_of_process}] отдельно: {single_error}")
                            total_failed += 1

                        self.update_task_update_fields(task)

                time.sleep(2)

        log_to_file(self.log_file, "📊 Результаты фазы обновления:")
        log_to_file(self.log_file, f"   • ✅ Успешно обновлено: {total_updated}")
        log_to_file(self.log_file, f"   • ❌ Неудачных обновлений: {total_failed}")
        log_to_file(self.log_file, f"   • ⚪ Пропущено (нет данных): {total_skipped}")
        log_to_file(self.log_file, f"   • 🔁 Всего задач в очереди обновления: {len(tasks_to_update)}")

        self.metrics_update = {
            "updated": total_updated,
            "failed": total_failed,
            "skipped": total_skipped
        }


    def update_tasks_batch(self, spreadsheet_id, tasks):
        """Пытается обновить задачи одним batchUpdate, а при ошибке — поштучно."""
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
            return

        success, error = self.batch_update(self.service, spreadsheet_id, batch_data, self.log_file)

        if success:
            for task in tasks:
                task.update_after_upload(success=True)
                self.update_task_update_fields(task)
            log_to_file(self.log_file, f"✅ Успешное обновление группы ({len(tasks)} задач) одним запросом.")
        else:
            log_to_file(self.log_file, f"❌ Ошибка batchUpdate для группы: {error}")
            log_to_file(self.log_file, "🔄 Переходим на поштучную отправку задач.")

            # Переходим к одиночной отправке
            for task in tasks:
                if not task.values_json:
                    continue

                single_data = [{
                    "range": f"{task.target_page_name}!{task.target_page_area}",
                    "values": task.values_json
                }]

                single_success, single_error = self.batch_update(self.service, spreadsheet_id, single_data, self.log_file)

                if single_success:
                    task.update_after_upload(success=True)
                    log_to_file(self.log_file, f"✅ Успешно обновлена задача [Task {task.name_of_process}] отдельно.")
                else:
                    task.update_after_upload(success=False)
                    log_to_file(self.log_file, f"❌ Ошибка обновления [Task {task.name_of_process}] отдельно: {single_error}")

                self.update_task_update_fields(task)

    def batch_update(self, service, spreadsheet_id, batch_data, log_file, retries=3, delay_seconds=10):
        """ Отправка batchUpdate запроса с повторными попытками при ошибках """
        attempt = 0

        while attempt < retries:
            try:
                body = {
                    "valueInputOption": "USER_ENTERED",
                    "data": batch_data
                }
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=body
                ).execute()

                return True, None  # Успех

            except HttpError as e:
                status_code = e.resp.status
                log_to_file(log_file, f"❌ Ошибка HTTP {status_code} при batchUpdate: {e}")

                if status_code in (429, 500, 503):  # Перегрузка сервера или лимит
                    attempt += 1
                    log_to_file(log_file, f"⏳ Повторная попытка {attempt}/{retries} через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                elif status_code == 401:  # Проблемы с авторизацией
                    log_to_file(log_file, "🔒 Ошибка авторизации (401). Требуется обновить токен.")
                    return False, f"Ошибка авторизации: {e}"
                else:
                    return False, str(e)

            except Exception as e:
                log_to_file(log_file, f"❌ Непредвиденная ошибка при batchUpdate: {e}")
                return False, str(e)

        return False, "Превышено количество попыток отправки batchUpdate."
