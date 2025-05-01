import time
from googleapiclient.errors import HttpError
from data import load_sheetsinfo_tasks
from logger import log_to_file

from config import SHEETSINFO_LOG

class SheetsInfoScanner:
    def __init__(self, conn, service, doc_id_map):
        self.conn = conn
        self.service = service
        self.doc_id_map = doc_id_map
        self.tasks = []
        self.log_file = SHEETSINFO_LOG

    def run(self):
        while True:
            self.load_tasks()
            self.scan_phase()
            self.process_phase()
            self.update_phase()
            time.sleep(60)

    def load_tasks(self):
        self.tasks = load_sheetsinfo_tasks(self.conn)
        for task in self.tasks:
            task.assign_doc_ids(self.doc_id_map)
        log_to_file(self.log_file, f"📋 Загружено {len(self.tasks)} актуальных задач из SheetsInfo.")

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

        self.conn.commit()

    def batch_get(self, service, spreadsheet_id, ranges, log_file, retries=3, delay_seconds=10):
        attempt = 0
        while attempt < retries:
            try:
                result = service.spreadsheets().values().batchGet(
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

            except HttpError as e:
                status_code = e.resp.status
                log_to_file(log_file, f"❌ Ошибка HTTP {status_code} при batchGet: {e}")

                if status_code in (429, 500, 503):
                    attempt += 1
                    log_to_file(log_file, f"⏳ Повторная попытка batchGet {attempt}/{retries} через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                elif status_code == 401:
                    log_to_file(log_file, "🔒 Ошибка авторизации (401) при batchGet. Требуется обновить токен.")
                    return {}
                else:
                    return {}

            except Exception as e:
                log_to_file(log_file, f"❌ Непредвиденная ошибка batchGet: {e}")
                return {}

        return {}

    def scan_phase(self):
        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для сканирования.")
            return

        ready_tasks = [task for task in self.tasks if task.is_ready_to_scan()]
        if not ready_tasks:
            log_to_file(self.log_file, "⚪ Нет задач, готовых к сканированию.")
            return

        log_to_file(self.log_file, f"🔎 Найдено {len(ready_tasks)} задач, готовых к сканированию.")

        tasks_by_doc = {}
        for task in ready_tasks:
            tasks_by_doc.setdefault(task.source_doc_id, []).append(task)

        for doc_id, tasks in tasks_by_doc.items():
            try:
                ranges = [f"{task.source_page_name}!{task.source_page_area}" for task in tasks]
                log_to_file(self.log_file, f"📄 Запрос данных из документа {task.source_table_type} для {len(ranges)} диапазонов.")

                response_data = self.batch_get(self.service, doc_id, ranges, self.log_file)

                if not response_data:
                    log_to_file(self.log_file, f"❌ Пустой ответ от batchGet для {task.source_table_type}.")
                    for task in tasks:
                        task.update_after_scan(success=False)
                    continue

                for task in tasks:
                    full_range = f"{task.source_page_name}!{task.source_page_area}"
                    task.raw_values_json = response_data.get(full_range)
                    if task.raw_values_json:
                        task.update_after_scan(success=True)
                    else:
                        task.update_after_scan(success=False)

            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при сканировании документа {task.source_table_type}: {e}")
                for task in tasks:
                    task.update_after_scan(success=False)

#==================================================================================================
#  Фаза обработки
#==================================================================================================

    def update_task_process_fields(self, task):
        cursor = self.conn.cursor()

        table_name = "SheetsInfo"

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

        self.conn.commit()

    def process_phase(self):
        log_to_file(self.log_file, "🛠️ Начало фазы обработки (SheetsInfo)...")
        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обработки.")
            return

        for task in self.tasks:
            if not task.raw_values_json:
                log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет сырых данных для обработки.")
                continue

            try:
                task.process_raw_value()
                task.check_for_update()
                self.update_task_process_fields(task)
                log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Успешно обработана и записана в БД.")

            except Exception as e:
                log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка обработки: {e}")

#==================================================================================================
#  Фаза обновления
#==================================================================================================

    def update_phase(self):
        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обновления.")
            return

        tasks_to_update = [task for task in self.tasks if task.need_update == 1]

        if not tasks_to_update:
            log_to_file(self.log_file, "⚪ Нет задач, требующих обновления в Google Sheets.")
            return

        log_to_file(self.log_file, f"🔄 Начало фазы обновления. Задач для выгрузки: {len(tasks_to_update)}.")

        tasks_by_update_group = {}
        for task in tasks_to_update:
            tasks_by_update_group.setdefault(task.update_group, []).append(task)

        for update_group, group_tasks in tasks_by_update_group.items():
            log_to_file(self.log_file, f"🔄 Обработка группы обновления: {update_group} ({len(group_tasks)} задач).")

            tasks_by_doc = {}
            for task in group_tasks:
                tasks_by_doc.setdefault(task.target_doc_id, []).append(task)

            for doc_id, tasks in tasks_by_doc.items():
                log_to_file(self.log_file, f"📄 Работаем с документом {doc_id} ({len(tasks)} задач).")
                self.update_tasks_batch(doc_id, tasks)

            time.sleep(2)

    def update_tasks_batch(self, spreadsheet_id, tasks):
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

                return True, None

            except HttpError as e:
                status_code = e.resp.status
                log_to_file(log_file, f"❌ Ошибка HTTP {status_code} при batchUpdate: {e}")

                if status_code in (429, 500, 503):
                    attempt += 1
                    log_to_file(log_file, f"⏳ Повторная попытка batchUpdate {attempt}/{retries} через {delay_seconds} секунд...")
                    time.sleep(delay_seconds)
                elif status_code == 401:
                    log_to_file(log_file, "🔒 Ошибка авторизации (401). Требуется обновить токен.")
                    return False, f"Ошибка авторизации: {e}"
                else:
                    return False, str(e)

            except Exception as e:
                log_to_file(log_file, f"❌ Непредвиденная ошибка batchUpdate: {e}")
                return False, str(e)

        return False, "Превышено количество попыток отправки batchUpdate."
