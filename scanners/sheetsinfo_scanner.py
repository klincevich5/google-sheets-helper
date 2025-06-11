# scanners/sheetsinfo_scanner.py

import time
from datetime import datetime, timedelta
from collections import defaultdict

from tg_bot.utils.settings_access import is_scanner_enabled
from core.data import load_sheetsinfo_tasks
from utils.logger import (
    log_info, log_success, log_warning, log_error, log_section, log_separator
)
from utils.db_orm import get_max_last_row
from utils.floor_resolver import get_floor_by_table_name
from database.session import SessionLocal

from database.db_models import MistakeStorage, FeedbackStorage,  ScheduleOT, DealerMonthlyStatus, QaList

from core.config import (
    SHEETSINFO_LOG,
    SHEETINFO_INTERVAL,
    FLOORS
)
from utils.db_orm import (
    update_task_scan_fields,
    update_task_process_fields,
    update_task_update_fields
)
from utils.utils import (
    load_credentials,
    check_sheet_exists,
    batch_get,
    batch_update,
)

class SheetsInfoScanner:
    """
    SheetsInfoScanner — основной исполнительный класс для сканирования, обработки и обновления задач по Google Sheets.
    """

    def __init__(self, token_map, doc_id_map):
        """
        Инициализация сканера.
        :param token_map: словарь токенов для Google Sheets API
        :param doc_id_map: словарь doc_id для отслеживаемых таблиц
        """
        self.token_map = token_map
        self.doc_id_map = doc_id_map
        self.log_file = SHEETSINFO_LOG
        self.tasks = []

    def run(self):
        """
        Основной цикл работы сканера: загрузка задач, сканирование, обработка, обновление.
        """
        while True:
            try:
                if not is_scanner_enabled("sheets_scanner"):
                    time.sleep(10)
                    continue

                log_separator(self.log_file, "run")
                log_section(self.log_file, "run", "▶️ SheetsInfo Активен. Новый цикл сканирования\n")

                token_name = list(self.token_map.keys())[0]
                token_path = self.token_map[token_name]
                self.token_name = token_name

                with SessionLocal() as session:
                    self.service = load_credentials(token_path, self.log_file)
                    log_info(self.log_file, "run", None, "token", f"Используется токен: {self.token_name}")

                    for phase_name, method in [
                        ("load_tasks", lambda: self.load_tasks(session)),
                        ("scan_phase", lambda: self.scan_phase(session)),
                        ("process_phase", lambda: self.process_phase(session)),
                        ("update_phase", lambda: self.update_phase(session)),
                    ]:
                        log_separator(self.log_file, phase_name)
                        try:
                            log_info(self.log_file, phase_name, None, "start", f"Старт этапа {phase_name}")
                            method()
                            log_success(self.log_file, phase_name, None, "finish", f"Этап {phase_name} завершён\n")
                        except Exception as e:
                            log_error(self.log_file, phase_name, None, "fail", "Ошибка на этапе", exc=e)
                            raise

            except Exception as e:
                log_error(self.log_file, "run", None, "fail", "Критическая ошибка в основном цикле", exc=e)
                time.sleep(10)

            time.sleep(SHEETINFO_INTERVAL)

    def _validate_sheet(self, sheet):
        """
        Проверяет, что sheet — это непустой список.
        """
        return sheet and isinstance(sheet, list)

    def _commit_or_rollback(self, session, log_msg=None):
        """
        Коммитит транзакцию или делает rollback при ошибке.
        """
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            if log_msg:
                log_error(self.log_file, "SheetsInfoScanner", "_commit_or_rollback", None, "commit_fail", f"{log_msg}: {e}")

    def _is_valid_qa_row(self, row):
        """
        Проверяет, что строка QA List валидна для добавления в БД.
        """
        # Минимум dealer_name и хотя бы один TRUE/FALSE в ключевых полях
        if not row or len(row) < 18:
            return False
        if not row[0] or not isinstance(row[0], str):
            return False
        # Можно добавить дополнительные проверки по уникальным полям
        return True

#############################################################################################
# загрузка задач из БД
#############################################################################################

    def load_tasks(self, session):
        log_section(self.log_file, "load_tasks", "📥 Загрузка задач из SheetsInfo")
        self.tasks = load_sheetsinfo_tasks(session, self.log_file)
        if not self.tasks:
            log_info(self.log_file, "load_tasks", None, "empty", "Нет активных задач для сканирования")
            self.tasks = []
            return
        skipped = 0
        for task in self.tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                skipped += 1
                log_warning(self.log_file, "load_tasks", getattr(task, 'name_of_process', None), "skipped", "Нет doc_id, задача пропущена")
        log_info(self.log_file, "load_tasks", None, "done", f"Загружено задач: {len(self.tasks)}, пропущено без doc_id: {skipped}")

#############################################################################################
# Фаза сканирования
#############################################################################################

    def scan_phase(self, session):
        log_section(self.log_file, "scan_phase", "🔍 Фаза сканирования")
        if not self.tasks:
            log_info(self.log_file, "scan_phase", None, "empty", "Нет задач для сканирования")
            return
        ready_tasks = [task for task in self.tasks if task.is_ready_to_scan()]
        log_info(self.log_file, "scan_phase", None, "ready", f"Готовых задач: {len(ready_tasks)}")
        if not ready_tasks:
            log_info(self.log_file, "scan_phase", None, "empty", "Нет задач, готовых к сканированию")
            return
        scan_groups = defaultdict(list)
        for task in ready_tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                log_warning(self.log_file, "scan_phase", getattr(task, 'name_of_process', None), "skipped", "Не удалось сопоставить doc_id. Пропуск.")
                continue
            scan_groups[task.scan_group].append(task)
        for scan_group, group_tasks in scan_groups.items():
            log_section(self.log_file, "scan_phase", f"\n🗂️ Обработка scan_group: {scan_group} ({len(group_tasks)} задач)\n")
            if not group_tasks:
                continue
            doc_id = group_tasks[0].source_doc_id
            unique_sheet_names = set(task.source_page_name for task in group_tasks)
            exists_map = {
                sheet_name: check_sheet_exists(self.service, doc_id, sheet_name, self.log_file, self.token_name)
                for sheet_name in unique_sheet_names
            }
            for sheet_name, exists in exists_map.items():
                log_info(self.log_file, "scan_phase", None, "sheet_exists", f"Лист '{sheet_name}' {'существует' if exists else 'не найден'}")
            valid_tasks = []
            for task in group_tasks:
                sheet_name = task.source_page_name
                if exists_map.get(sheet_name):
                    valid_tasks.append(task)
                else:
                    log_warning(self.log_file, "scan_phase", task.name_of_process, "skipped", f"Лист '{sheet_name}' не найден")
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")
            if not valid_tasks:
                log_info(self.log_file, "scan_phase", None, "empty", f"Все задачи группы {scan_group} отфильтрованы. Пропуск batchGet.")
                continue
            range_to_tasks = defaultdict(list)
            for task in valid_tasks:
                range_str = f"{task.source_page_name}!{task.source_page_area}"
                range_to_tasks[range_str].append(task)
            ranges = list(range_to_tasks.keys())
            log_info(self.log_file, "scan_phase", None, "batch_get", f"Отправка batchGet на документ {task.source_table_type} с {len(ranges)} диапазонами")
            response_data = batch_get(
                self.service,
                doc_id,
                ranges,
                scan_group,
                self.log_file,
                self.token_name
            )
            if not response_data:
                for task in valid_tasks:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")
                log_warning(self.log_file, "scan_phase", None, "empty", "Пустой ответ от batchGet. Все задачи будут отмечены как неудачные.")
                continue
            normalized_response = {}
            for k, v in response_data.items():
                clean_key = k.replace("'", "")
                if "!" in clean_key:
                    sheet_name, cells_range = clean_key.split("!", 1)
                    normalized_response[(sheet_name.strip(), cells_range.strip())] = v
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
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")
                    log_success(self.log_file, "scan_phase", task.name_of_process, "found", f"Найден диапазон {sheet_name}!{cells_range}, строк: {len(matched_values)}")
                else:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")
                    log_warning(self.log_file, "scan_phase", task.name_of_process, "not_found", f"Диапазон {expected_sheet}!{task.source_page_area} не найден или пуст.")
        log_info(self.log_file, "scan_phase", None, "summary", "\n".join(
            [f"• {task.name_of_process} {task.source_page_name}: scanned={task.scanned}, processed={task.proceed}, changed={task.changed}, uploaded={task.uploaded}"
             for task in self.tasks]
        ) + "\n")
        log_success(self.log_file, "scan_phase", None, "finish", "Фаза сканирования завершена\n")

#############################################################################################
# Фаза обработки
#############################################################################################

    def process_phase(self, session):
        log_section(self.log_file, "process_phase", "🛠️ Фаза обработки")
        if not self.tasks:
            log_info(self.log_file, "process_phase", None, "empty", "Нет задач для обработки")
            return
        for task in self.tasks:
            if task.scanned == 0:
                continue
            try:
                try:
                    task.process_raw_value()
                except Exception as e:
                    log_error(self.log_file, "process_phase", task.name_of_process, "fail", "Ошибка в process_raw_value", exc=e)
                    continue
                try:
                    task.check_for_update()
                except Exception as e:
                    log_error(self.log_file, "process_phase", task.name_of_process, "fail", "Ошибка в check_for_update", exc=e)
                    continue
                if task.changed:
                    try:
                        update_task_process_fields(session, task, self.log_file, table_name="SheetsInfo")
                        log_success(self.log_file, "process_phase", task.name_of_process, "changed", "Данные изменены и сохранены")
                    except Exception as e:
                        log_error(self.log_file, "process_phase", task.name_of_process, "fail", "Ошибка при сохранении изменений в БД", exc=e)
            except Exception as e:
                log_error(self.log_file, "process_phase", task.name_of_process, "fail", "Неизвестная ошибка при обработке", exc=e)
        log_info(self.log_file, "process_phase", None, "summary", "\n".join(
            [f"• {task.name_of_process} {task.source_page_name}: scanned={task.scanned}, processed={task.proceed}, changed={task.changed}, uploaded={task.uploaded}"
             for task in self.tasks]
        ) + "\n")
        log_success(self.log_file, "process_phase", None, "finish", "Фаза обработки завершена\n")

#############################################################################################
# Фаза обновления
#############################################################################################

    def update_phase(self, session):
        log_section(self.log_file, "update_phase", "🔼 Фаза обновления")
        # --- Категоризация задач ---
        tasks_to_update = []
        mistakes_to_update = []
        feedback_to_update = []
        schedule_OT_to_update = []
        qa_list_update = []

        for t in self.tasks:
            if not (t.values_json and t.changed):
                continue
            if t.update_group == "update_mistakes_in_db":
                mistakes_to_update.append(t)
            elif t.update_group == "feedback_status_update":
                feedback_to_update.append(t)
            elif t.update_group == "update_schedule_OT":
                schedule_OT_to_update.append(t)
            elif t.update_group == "update_qa_list_db":
                qa_list_update.append(t)
            else:
                tasks_to_update.append(t)

        # --- Логгирование категорий ---
        log_info(self.log_file, "update_phase", None, "tasks_to_update", f"🔼 Задач для обновления: {len(tasks_to_update)}")
        for task in tasks_to_update:
            log_info(self.log_file, "update_phase", task.name_of_process, "tasks_to_update", f"   • {task.name_of_process} ({task.update_group})")
        log_info(self.log_file, "update_phase", None, "mistakes_to_update", f"🔼 Ошибок для обновления: {len(mistakes_to_update)}")
        for task in mistakes_to_update:
            log_info(self.log_file, "update_phase", task.name_of_process, "mistakes_to_update", f"   • {task.name_of_process} ({task.update_group})")
        log_info(self.log_file, "update_phase", None, "feedback_to_update", f"🔼 Фидбеков для обновления: {len(feedback_to_update)}")
        for task in feedback_to_update:
            log_info(self.log_file, "update_phase", task.name_of_process, "feedback_to_update", f"   • {task.name_of_process} ({task.update_group})")
        log_info(self.log_file, "update_phase", None, "schedule_OT_to_update", f"🔼 Schedule OT для обновления: {len(schedule_OT_to_update)}")
        for task in schedule_OT_to_update:
            log_info(self.log_file, "update_phase", task.name_of_process, "schedule_OT_to_update", f"   • {task.name_of_process} ({task.update_group})")
        log_info(self.log_file, "update_phase", None, "qa_list_update", f"🔼 QA List для обновления: {len(qa_list_update)}")
        for task in qa_list_update:
            log_info(self.log_file, "update_phase", task.name_of_process, "qa_list_update", f"   • {task.name_of_process} ({task.update_group})")

        # --- Обычные задачи ---
        if tasks_to_update:
            log_info(self.log_file, "update_phase", None, "tasks_to_update", f"🔼 Обновление обычных задач: {len(tasks_to_update)}")
            try:
                self.import_tasks_to_update(tasks_to_update, session)
            except Exception as e:
                log_error(self.log_file, "update_phase", None, "tasks_to_update_fail", f"❌ Ошибка при обновлении задач: {e}")
            time.sleep(3)

        # --- Ошибки ---
        if mistakes_to_update:
            log_info(self.log_file, "update_phase", None, "mistakes_to_update", f"🔼 Обновление ошибок: {len(mistakes_to_update)}")
            try:
                self.import_mistakes_to_update(mistakes_to_update, session)
                for t in mistakes_to_update:
                    t.update_after_upload(success=True)
                    update_task_update_fields(session, t, self.log_file, table_name="SheetsInfo")
                session.commit()
            except Exception as e:
                session.rollback()
                log_error(self.log_file, "update_phase", None, "mistakes_to_update_fail", f"❌ Ошибка при обновлении ошибок: {e}")
            time.sleep(3)

        # --- Фидбеки ---
        if feedback_to_update:
            log_info(self.log_file, "update_phase", None, "feedback_to_update", f"🔼 Обновление фидбеков: {len(feedback_to_update)}")
            try:
                self.import_feedbacks_to_update(feedback_to_update, self.service, session)
                for t in feedback_to_update:
                    t.update_after_upload(success=True)
                    update_task_update_fields(session, t, self.log_file, table_name="SheetsInfo")
                session.commit()
            except Exception as e:
                session.rollback()
                log_error(self.log_file, "update_phase", None, "feedback_to_update_fail", f"❌ Ошибка при обновлении фидбеков: {e}")

        # --- Schedule OT ---
        if schedule_OT_to_update:
            log_info(self.log_file, "update_phase", None, "schedule_OT_to_update", f"🔼 Обновление графиков OT: {len(schedule_OT_to_update)}")
            try:
                self.import_schedule_OT_to_update(schedule_OT_to_update, session)
                for t in schedule_OT_to_update:
                    t.update_after_upload(success=True)
                    update_task_update_fields(session, t, self.log_file, table_name="SheetsInfo")
                session.commit()
            except Exception as e:
                session.rollback()
                log_error(self.log_file, "update_phase", None, "schedule_OT_to_update_fail", f"❌ Ошибка при обновлении schedule OT: {e}")
            time.sleep(3)

        # --- QA List ---
        if qa_list_update:
            log_info(self.log_file, "update_phase", None, "qa_list_update", f"🔼 Обновление QA List: {len(qa_list_update)}")
            try:
                self.import_qa_list_to_update(qa_list_update, session)
                for t in qa_list_update:
                    t.update_after_upload(success=True)
                    update_task_update_fields(session, t, self.log_file, table_name="SheetsInfo")
                session.commit()
            except Exception as e:
                session.rollback()
                log_error(self.log_file, "update_phase", None, "qa_list_update_fail", f"❌ Ошибка при обновлении QA List: {e}")
            time.sleep(3)

        # --- Статистика по задачам ---
        log_info(self.log_file, "update_phase", None, "summary", "🔼 Итоговая статистика по задачам:")
        for task in self.tasks:
            log_info(
                self.log_file,
                "update_phase",
                task.name_of_process,
                "summary",
                f"⚪ [Task {task.name_of_process} {task.source_page_name} {task.related_month}] "
                f"Отсканировано: {task.scanned} | Обработано: {task.proceed} | "
                f"Изменено: {task.changed} | Загружено: {task.uploaded}"
            )

        log_section(self.log_file, "update_phase", "🔼 Обновление завершено.\n")

##############################################################################################
# Импорт Обычных задач 
##############################################################################################

    def import_tasks_to_update(self, tasks_to_update, session):
        log_info(self.log_file, "import_tasks_to_update", None, "start", f"🔄 Начало фазы tasks_to_update. Всего задач: {len(tasks_to_update)}")

        tasks_by_group = defaultdict(list)
        for task in tasks_to_update:
            tasks_by_group[task.update_group].append(task)

        for update_group, group_tasks in tasks_by_group.items():
            log_info(self.log_file, "import_tasks_to_update", None, "group", f"🔄 Обработка группы: {update_group} ({len(group_tasks)} задач)")

            # Проверка doc_id
            doc_ids = set(t.target_doc_id for t in group_tasks)
            if len(doc_ids) != 1:
                log_error(self.log_file, "import_tasks_to_update", None, "multi_doc_id", f"❌ В группе {update_group} несколько doc_id: {doc_ids}. Пропуск.")
                continue
            doc_id = doc_ids.pop()

            batch_data = self._build_batch_data(group_tasks)
            if not batch_data:
                log_warning(self.log_file, "import_tasks_to_update", None, "no_data", f"⚠️ Нет валидных данных для batchUpdate группы {update_group}. Пропуск.")
                continue

            try:
                success, error = batch_update(
                    service=self.service,
                    spreadsheet_id=doc_id,
                    batch_data=batch_data,
                    token_name=self.token_name,
                    update_group=update_group,
                    log_file=self.log_file
                )
                if success:
                    log_success(self.log_file, "import_tasks_to_update", None, "batch_update", f"✅ Пакетное обновление успешно для группы {update_group}")
                    try:
                        self._mark_tasks_uploaded(group_tasks, session)
                        session.commit()
                    except Exception as db_err:
                        session.rollback()
                        log_error(self.log_file, "import_tasks_to_update", None, "db_update_fail", f"❌ Ошибка при обновлении статусов задач в БД: {db_err}")
                else:
                    log_error(self.log_file, "import_tasks_to_update", None, "batch_update_fail", f"❌ Ошибка при пакетной отправке: {error}")
                    self._fallback_single_upload(group_tasks, doc_id, update_group, session)
            except Exception as e:
                log_error(self.log_file, "import_tasks_to_update", None, "batch_update_exception", f"❌ Исключение при batch_update: {e}")
                self._fallback_single_upload(group_tasks, doc_id, update_group, session)

    def _mark_tasks_uploaded(self, tasks, session):
        for task in tasks:
            try:
                task.update_after_upload(success=True)
                update_task_update_fields(
                    session=session,
                    task=task,
                    log_file=self.log_file,
                    table_name="SheetsInfo"
                )
            except Exception as e:
                log_error(self.log_file, "import_tasks_to_update", task.name_of_process, "db_update_fail", f"❌ Ошибка при обновлении статуса задачи {task.name_of_process}: {e}")

    def _fallback_single_upload(self, tasks, doc_id, update_group, session):
        for task in tasks:
            if not task.values_json:
                continue

            single_data = [{
                "range": f"{task.target_page_name}!{task.target_page_area}",
                "values": task.values_json
            }]

            try:
                success, error = batch_update(
                    service=self.service,
                    spreadsheet_id=doc_id,
                    batch_data=single_data,
                    token_name=self.token_name,
                    update_group=update_group,
                    log_file=self.log_file
                )
                if success:
                    log_success(self.log_file, "import_tasks_to_update", task.name_of_process, "single_update", f"✅ [Task {task.name_of_process} {task.source_page_name} {task.related_month}] Обновлена поштучно.")
                else:
                    log_error(self.log_file, "import_tasks_to_update", task.name_of_process, "single_update_fail", f"❌ [Task {task.name_of_process} {task.source_page_name} {task.related_month}] Ошибка при обновлении: {error}")
                try:
                    task.update_after_upload(success=success)
                    update_task_update_fields(
                        session=session,
                        task=task,
                        log_file=self.log_file,
                        table_name="SheetsInfo"
                    )
                    session.commit()
                except Exception as db_err:
                    session.rollback()
                    log_error(self.log_file, "import_tasks_to_update", task.name_of_process, "db_update_fail", f"❌ Ошибка при обновлении статуса задачи {task.name_of_process} в БД: {db_err}")
            except Exception as e:
                log_error(self.log_file, "import_tasks_to_update", task.name_of_process, "single_update_exception", f"❌ Исключение при поштучном обновлении задачи {task.name_of_process}: {e}")

###############################################################################################
# Импорт Ошибок в БД
###############################################################################################

    def import_mistakes_to_update(self, mistakes_to_update, session):
        """
        Импорт ошибок в БД с проверкой дубликатов и валидацией данных.
        """
        total_success = 0
        total_error = 0

        for task in mistakes_to_update:
            success_count = 0
            error_count = 0
            try:
                sheet = task.raw_values_json
                if not self._validate_sheet(sheet):
                    log_warning(self.log_file, "import_mistakes_to_update", task.name_of_process, "empty_sheet", f"⚠️ Пустой или некорректный sheet в задаче: {task.name_of_process}")
                    continue

                page_name = task.source_page_name
                floor = get_floor_by_table_name(page_name, FLOORS)
                max_row_in_db = get_max_last_row(session, page_name)

                for row_index, row in enumerate(sheet[1:], start=2):
                    if row_index <= max_row_in_db or not row or len(row) < 8:
                        continue

                    exists = session.query(MistakeStorage).filter_by(
                        related_month=task.related_month,
                        table_name=page_name,
                        last_row=row_index
                    ).first()
                    if exists:
                        continue

                    try:
                        mistake = self._parse_mistake_row(task, row, row_index, floor, page_name)
                        if mistake:
                            session.add(mistake)
                            success_count += 1
                    except Exception as row_err:
                        log_error(self.log_file, "import_mistakes_to_update", task.name_of_process, "row_fail", f"❌ Ошибка при добавлении строки {row_index} из {page_name}: {row_err}. Строка: {row}")
                        error_count += 1

                self._commit_or_rollback(session, log_msg=f"Ошибка при коммите ошибок для {task.name_of_process}")
                log_success(self.log_file, "import_mistakes_to_update", task.name_of_process, "imported", f"✅ [{task.name_of_process}] Импортировано ошибок: {success_count}, ошибок: {error_count}")
                total_success += success_count
                total_error += error_count

            except Exception as task_err:
                session.rollback()
                log_error(self.log_file, "import_mistakes_to_update", task.name_of_process, "task_fail", f"❌ Ошибка при обработке задачи {task.name_of_process}: {task_err}")

        log_success(self.log_file, "import_mistakes_to_update", None, "imported_total", f"✅ Импортировано ошибок всего: {total_success}, ошибок: {total_error}")

    def _parse_mistake_row(self, task, row, row_index, floor, page_name):
        date = self.parse_date(row[0])
        time_ = self.parse_time(row[1])
        shift = self.determine_shift(time_.hour) if time_ else None

        return MistakeStorage(
            related_month=task.related_month,
            related_date=date,
            related_shift=shift,
            floor=floor,
            table_name=page_name,
            event_time=time_,
            game_id=row[2],
            mistake=row[3],
            mistake_type=row[4],
            is_cancel=self.parse_cancel(row[5]),
            dealer_name=row[6],
            sm_name=row[7],
            last_row=row_index
        )


################################################################################################
# Импорт статуса фидбеков
################################################################################################

    @staticmethod
    def safe_int(value):
        try:
            if value == '' or value is None:
                return None
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def parse_cancel(value):
        if value == '':
            return 0
        elif value.lower() == 'cancel':
            return 1
        return None

    @staticmethod
    def parse_date(value):
        try:
            return datetime.strptime(value.strip(), "%d.%m.%Y").date()
        except Exception:
            return None
        
    @staticmethod
    def parse_time(value):
        try:
            return datetime.strptime(value.strip(), "%H.%M").time()
        except Exception:
            return None
        
    def _parse_feedback_row(self, row, task):
        """
        Преобразует строку feedback в dict для FeedbackStorage.
        Ожидается порядок: [related_date, related_shift, floor, game, dealer_name, sm_name, reason, total, proof, explanation_of_the_reason, action_taken, forwarded_feedback, comment_after_forwarding]
        """
        # Поддержка коротких строк (например, если proof или comment отсутствуют)
        def safe(idx):
            return row[idx] if len(row) > idx and row[idx] != "" else None

        return {
            "related_date": self.parse_date(safe(0)),
            "related_shift": safe(1),
            "floor": safe(2),
            "game": safe(3),
            "dealer_name": safe(4),
            "sm_name": safe(5),
            "reason": safe(6),
            "total": self.safe_int(safe(7)),
            "proof": safe(8),
            "explanation_of_the_reason": safe(9),
            "action_taken": safe(10),
            "forwarded_feedback": safe(11),
            "comment_after_forwarding": safe(12),
        }

    def import_feedbacks_to_update(self, feedback_to_update, sheets_service, session):
        total_success = 0
        total_error = 0

        for task in feedback_to_update:
            success_count = 0
            error_count = 0
            try:
                sheet = task.raw_values_json
                if not sheet or not isinstance(sheet, list):
                    log_warning(self.log_file, "import_feedbacks_to_update", task.name_of_process, "empty_sheet", f"⚠️ Пустой или некорректный sheet в задаче: {task.name_of_process}")
                    continue

                page_name = task.target_page_name
                empty_row_streak = 0

                for row_index, row in enumerate(sheet[1:], start=2):  # пропускаем заголовок
                    if not row or not str(row[0]).isdigit():
                        continue

                    feedback_id = int(row[0])
                    try:
                        parsed = self._parse_feedback_row(row[1:], task)
                        # Пропуск строки, если related_date и related_shift пустые
                        if not parsed["related_date"] and not parsed["related_shift"]:
                            continue
                        if parsed is None:
                            empty_row_streak += 1
                            if empty_row_streak >= 15:
                                break
                            continue
                        else:
                            empty_row_streak = 0

                        # Проверка на дубликат по id
                        existing = session.query(FeedbackStorage).filter_by(id=feedback_id).first()
                        if existing:
                            for attr, val in parsed.items():
                                setattr(existing, attr, val)
                            existing.related_month = task.related_month
                        else:
                            session.add(FeedbackStorage(id=feedback_id, related_month=task.related_month, **parsed))

                        success_count += 1

                    except Exception as row_err:
                        error_count += 1
                        log_error(self.log_file, "import_feedbacks_to_update", task.name_of_process, "row_fail", f"❌ Ошибка при обработке строки {row_index} из {page_name}: {row_err}. Строка: {row}")

                session.commit()
                log_success(self.log_file, "import_feedbacks_to_update", task.name_of_process, "imported", f"✅ [{task.name_of_process}] Импортировано фидбеков: {success_count}, ошибок: {error_count}")
                total_success += success_count
                total_error += error_count

                # --- Обновление DealerMonthlyStatus ---
                log_info(self.log_file, "import_feedbacks_to_update", task.name_of_process, "dealer_status_update", f"🔄 Обновление DealerMonthlyStatus по фидбекам для {task.related_month}...")
                dealers = session.query(DealerMonthlyStatus).filter_by(related_month=task.related_month).all()
                output_data = []

                for dealer in dealers:
                    feedbacks = session.query(FeedbackStorage).filter_by(
                        dealer_name=dealer.dealer_name,
                        related_month=dealer.related_month
                    ).all()

                    if not feedbacks:
                        dealer.feedback_status = True
                        output_data.append([dealer.dealer_name, "✅"])
                        continue

                    if any(f.forwarded_feedback is None for f in feedbacks):
                        dealer.feedback_status = False
                        output_data.append([dealer.dealer_name, "❌"])
                    else:
                        dealer.feedback_status = True
                        output_data.append([dealer.dealer_name, "✅"])

                session.commit()
                log_success(self.log_file, "import_feedbacks_to_update", task.name_of_process, "dealer_status_updated", f"✅ Обновлено DealerMonthlyStatus: {len(output_data)} записей")

                # --- Выгрузка в Google Sheets ---
                try:
                    batch_data = [{
                        "range": f"{task.target_page_name}!{task.target_page_area}",
                        "values": output_data
                    }]
                    success, error = batch_update(
                        service=self.service,
                        spreadsheet_id=task.target_doc_id,
                        batch_data=batch_data,
                        token_name=self.token_name,
                        update_group="feedback_status_update",
                        log_file=self.log_file
                    )
                    if success:
                        log_success(self.log_file, "import_feedbacks_to_update", task.name_of_process, "sheet_upload", f"📤 Выгрузка статусов в Google Sheet завершена: {task.target_page_name} ({task.target_page_area})")
                    else:
                        log_error(self.log_file, "import_feedbacks_to_update", task.name_of_process, "sheet_upload_fail", f"❌ Ошибка при выгрузке в Google Sheet: {error}")
                except Exception as gs_err:
                    log_error(self.log_file, "import_feedbacks_to_update", task.name_of_process, "sheet_upload_exception", f"❌ Исключение при выгрузке в Google Sheet: {gs_err}")

            except Exception as e:
                session.rollback()
                log_error(self.log_file, "import_feedbacks_to_update", task.name_of_process, "task_fail", f"❌ Ошибка при обработке задачи {task.name_of_process}: {e}")

        log_success(self.log_file, "import_feedbacks_to_update", None, "imported_total", f"✅ Импортировано фидбеков всего: {total_success}, ошибок: {total_error}")
        log_info(self.log_file, "import_feedbacks_to_update", None, "finish", "🔄 Завершение фазы feedback_status_update.")

################################################################################################
# Импорт Schedule OT
################################################################################################

    def import_schedule_OT_to_update(self, tasks, session):
        total_new = 0
        total_updated = 0

        for task in tasks:
            new_entries = 0
            updated_entries = 0
            try:
                values = task.values_json
                if not values or not isinstance(values, list):
                    log_error(self.log_file, "import_schedule_OT_to_update", task.name_of_process, "empty_values", f"❌ values_json пуст или некорректен в задаче {task.name_of_process}")
                    continue

                related_month = task.related_month.replace(day=1)
                existing_records = session.query(ScheduleOT).filter_by(related_month=related_month).all()
                existing_lookup = {
                    (rec.dealer_name.strip(), rec.date): rec
                    for rec in existing_records if rec.dealer_name
                }

                for row in values:
                    if not row or not isinstance(row, list) or len(row) < 2:
                        continue

                    dealer_name = row[0]
                    if not dealer_name or not isinstance(dealer_name, str):
                        continue

                    dealer_name = dealer_name.strip()

                    for col_idx, shift in enumerate(row[1:], start=1):
                        shift = (shift or "").strip()
                        if shift in {"", "-", "/"}:
                            continue

                        try:
                            shift_date = related_month.replace(day=col_idx)
                        except ValueError:
                            continue

                        key = (dealer_name, shift_date)

                        if key in existing_lookup:
                            record = existing_lookup[key]
                            if record.shift_type != shift:
                                record.shift_type = shift
                                updated_entries += 1
                        else:
                            exists = session.query(ScheduleOT).filter_by(
                                dealer_name=dealer_name,
                                date=shift_date,
                                related_month=related_month
                            ).first()
                            if exists:
                                continue
                            session.add(ScheduleOT(
                                date=shift_date,
                                dealer_name=dealer_name,
                                shift_type=shift,
                                related_month=related_month
                            ))
                            new_entries += 1

                session.commit()
                log_success(self.log_file, "import_schedule_OT_to_update", task.name_of_process, "imported", f"📅 [{task.name_of_process}] ScheduleOT — новых: {new_entries}, обновлено: {updated_entries}")
                total_new += new_entries
                total_updated += updated_entries

            except Exception as e:
                session.rollback()
                log_error(self.log_file, "import_schedule_OT_to_update", task.name_of_process, "task_fail", f"❌ Ошибка при обработке задачи {task.name_of_process}: {e}")

        log_success(self.log_file, "import_schedule_OT_to_update", None, "imported_total", f"✅ ScheduleOT итого — новых: {total_new}, обновлено: {total_updated}")

    def import_qa_list_to_update(self, qa_list_update, session):
        """
        Импорт QA List в БД с расширенной проверкой уникальности.
        """
        total_success = 0
        total_error = 0

        for task in qa_list_update:
            success_count = 0
            error_count = 0
            try:
                sheet = task.raw_values_json
                if not self._validate_sheet(sheet):
                    log_warning(self.log_file, "import_qa_list_to_update", task.name_of_process, "empty_sheet", f"⚠️ Пустой или некорректный sheet в задаче: {task.name_of_process}")
                    continue

                page_name = task.source_page_name
                for row_index, row in enumerate(sheet[1:], start=2):  # пропускаем заголовок
                    # Пропуск строки, если dealer_name отсутствует или пустой
                    if not row or len(row) < 2 or not row[0] or not str(row[0]).strip():
                        log_warning(self.log_file, "import_qa_list_to_update", task.name_of_process, "invalid_row", f"⚠️ Пропущена строка {row_index} без dealer_name или с недостатком данных в {page_name}")
                        continue

                    if not self._is_valid_qa_row(row):
                        log_warning(self.log_file, "import_qa_list_to_update", task.name_of_process, "invalid_row", f"⚠️ Пропущена неполная строка {row_index} в {page_name}")
                        continue

                    try:
                        with session.no_autoflush:
                            exists = session.query(QaList).filter_by(
                                dealer_name=row[0]
                            ).first()
                            if exists:
                                qa_item = self._parse_qa_list_row(task, row, row_index, page_name)
                                for attr in qa_item.__table__.columns.keys():
                                    if attr != "id":
                                        setattr(exists, attr, getattr(qa_item, attr))
                            else:
                                qa_item = self._parse_qa_list_row(task, row, row_index, page_name)
                                if qa_item:
                                    session.add(qa_item)
                                    success_count += 1
                    except Exception as row_err:
                        log_error(self.log_file, "import_qa_list_to_update", task.name_of_process, "row_fail", f"❌ Ошибка при добавлении/обновлении строки {row_index} из {page_name}: {row_err}. Строка: {row}")
                        error_count += 1

                session.commit()
                log_success(self.log_file, "import_qa_list_to_update", task.name_of_process, "imported", f"✅ [{task.name_of_process}] Импортировано/обновлено QA записей: {success_count}, ошибок: {error_count}")
                total_success += success_count
                total_error += error_count

            except Exception as task_err:
                session.rollback()
                log_error(self.log_file, "import_qa_list_to_update", task.name_of_process, "task_fail", f"❌ Ошибка при обработке задачи {task.name_of_process}: {task_err}")

        log_success(self.log_file, "import_qa_list_to_update", None, "imported_total", f"✅ Импортировано/обновлено QA записей всего: {total_success}, ошибок: {total_error}")

    def _build_batch_data(self, group_tasks):
        """
        Формирует данные для batch_update для группы задач.
        """
        batch_data = []
        for task in group_tasks:
            if not task.values_json:
                continue
            batch_data.append({
                "range": f"{task.target_page_name}!{task.target_page_area}",
                "values": task.values_json
            })
        return batch_data

    def _parse_qa_list_row(self, task, row, row_index, page_name):
        """
        Преобразует строку QA List в объект QaList.
        """
        # Пример: row = [dealer_name, VIP, GENERIC, LEGENDZ, GSBJ, TURKISH, TRISTAR, TritonRL, QA_comment, Male, BJ, BC, RL, DT, HSB, swBJ, swBC, swRL, SH, gsDT]
        # Количество полей и порядок должны соответствовать вашей модели QaList!
        return QaList(
            dealer_name=row[0].strip() if row[0] else "",
            VIP=row[1] if len(row) > 1 else "",
            GENERIC=row[2] if len(row) > 2 else "",
            LEGENDZ=row[3] if len(row) > 3 else "",
            GSBJ=row[4] if len(row) > 4 else "",
            TURKISH=row[5] if len(row) > 5 else "",
            TRISTAR=row[6] if len(row) > 6 else "",
            TritonRL=row[7] if len(row) > 7 else "",
            QA_comment=row[8] if len(row) > 8 else "",
            Male=row[9] if len(row) > 9 else "",
            BJ=row[10] if len(row) > 10 else "",
            BC=row[11] if len(row) > 11 else "",
            RL=row[12] if len(row) > 12 else "",
            DT=row[13] if len(row) > 13 else "",
            HSB=row[14] if len(row) > 14 else "",
            swBJ=row[15] if len(row) > 15 else "",
            swBC=row[16] if len(row) > 16 else "",
            swRL=row[17] if len(row) > 17 else "",
            SH=row[18] if len(row) > 18 else "",
            gsDT=row[19] if len(row) > 19 else "",
        )
