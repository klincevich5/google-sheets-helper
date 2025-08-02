# scanners/sheetsinfo_scanner.py

import time
from datetime import datetime, timedelta, date
from collections import defaultdict
import traceback
import calendar
import json
from typing import Optional

from tg_bot.utils.settings_access import is_scanner_enabled
from core.data import load_sheetsinfo_tasks
from utils.logger import (
    log_info, log_success, log_warning, log_error, log_section, log_separator
)
from utils.db_orm import get_max_last_row
from utils.floor_resolver import get_floor_by_table_name
from database.session import get_session
# Вверху файла:
from core.data import refresh_materialized_views

from database.db_models import GamingTable, ScheduleOT

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
from .sheetsinfo_imports import (
    import_mistakes_to_update,
    import_feedbacks_to_update,
    import_qa_list_to_update,
)

class SheetsInfoScanner:  
    """
    SheetsInfoScanner — основной исполнительный класс для сканирования, обработки и обновления задач по Google Sheets.
    """

    def __init__(self, token_map, log_file=None):
        from core.config import SHEETSINFO_LOG
        self.token_map = token_map
        self.log_file = log_file if log_file else (SHEETSINFO_LOG if SHEETSINFO_LOG else "logs/scanner_sheetsinfo.log")
        self.tasks = []

    def run(self):
        """
        Основной цикл работы сканера: загрузка задач, сканирование, обработка, обновление.
        Каждая фаза работает в отдельной сессии, чтобы ошибки не влияли на остальные фазы.
        """
        while True:
            if not is_scanner_enabled("sheets_scanner"):
                time.sleep(10)
                continue
            try:
                log_separator(self.log_file, "run")
                log_section(self.log_file, "run", "▶️ SheetsInfo Активен. Новый цикл сканирования\n")

                token_name = list(self.token_map.keys())[0]
                token_path = self.token_map[token_name]
                self.token_name = token_name

                # Сервис и doc_id_map инициализируем один раз
                with get_session() as session:
                    self.service = load_credentials(token_path, self.log_file)
                    log_info(self.log_file, "run", None, "token", f"Используется токен: {self.token_name}")
                    from core.data import return_tracked_tables
                    self.doc_id_map = return_tracked_tables(session)

                # Каждая фаза — отдельная сессия
                for phase_name, method in [
                    ("load_tasks", self.load_tasks),
                    ("scan_phase", self.scan_phase),
                    ("process_phase", self.process_phase),
                    ("update_phase", self.update_phase),
                ]:
                    log_separator(self.log_file, phase_name)
                    try:
                        log_info(self.log_file, phase_name, None, "start", f"Старт этапа {phase_name}")
                        with get_session() as session:
                            method(session)
                        log_success(self.log_file, phase_name, None, "finish", f"Этап {phase_name} завершён\n")
                    except Exception as e:
                        log_error(self.log_file, phase_name, None, "fail", f"Ошибка на этапе {phase_name}", exc=e)
                        # Не прерываем цикл, просто логируем ошибку

            except Exception as e:
                log_error(self.log_file, "run", None, "fail", "Критическая ошибка в основном цикле", exc=e)
                time.sleep(10)

            # Гарантированная задержка между циклами (не менее 3 секунд)
            interval = max(SHEETINFO_INTERVAL, 3)
            time.sleep(interval)

#############################################################################################
# загрузка задач из БД
#############################################################################################

    def load_tasks(self, session):
        log_section(self.log_file, "load_tasks", "📥 Загрузка задач из SheetsInfo")

        # Загружаем с передачей doc_id_map
        self.tasks = load_sheetsinfo_tasks(session, self.log_file)

        if not self.tasks:
            log_info(self.log_file, "load_tasks", None, "empty", "Нет активных задач для сканирования")
            self.tasks = []
            return

        skipped = 0
        for task in self.tasks:
            try:
                ok = task.assign_doc_ids(self.doc_id_map, self.log_file)
                if not ok:
                    skipped += 1
                    log_warning(self.log_file, "load_tasks", task.name_of_process, "skipped", "Нет doc_id, задача пропущена")
            except Exception as e:
                skipped += 1
                log_error(self.log_file, "load_tasks", task.name_of_process, "fail", "Ошибка в assign_doc_ids", exc=e)

        log_info(self.log_file, "load_tasks", None, "done",
                f"Загружено задач: {len(self.tasks)}, пропущено без doc_id: {skipped}")

#############################################################################################
# Фаза сканирования
#############################################################################################

    def scan_phase(self, session):
        log_section(self.log_file, "scan_phase", "🔍 Фаза сканирования")

        if not self.tasks:
            log_info(self.log_file, "scan_phase", None, "empty", "Нет задач для сканирования")
            return

        # 🧩 Группировка по scan_group
        scan_groups = defaultdict(list)
        for task in self.tasks:
            scan_groups[task.scan_group].append(task)

        # 📦 Обработка каждой группы
        for scan_group, group_tasks in scan_groups.items():
            log_section(self.log_file, "scan_phase", f"\n🗂️ Обработка scan_group: {scan_group} ({len(group_tasks)} задач)\n")
            if not group_tasks:
                continue

            doc_id = group_tasks[0].source_doc_id
            unique_sheet_names = set(task.source_page_name for task in group_tasks)

            # ✅ Проверка наличия листов (первая работа с API)
            exists_map = {
                sheet_name: check_sheet_exists(self.service, doc_id, sheet_name, self.log_file, self.token_name)
                for sheet_name in unique_sheet_names
            }

            for sheet_name, exists in exists_map.items():
                log_info(self.log_file, "scan_phase", None, "sheet_exists", f"Лист '{sheet_name}' {'существует' if exists else 'не найден'}")

            # 🧾 Фильтрация валидных задач
            valid_tasks = []
            for task in group_tasks:
                if exists_map.get(task.source_page_name):
                    valid_tasks.append(task)
                else:
                    log_warning(self.log_file, "scan_phase", task.name_of_process, "skipped", f"Лист '{task.source_page_name}' не найден")
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")

            if not valid_tasks:
                log_info(self.log_file, "scan_phase", None, "empty", f"Все задачи группы {scan_group} отфильтрованы. Пропуск batchGet.")
                continue

            # 🔗 Группировка по диапазонам
            range_to_tasks = defaultdict(list)
            for task in valid_tasks:
                range_str = f"{task.source_page_name}!{task.source_page_area}"
                range_to_tasks[range_str].append(task)

            ranges = list(range_to_tasks.keys())
            log_info(self.log_file, "scan_phase", None, "batch_get", f"Отправка batchGet на документ {task.source_table_type} с {len(ranges)} диапазонами")

            # 📥 batchGet запрос
            response_data = batch_get(
                self.service,
                doc_id,
                ranges,
                scan_group,
                self.log_file,
                self.token_name
            )

            # ❌ Ошибка запроса — все задачи failed
            if not response_data:
                for task in valid_tasks:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")
                log_warning(self.log_file, "scan_phase", None, "empty", "Пустой ответ от batchGet. Все задачи будут отмечены как неудачные.")
                continue

            # ✅ Очистка ключей от кавычек
            normalized_response = {}
            for k, v in response_data.items():
                clean_key = k.replace("'", "")
                if "!" in clean_key:
                    sheet_name, cells_range = clean_key.split("!", 1)
                    normalized_response[(sheet_name.strip(), cells_range.strip())] = v

            # 📤 Назначение значений задачам
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

        # 🧾 Финальный отчёт
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
                    task.process_raw_value(self.log_file)
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
                        session.commit()  # коммитим изменения по задаче
                        log_success(self.log_file, "process_phase", task.name_of_process, "changed", "Данные изменены и сохранены")
                    except Exception as e:
                        session.rollback()  # откатываем только изменения по этой задаче
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

        grouped_tasks = defaultdict(list)
        updated_groups = set()

        # Группировка задач по update_group
        for t in self.tasks:
            if not (t.values_json and t.changed):
                log_warning(self.log_file, "update_phase", t.name_of_process, "skipped", f"Пропуск задачи {t.name_of_process}: нет изменений или пустой values_json")
                continue
            grouped_tasks[t.update_group].append(t)

        # Основной импорт — одной функцией
        for group_name, tasks in grouped_tasks.items():
            time.sleep(5)  # Задержка для избежания перегрузки API
            log_info(self.log_file, "update_phase", None, group_name, f"🔼 Обновление {group_name}: {len(tasks)}")
            try:
                self.import_tasks_to_update(tasks, session)
                updated_groups.add(group_name)
            except Exception as e:
                session.rollback()
                log_error(self.log_file, "update_phase", None, f"{group_name}_fail", f"❌ Ошибка при обновлении группы: {e}")
            time.sleep(3)

        # Обновление материализованных вью
        refresh_materialized_views(session, updated_groups, self.log_file)

        # Статистика
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

            doc_ids = set(t.target_doc_id for t in group_tasks)
            if len(doc_ids) != 1:
                log_error(self.log_file, "import_tasks_to_update", None, "multi_doc_id", f"❌ В группе {update_group} несколько doc_id: {doc_ids}. Пропуск.")
                continue
            doc_id = doc_ids.pop()

            valid_tasks = self._build_batch_data(group_tasks, session)
            if not valid_tasks:
                log_warning(self.log_file, "import_tasks_to_update", None, "no_data", f"⚠️ Нет валидных данных для batchUpdate группы {update_group}. Пропуск.")
                continue

            batch_data = [{
                "range": f"{task.target_page_name}!{task.target_page_area}",
                "values": values
            } for task, values in valid_tasks.items()]

            try:
                success, error = self._try_batch_update(doc_id, batch_data, update_group)
                if success:
                    log_success(self.log_file, "import_tasks_to_update", None, "batch_update", f"✅ Пакетное обновление успешно для группы {update_group}")
                    self._commit_task_updates(valid_tasks.keys(), session, success=True)
                else:
                    log_error(self.log_file, "import_tasks_to_update", None, "batch_update_fail", f"❌ Ошибка при пакетной отправке: {error}")
                    self._fallback_single_upload(valid_tasks, doc_id, update_group, session)
            except Exception as e:
                log_error(self.log_file, "import_tasks_to_update", None, "batch_update_exception", f"❌ Исключение при batch_update: {e}")
                self._fallback_single_upload(valid_tasks, doc_id, update_group, session)

    def _convert_jsonb_to_tabular(self, jsonb_data: list) -> list:
        """
        Преобразует список словарей (JSONB) в таблицу (список списков),
        где первая строка — заголовки, а последующие — значения.
        """
        if not jsonb_data or not isinstance(jsonb_data, list):
            return []

        if not isinstance(jsonb_data[0], dict):
            raise ValueError("Ожидался список словарей")

        headers = list(jsonb_data[0].keys())
        table = [headers]

        for row in jsonb_data:
            table.append([row.get(h, "") for h in headers])

        return table

    def _build_batch_data(self, tasks, session):
        valid_tasks = {}
        for task in tasks:
            try:
                raw = json.loads(task.values_json) if isinstance(task.values_json, str) else task.values_json
                values = self._convert_jsonb_to_tabular(raw)
                if not isinstance(values, list):
                    raise ValueError("JSONB должен быть списком списков")
                valid_tasks[task] = values
            except Exception as e:
                log_error(self.log_file, "import_tasks_to_update", task.name_of_process, "json_decode_error", f"❌ Невалидный JSONB: {e}")
                try:
                    task.update_after_upload(success=False)
                    update_task_update_fields(session=session, task=task, log_file=self.log_file, table_name="SheetsInfo")
                    session.commit()
                except Exception as inner:
                    session.rollback()
                    log_error(self.log_file, "import_tasks_to_update", task.name_of_process, "db_flag_fail", f"❌ Не удалось пометить задачу как failed: {inner}")
        return valid_tasks

    def _fallback_single_upload(self, valid_tasks, doc_id, update_group, session):
        for task, values in valid_tasks.items():
            try:
                single_data = [{
                    "range": f"{task.target_page_name}!{task.target_page_area}",
                    "values": values
                }]
                success, error = self._try_batch_update(doc_id, single_data, update_group)
                if success:
                    log_success(self.log_file, "import_tasks_to_update", task.name_of_process, "single_update", f"✅ [Task {task.name_of_process} {task.source_page_name} {task.related_month}] Обновлена поштучно.")
                else:
                    log_error(self.log_file, "import_tasks_to_update", task.name_of_process, "single_update_fail", f"❌ [Task {task.name_of_process} {task.source_page_name} {task.related_month}] Ошибка при обновлении: {error}")

                task.update_after_upload(success=success)
                update_task_update_fields(session=session, task=task, log_file=self.log_file, table_name="SheetsInfo")
                session.commit()
            except Exception as e:
                session.rollback()
                log_error(self.log_file, "import_tasks_to_update", task.name_of_process, "single_update_exception", f"❌ Исключение при поштучном обновлении: {e}")

    def _try_batch_update(self, doc_id, batch_data, update_group, retries=3):
        last_error = None
        for attempt in range(retries):
            try:
                return batch_update(
                    service=self.service,
                    spreadsheet_id=doc_id,
                    batch_data=batch_data,
                    token_name=self.token_name,
                    update_group=update_group,
                    log_file=self.log_file
                )
            except Exception as e:
                last_error = e
                time.sleep(2 ** attempt)
        return False, last_error

    def _commit_task_updates(self, tasks, session, success):
        try:
            for task in tasks:
                # Проверка на корректность task перед обновлением
                if not hasattr(task, "update_after_upload") or not hasattr(task, "name_of_process"):
                    log_error(self.log_file, "import_tasks_to_update", None, "invalid_task", f"❌ Некорректный объект задачи: {repr(task)}")
                    continue
                task.update_after_upload(success=success)
                update_task_update_fields(session=session, task=task, log_file=self.log_file, table_name="SheetsInfo")
            session.commit()
        except Exception as e:
            session.rollback()
            log_error(self.log_file, "import_tasks_to_update", None, "db_update_fail", f"❌ Ошибка при обновлении статуса задач: {e}")