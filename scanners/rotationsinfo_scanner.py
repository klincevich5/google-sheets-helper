# scanners/rotationsinfo_scanner.py

import time
from collections import defaultdict

from tg_bot.utils.settings_access import is_scanner_enabled
from core.data import load_rotationsinfo_tasks
from utils.logger import (
    log_info, log_success, log_warning, log_error, log_section, log_separator
)
from utils.formatting_utils import format_sheet
from database.session import SessionLocal

from core.config import (
    ROTATIONSINFO_LOG,
    ROTATIONSINFO_INTERVAL,
    ROTATION_ORDER
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

class RotationsInfoScanner:
    def __init__(self, token_map, doc_id_map):
        self.token_map = token_map
        self.doc_id_map = doc_id_map
        self.log_file = ROTATIONSINFO_LOG
        self.tasks = []

    def run(self):
        while True:
            try:
                if not is_scanner_enabled("rotations_scanner"):
                    time.sleep(10)
                    continue

                log_separator(self.log_file, "run")
                log_section(self.log_file, "run", "▶️ RotationsInfo Активен. Новый цикл сканирования\n")

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

            time.sleep(ROTATIONSINFO_INTERVAL)

    def load_tasks(self, session):
        log_section(self.log_file, "load_tasks", "📥 Загрузка задач из RotationsInfo")
        self.tasks = load_rotationsinfo_tasks(session, self.log_file)
        if not self.tasks:
            log_info(self.log_file, "load_tasks", None, "empty", "Нет активных задач для сканирования")
            self.tasks = []
            return
        skipped = 0
        for task in self.tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                skipped += 1
                log_warning(self.log_file, "load_tasks", getattr(task, 'name_of_process', None), "skipped", "Нет doc_id, задача пропущена")
        log_info(self.log_file, "load_tasks", None, "done", f"Загружено задач: {len(self.tasks)}, пропущено без doc_id: {skipped}\n")

    def scan_phase(self, session):
        phase = "scan_phase"
        log_section(self.log_file, phase, "🔍 Фаза сканирования")
        if not self.tasks:
            log_info(self.log_file, phase, None, "empty", "Нет задач для сканирования")
            return
        ready_tasks = [task for task in self.tasks if task.is_ready_to_scan()]
        log_info(self.log_file, phase, None, "ready", f"Готовых задач: {len(ready_tasks)}")
        if not ready_tasks:
            log_info(self.log_file, phase, None, "empty", "Нет задач, готовых к сканированию")
            return
        scan_groups = defaultdict(list)
        for task in ready_tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                log_warning(self.log_file, phase, getattr(task, 'name_of_process', None), "skipped", "Не удалось сопоставить doc_id. Пропуск.")
                continue
            scan_groups[task.scan_group].append(task)
        for scan_group, group_tasks in scan_groups.items():
            log_section(self.log_file, phase, f"\n🗂️ Обработка scan_group: {scan_group} ({len(group_tasks)} задач)\n")
            if not group_tasks:
                continue
            doc_id = group_tasks[0].source_doc_id
            unique_sheet_names = set(task.source_page_name for task in group_tasks)
            exists_map = {
                sheet_name: check_sheet_exists(self.service, doc_id, sheet_name, self.log_file, self.token_name)
                for sheet_name in unique_sheet_names
            }
            for sheet_name, exists in exists_map.items():
                log_info(self.log_file, phase, None, "sheet_exists", f"Лист '{sheet_name}' {'существует' if exists else 'не найден'}")
            valid_tasks = []
            for task in group_tasks:
                sheet_name = task.source_page_name
                if exists_map.get(sheet_name):
                    valid_tasks.append(task)
                else:
                    log_warning(self.log_file, phase, task.name_of_process, "skipped", f"Лист '{sheet_name}' не найден")
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="RotationsInfo")
            if not valid_tasks:
                log_info(self.log_file, phase, None, "empty", f"Все задачи группы {scan_group} отфильтрованы. Пропуск batchGet.")
                continue
            range_to_tasks = defaultdict(list)
            for task in valid_tasks:
                range_str = f"{task.source_page_name}!{task.source_page_area}"
                range_to_tasks[range_str].append(task)
            ranges = list(range_to_tasks.keys())
            log_info(self.log_file, phase, None, "batch_get", f"Отправка batchGet на документ {task.source_table_type} с {len(ranges)} диапазонами")
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
                    update_task_scan_fields(session, task, self.log_file, table_name="RotationsInfo")
                log_warning(self.log_file, phase, None, "empty", "Пустой ответ от batchGet. Все задачи будут отмечены как неудачные.")
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
                    update_task_scan_fields(session, task, self.log_file, table_name="RotationsInfo")
                    log_success(self.log_file, phase, task.name_of_process, "found", f"Найден диапазон {sheet_name}!{cells_range}, строк: {len(matched_values)}")
                else:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="RotationsInfo")
                    log_warning(self.log_file, phase, task.name_of_process, "not_found", f"Диапазон {expected_sheet}!{task.source_page_area} не найден или пуст.")
        log_info(self.log_file, phase, None, "summary", "\n".join(
            [f"• {task.name_of_process} {task.source_page_name}: scanned={task.scanned}, processed={task.proceed}, changed={task.changed}, uploaded={task.uploaded}"
             for task in self.tasks]
        ) + "\n")
        log_success(self.log_file, phase, None, "finish", "Фаза сканирования завершена\n")

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
                        update_task_process_fields(session, task, self.log_file, table_name="RotationsInfo")
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
        has_main_changes = any(task.changed for task in self.tasks if task.update_group == "update_main")
        has_shuffle_changes = any(task.changed for task in self.tasks if "shuffle" in task.update_group)

        main_tasks = [
            task for task in self.tasks
            if task.update_group == "update_main" and task.values_json and has_main_changes
        ]
        shuffle_tasks = [
            task for task in self.tasks
            if "shuffle" in task.update_group and task.values_json and has_shuffle_changes
        ]

        if main_tasks:
            try:
                log_info(self.log_file, "update_phase", None, "main_start", f"Начало обновления основной группы задач: {len(main_tasks)}")
                self.import_main_data(main_tasks, session)
                log_success(self.log_file, "update_phase", None, "main_finish", "Обновление основной группы задач завершено")
            except Exception as e:
                log_error(self.log_file, "update_phase", None, "main_fail", "Ошибка при обновлении update_main", exc=e)

        if shuffle_tasks:
            try:
                log_info(self.log_file, "update_phase", None, "shuffle_start", f"Начало обновления shuffle-группы задач: {len(shuffle_tasks)}")
                self.import_shuffle_data(shuffle_tasks, session)
                log_success(self.log_file, "update_phase", None, "shuffle_finish", "Обновление shuffle-группы задач завершено")
            except Exception as e:
                log_error(self.log_file, "update_phase", None, "shuffle_fail", "Ошибка при обновлении update_shuffle", exc=e)

        log_info(self.log_file, "update_phase", None, "summary", "\n".join(
            [f"• {task.name_of_process} {task.source_page_name}: scanned={task.scanned}, processed={task.proceed}, changed={task.changed}, uploaded={task.uploaded}"
             for task in self.tasks]
        ) + "\n")
        if not main_tasks and not shuffle_tasks:
            log_section(self.log_file, "update_phase", "⚪ Нет задач для обновления. Пропуск.\n")
        log_success(self.log_file, "update_phase", None, "finish", "Этап update_phase завершён\n")

    def import_main_data(self, all_main_tasks, session):
        try:
            grouped_by_page = defaultdict(list)
            for task in all_main_tasks:
                grouped_by_page[task.target_page_name].append(task)

            for page_name, tasks in grouped_by_page.items():
                try:
                    log_info(self.log_file, "import_main_data", None, "page_start", f"Страница: {page_name}, задач: {len(tasks)}")
                    task_map = {task.name_of_process: task for task in tasks}
                    sorted_tasks = []
                    all_values = []

                    for name in ROTATION_ORDER:
                        try:
                            task = task_map.get(name)
                            if not task:
                                log_warning(self.log_file, "import_main_data", name, "not_found", "Задача не найдена")
                                continue

                            values = task.values_json
                            if not values or not isinstance(values, list):
                                task.update_after_upload(False)
                                update_task_update_fields(
                                    session=session,
                                    task=task,
                                    log_file=self.log_file,
                                    table_name="RotationsInfo"
                                )
                                log_warning(self.log_file, "import_main_data", name, "no_data", "Нет корректных данных. Пропуск.")
                                continue

                            flat = [str(cell).strip().upper() for row in values for cell in row if cell is not None]
                            if flat == ["NULL"]:
                                task.update_after_upload(False)
                                update_task_update_fields(
                                    session=session,
                                    task=task,
                                    log_file=self.log_file,
                                    table_name="RotationsInfo"
                                )
                                log_warning(self.log_file, "import_main_data", name, "null", "Содержит 'NULL'. Пропуск.")
                                continue

                            log_info(self.log_file, "import_main_data", name, "data", f"{len(values)} строк (новые данные)")
                            sorted_tasks.append(task)
                            all_values.extend(values)
                            all_values.append([""] * 26)

                        except Exception as e:
                            log_error(self.log_file, "import_main_data", name, "fail", "Ошибка при обработке задачи", exc=e)

                    if not sorted_tasks:
                        log_info(self.log_file, "import_main_data", page_name, "empty", "Нет задач с данными для вставки. Пропуск смены.")
                        continue

                    if all_values[-1] == [""] * 26:
                        all_values.pop()

                    if len(all_values) < 100:
                        padding = 100 - len(all_values)
                        all_values.extend([[""] * 26 for _ in range(padding)])
                        log_info(self.log_file, "import_main_data", page_name, "padding", f"Добавлены {padding} пустых строк до 100.")
                    elif len(all_values) > 100:
                        all_values = all_values[:100]
                        log_warning(self.log_file, "import_main_data", page_name, "truncate", "Обрезано до 100 строк.")

                    reference_task = sorted_tasks[0]
                    spreadsheet_id = reference_task.target_doc_id
                    target_page_area = reference_task.target_page_area
                    insert_range = f"{page_name}!{target_page_area}"

                    batch_data = [{
                        "range": insert_range,
                        "values": all_values
                    }]

                    try:
                        success, error = batch_update(
                            service=self.service,
                            spreadsheet_id=spreadsheet_id,
                            batch_data=batch_data,
                            token_name=self.token_name,
                            update_group=reference_task.update_group,
                            log_file=self.log_file
                        )
                        if success:
                            log_success(self.log_file, "import_main_data", page_name, "uploaded", "Вставка смены завершена успешно")
                        else:
                            log_error(self.log_file, "import_main_data", page_name, "fail", f"Ошибка batch_update: {error}")
                    except Exception as e:
                        log_error(self.log_file, "import_main_data", page_name, "fail", "Ошибка batch_update", exc=e)

                    try:
                        format_sheet(
                            service=self.service,
                            spreadsheet_id=spreadsheet_id,
                            sheet_title=page_name,
                            values=all_values,
                            token_name=self.token_name,
                            update_group=reference_task.update_group,
                            log_file=self.log_file,
                            session=session
                        )
                        log_success(self.log_file, "import_main_data", page_name, "formatted", "Лист отформатирован")
                    except Exception as e:
                        log_error(self.log_file, "import_main_data", page_name, "fail", "Ошибка при форматировании листа", exc=e)

                    for task in sorted_tasks:
                        try:
                            task.update_after_upload(success)
                            update_task_update_fields(
                                session=session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                        except Exception as e:
                            log_error(self.log_file, "import_main_data", task.name_of_process, "fail", "Ошибка при обновлении task", exc=e)

                except Exception as e:
                    log_error(self.log_file, "import_main_data", page_name, "fail", "Критическая ошибка при обработке страницы", exc=e)

        except Exception as e:
            log_error(self.log_file, "import_main_data", None, "fail", "Ошибка при обновлении update_main", exc=e)

    def import_shuffle_data(self, tasks, session):
        log_section(self.log_file, "import_shuffle_data", "📥 Импорт данных для update_shuffle")
        shuffle_groups = defaultdict(list)
        for task in tasks:
            shuffle_groups[task.update_group].append(task)

        for update_group, group_tasks in shuffle_groups.items():
            pages = defaultdict(list)
            for task in group_tasks:
                pages[task.target_page_name].append(task)

            for page_name, page_tasks in pages.items():
                reference_task = page_tasks[0]
                spreadsheet_id = reference_task.target_doc_id

                log_info(self.log_file, "import_shuffle_data", page_name, "start", f"Документ: {page_tasks[0].name_of_process}, страница: {page_name}")

                try:
                    raw = batch_get(
                        service=self.service,
                        spreadsheet_id=spreadsheet_id,
                        ranges=[f"{page_name}!D1:AC200"],
                        scan_group=update_group,
                        log_file=self.log_file,
                        token_name=self.token_name
                    )
                    sheet_values = list(raw.values())[0] if raw else []

                    shift_row_index = None
                    for idx, row in enumerate(sheet_values):
                        if row and isinstance(row[0], str) and "shift:" in row[0].lower():
                            log_info(self.log_file, "import_shuffle_data", page_name, "shift_row", f"row number for shift: {row}")
                            shift_row_index = idx + 1
                            break

                    if shift_row_index is None:
                        log_warning(self.log_file, "import_shuffle_data", page_name, "no_shift", "Строка с 'shift:' не найдена. Пропуск страницы.")
                        for task in page_tasks:
                            task.update_after_upload(False)
                            update_task_update_fields(
                                session=session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                        continue

                    all_values = []
                    tasks_with_data = []

                    for task in page_tasks:
                        if not task.values_json or not isinstance(task.values_json, list):
                            task.update_after_upload(False)
                            update_task_update_fields(
                                session=session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                            log_warning(self.log_file, "import_shuffle_data", task.name_of_process, "no_data", "Нет данных. Пропуск.")
                            continue

                        flat = [str(cell).strip().upper() for row in task.values_json for cell in row if cell is not None]
                        if flat == ["NULL"]:
                            task.update_after_upload(False)
                            update_task_update_fields(
                                session=session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                            log_warning(self.log_file, "import_shuffle_data", task.name_of_process, "null", "Содержит 'NULL'. Пропуск.")
                            continue

                        all_values.extend(task.values_json)
                        tasks_with_data.append(task)

                    if not tasks_with_data:
                        log_info(self.log_file, "import_shuffle_data", page_name, "empty", "Нет допустимых данных на странице. Пропуск.")
                        continue

                    start_row = shift_row_index + 1
                    end_row = start_row + len(all_values) - 1
                    insert_range = f"{page_name}!D{start_row}:AC{end_row}"

                    batch_data = [{
                        "range": insert_range,
                        "values": all_values
                    }]

                    success, error = batch_update(
                        service=self.service,
                        spreadsheet_id=spreadsheet_id,
                        batch_data=batch_data,
                        token_name=self.token_name,
                        update_group=update_group,
                        log_file=self.log_file
                    )

                    for task in page_tasks:
                        if task in tasks_with_data:
                            task.update_after_upload(success)
                            update_task_update_fields(
                                session=session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                    if success:
                        log_success(self.log_file, "import_shuffle_data", page_name, "uploaded", "Успешная вставка данных страницы")
                    else:
                        log_error(self.log_file, "import_shuffle_data", page_name, "fail", f"Ошибка вставки страницы: {error}")

                except Exception as e:
                    log_error(self.log_file, "import_shuffle_data", page_name, "fail", "Критическая ошибка страницы", exc=e)
                    for task in page_tasks:
                        task.update_after_upload(False)
                        update_task_update_fields(
                            session=session,
                            task=task,
                            log_file=self.log_file,
                            table_name="RotationsInfo"
                        )