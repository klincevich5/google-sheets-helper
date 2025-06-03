# scanners/rotationsinfo_scanner.py

import time
from collections import defaultdict

from tg_bot.utils.settings_access import is_scanner_enabled
from core.data import load_rotationsinfo_tasks
from utils.logger import log_to_file, log_separator, log_section
from utils.formatting_utils import format_sheet
from core.token_manager import TokenManager

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
    def __init__(self, session, token_map, doc_id_map):
        self.session = session
        self.token_map = token_map
        self.doc_id_map = doc_id_map
        self.log_file = ROTATIONSINFO_LOG
        self.tasks = []

    def run(self):
        try:
            manager = TokenManager(self.token_map)
        except Exception as e:
            print(f"❌ Ошибка при инициализации TokenManager: {e}")
            raise

        while True:
            try:
                if not is_scanner_enabled("rotations_scanner"):
                    time.sleep(10)
                    continue

                try:
                    self.token_name, token_path = manager.select_best_token(self.log_file, self.session)
                    log_to_file(self.log_file, f"🔑 Выбран {self.token_name} with token_path {token_path}")
                    self.service = load_credentials(token_path, self.log_file, self.session)
                    log_to_file(self.log_file, f"🔐 Используется токен: {self.token_name}")
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка при выборе токена: {e}")
                    time.sleep(10)
                    continue

                # # Получаем актуальный doc_id_map перед каждой фазой
                # log_to_file(self.log_file, "♻️ Обновление doc_id_map...")
                # self.doc_id_map = self.shared_doc_map.get()
                # log_to_file(self.log_file, f"📑 doc_id_map обновлён: {len(self.doc_id_map)} записей")

                for phase_name, method in [
                    ("загрузки задач", self.load_tasks),
                    ("сканирования", self.scan_phase),
                    ("обработки", self.process_phase),
                    ("обновления", self.update_phase)
                ]:
                    try:
                        method()
                    except Exception as e:
                        log_to_file(self.log_file, f"❌ Ошибка на этапе {phase_name}: {e}")
                        raise

            except Exception as e:
                log_to_file(self.log_file, f"❌ Критическая ошибка в основном цикле: {e}")
                time.sleep(10)
                continue

            finally:
                self.session.close()

            time.sleep(ROTATIONSINFO_INTERVAL)

#############################################################################################
# загрузка задач из БД
#############################################################################################

    def load_tasks(self):
        # log_section("📥 Загрузка задач из RotationsInfo", self.log_file)

        self.tasks = load_rotationsinfo_tasks(self.session, self.log_file)

        if not self.tasks:
            # log_to_file(self.log_file, "⚪ Нет активных задач для сканирования.")
            self.tasks = []
            return

        # log_to_file(self.log_file, f"🔄 Загружено задач: {len(self.tasks)}")

        skipped = 0
        for task in self.tasks:
            if not task.assign_doc_ids(self.doc_id_map):

                skipped += 1

        # if skipped:
        #     log_to_file(self.log_file, f"⚠️ Пропущено задач без doc_id: {skipped}")

#############################################################################################
# Фаза сканирования
#############################################################################################

    def scan_phase(self):
        # log_section("🔍 Фаза сканирования", self.log_file)

        if not self.tasks:
            # log_to_file(self.log_file, "⚪ Нет задач для сканирования.")
            return

        ready_tasks = [task for task in self.tasks if task.is_ready_to_scan()]
        if not ready_tasks:
            # log_to_file(self.log_file, "⚪ Нет задач, готовых к сканированию.")
            return

        # log_to_file(self.log_file, f"🔎 Найдено {len(ready_tasks)} задач, готовых к сканированию:")

        scan_groups = defaultdict(list)
        for task in ready_tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process}] Не удалось сопоставить doc_id. Пропуск.")
                continue
            scan_groups[task.scan_group].append(task)

        for scan_group, group_tasks in scan_groups.items():
            # log_separator(self.log_file)
            log_to_file(self.log_file, f"📘 Обработка scan_group: {scan_group} ({len(group_tasks)} задач)")

            if not group_tasks:
                # log_to_file(self.log_file, "⚪ В группе нет задач.")
                continue

            doc_id = group_tasks[0].source_doc_id
            unique_sheet_names = set(task.source_page_name for task in group_tasks)
            # log_to_file(self.log_file, f"Уникальные названия листов: {unique_sheet_names}")

            exists_map = {
                sheet_name: check_sheet_exists(self.service, doc_id, sheet_name, self.log_file, self.token_name, self.session)
                for sheet_name in unique_sheet_names
            }

            for sheet_name, exists in exists_map.items():
                log_to_file(self.log_file, f"{'✅' if exists else '⚠️'} Лист '{sheet_name}' {'существует' if exists else 'не найден'}.")

            valid_tasks = []
            for task in group_tasks:
                sheet_name = task.source_page_name
                if exists_map.get(sheet_name):
                    log_to_file(self.log_file, f"➡️ Используем '{sheet_name}' для задачи {task.name_of_process}.")
                    valid_tasks.append(task)
                else:
                    log_to_file(self.log_file, f"⛔ Пропуск задачи {task.name_of_process}: лист '{sheet_name}' не найден.")
                    task.update_after_scan(success=False)
                    update_task_scan_fields(self.session, task, self.log_file, table_name="RotationsInfo")

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

            response_data = batch_get(
                self.service,
                doc_id,
                ranges,
                scan_group,
                self.log_file,
                self.token_name,
                self.session
            )
            if not response_data:
                # log_to_file(self.log_file, "❌ Пустой ответ от batchGet. Все задачи будут отмечены как неудачные.")
                for task in valid_tasks:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(self.session, task, self.log_file, table_name="RotationsInfo")
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
                    update_task_scan_fields(self.session, task, self.log_file, table_name="RotationsInfo")
                    log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Найден диапазон {sheet_name}!{cells_range}, строк: {len(matched_values)}")
                else:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(self.session, task, self.log_file, table_name="RotationsInfo")
                    log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process}] Диапазон {expected_sheet}!{task.source_page_area} не найден или пуст.")

        for task in self.tasks:
            log_to_file(
                self.log_file,
                f"⚪ [Task {task.name_of_process}] Отсканировано: {task.scanned} | "
                f"Обработано: {task.proceed} | Изменено: {task.changed} | Загружено: {task.uploaded}"
            )

#############################################################################################
# Фаза обработки
#############################################################################################

    def process_phase(self):
        log_section("🛠️ Фаза обработки", self.log_file)

        if not self.tasks:
            # log_to_file(self.log_file, "⚪ Нет задач для обработки.")
            return

        for task in self.tasks:
            if task.scanned == 0:
                continue

            try:
                # Обработка сырых значений
                try:
                    task.process_raw_value()
                except Exception as e:
                    # log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка в process_raw_value: {e}")
                    continue

                # Проверка изменений
                try:
                    task.check_for_update()
                except Exception as e:
                    # log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка в check_for_update: {e}")
                    continue

                # Обновление в БД, если данные изменились
                if task.changed:
                    try:
                        update_task_process_fields(self.session, task, self.log_file, table_name="RotationsInfo")
                    except Exception as e:
                        log_to_file(self.log_file, f"❌ Ошибка при сохранении изменений в БД: {e}")

            except Exception as e:
                log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Неизвестная ошибка при обработке: {e}")

        # Итоговый отчёт
        for task in self.tasks:
            log_to_file(
                self.log_file,
                f"⚪ [Task {task.name_of_process}] Отсканировано: {task.scanned} | "
                f"Обработано: {task.proceed} | Изменено: {task.changed} | Загружено: {task.uploaded}"
            )

#############################################################################################
# Фаза обновления
#############################################################################################

    def update_phase(self):
        # log_section("🔼 Фаза обновления", self.log_file)

        # Определяем наличие изменений по группам
        has_main_changes = any(task.changed for task in self.tasks if task.update_group == "update_main")
        has_shuffle_changes = any(task.changed for task in self.tasks if "shuffle" in task.update_group)

        # Фильтруем задачи для каждой группы
        main_tasks = [
            task for task in self.tasks
            if task.update_group == "update_main" and task.values_json and has_main_changes
        ]
        shuffle_tasks = [
            task for task in self.tasks
            if "shuffle" in task.update_group and task.values_json and has_shuffle_changes
        ]

        # Обработка основной группы задач
        if main_tasks:
            try:
                self.import_main_data(main_tasks)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении update_main: {e}")

        # Обработка shuffle-группы задач
        if shuffle_tasks:
            try:
                self.import_shuffle_data(shuffle_tasks)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении update_shuffle: {e}")

        # Логируем результат для каждой задачи
        for task in self.tasks:
            log_to_file(
                self.log_file,
                f"⚪ [Task {task.name_of_process} {task.source_page_name}] "
                f"Отсканировано: {task.scanned} | Обработано: {task.proceed} | "
                f"Изменено: {task.changed} | Загружено: {task.uploaded}"
            )

        # Финальное сообщение
        if not main_tasks and not shuffle_tasks:
            log_section("⚪ Нет задач для обновления. Пропуск.", self.log_file)
        else:
            log_section("🔼 Обновление завершено.", self.log_file)

##############################################################################################
# Импорт данных в Main
##############################################################################################

    def import_main_data(self, all_main_tasks):
        try:
            grouped_by_page = defaultdict(list)
            for task in all_main_tasks:
                grouped_by_page[task.target_page_name].append(task)

            for page_name, tasks in grouped_by_page.items():
                try:
                    # log_to_file(self.log_file, f"📄 Лист: {page_name} ({len(tasks)} задач)")
                    # log_to_file(self.log_file, "")

                    task_map = {task.name_of_process: task for task in tasks}
                    sorted_tasks = []
                    all_values = []

                    for name in ROTATION_ORDER:
                        try:
                            task = task_map.get(name)
                            if not task:
                                # log_to_file(self.log_file, f"⚠️ Задача '{name}' не найдена.")
                                continue

                            values = task.values_json
                            if not values or not isinstance(values, list):
                                # log_to_file(self.log_file, f"⚪ [Task {name} {getattr(task, 'source_page_name', '?')}] нет корректных данных. Пропуск.")
                                task.update_after_upload(False)
                                update_task_update_fields(
                                    session=self.session,
                                    task=task,
                                    log_file=self.log_file,
                                    table_name="RotationsInfo"
                                )
                                continue

                            flat = [str(cell).strip().upper() for row in values for cell in row if cell is not None]
                            if flat == ["NULL"]:
                                # log_to_file(self.log_file, f"⚪ [Task {name} {task.source_page_name}] содержит 'NULL'. Пропуск.")
                                task.update_after_upload(False)
                                update_task_update_fields(
                                    session=self.session,
                                    task=task,
                                    log_file=self.log_file,
                                    table_name="RotationsInfo"
                                )
                                continue

                            log_to_file(self.log_file, f"📦 [Task {name}] — {len(values)} строк (🔄 новые данные)")
                            sorted_tasks.append(task)
                            all_values.extend(values)
                            all_values.append([""] * 26)

                        except Exception as e:
                            log_to_file(self.log_file, f"❌ Ошибка при обработке задачи '{name}': {e}")

                    if not sorted_tasks:
                        log_to_file(self.log_file, f"⚪ Нет задач с данными для вставки. Пропуск смены {page_name}.")
                        continue

                    if all_values[-1] == [""] * 26:
                        all_values.pop()

                    if len(all_values) < 100:
                        padding = 100 - len(all_values)
                        all_values.extend([[""] * 26 for _ in range(padding)])
                        # log_to_file(self.log_file, "")
                        # log_to_file(self.log_file, f"➕ Добавлены {padding} пустых строк до 100.")
                    elif len(all_values) > 100:
                        all_values = all_values[:100]
                        # log_to_file(self.log_file, "")
                        # log_to_file(self.log_file, "⚠️ Обрезано до 100 строк.")

                    reference_task = sorted_tasks[0]
                    spreadsheet_id = reference_task.target_doc_id
                    target_page_area = reference_task.target_page_area
                    insert_range = f"{page_name}!{target_page_area}"

                    # log_to_file(self.log_file, f"📤 Вставка итогового блока из {len(all_values)} строк в диапазон {insert_range}.")

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
                            log_file=self.log_file,
                            session=self.session
                        )
                    except Exception as e:
                        success, error = False, str(e)
                        log_to_file(self.log_file, f"❌ Ошибка batch_update: {error}")

                    try:
                        format_sheet(
                            service=self.service,
                            spreadsheet_id=spreadsheet_id,
                            sheet_title=page_name,
                            values=all_values,
                            token_name=self.token_name,
                            update_group=reference_task.update_group,
                            log_file=self.log_file,
                            session=self.session
                        )
                    except Exception as e:
                        log_to_file(self.log_file, f"⚠️ Ошибка при форматировании листа: {e}")

                    for task in sorted_tasks:
                        try:
                            task.update_after_upload(success)
                            update_task_update_fields(
                                session=self.session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                        except Exception as e:
                            log_to_file(self.log_file, f"⚠️ Ошибка при обновлении task {task.name_of_process}: {e}")

                    # if success:
                    #     log_to_file(self.log_file, f"✅ Вставка смены {page_name} завершена успешно ({len(sorted_tasks)} задач).\n")
                    # else:
                    #     log_to_file(self.log_file, f"❌ Ошибка при вставке смены {page_name}: {error}\n")

                except Exception as e:
                    log_to_file(self.log_file, f"❌ Критическая ошибка при обработке страницы {page_name}: {e}")

        except Exception as e:
            log_to_file(self.log_file, f"❌ Ошибка при обновлении update_main: {e}")


##############################################################################################
# Импорт Shuffle в ротации
##############################################################################################

    def import_shuffle_data(self, tasks):
        log_section("📥 Импорт данных для update_shuffle", self.log_file)

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

                log_to_file(self.log_file, f"📄 Документ: {page_tasks[0].name_of_process}, страница: {page_name}\n")

                try:
                    raw = batch_get(
                        service=self.service,
                        spreadsheet_id=spreadsheet_id,
                        ranges=[f"{page_name}!D1:AC200"],
                        scan_group=update_group,
                        log_file=self.log_file,
                        token_name=self.token_name,
                        session=self.session  # важно!
                    )
                    sheet_values = list(raw.values())[0] if raw else []

                    # Поиск строки с "shift:"
                    shift_row_index = None
                    for idx, row in enumerate(sheet_values):
                        if row and isinstance(row[0], str) and "shift:" in row[0].lower():
                            log_to_file(self.log_file, f"row number for shift {page_tasks[0].name_of_process} is: {row}")
                            shift_row_index = idx + 1  # +1, т.к. индексация с 0
                            break

                    if shift_row_index is None:
                        log_to_file(self.log_file, f"\n❌ Строка с 'shift:' не найдена. Пропуск страницы {page_name}.")
                        for task in page_tasks:
                            task.update_after_upload(False)
                            update_task_update_fields(
                                session=self.session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                        continue

                    all_values = []
                    tasks_with_data = []

                    for task in page_tasks:
                        if not task.values_json or not isinstance(task.values_json, list):
                            # log_to_file(self.log_file, f"⚪ [Task {task.name_of_process} {task.source_page_name}] Нет данных. Пропуск.")
                            task.update_after_upload(False)
                            update_task_update_fields(
                                session=self.session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                            continue
                        
                        # log_to_file(self.log_file, f"📦 [Task {task.name_of_process}] — {len(task.values_json)} строк (🔄 новые данные)")

                        flat = [str(cell).strip().upper() for row in task.values_json for cell in row if cell is not None]
                        if flat == ["NULL"]:
                            # log_to_file(self.log_file, f"⚪ [Task {task.name_of_process} {task.source_page_name}] Содержит 'NULL'. Пропуск.")
                            task.update_after_upload(False)
                            update_task_update_fields(
                                session=self.session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                            continue

                        all_values.extend(task.values_json)
                        tasks_with_data.append(task)

                    if not tasks_with_data:
                        # log_to_file(self.log_file, f"⚪ Нет допустимых данных на странице {page_name}. Пропуск.")
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
                        log_file=self.log_file,
                        session=self.session  # ← добавлено!
                    )

                    for task in page_tasks:
                        if task in tasks_with_data:
                            task.update_after_upload(success)
                            update_task_update_fields(
                                session=self.session,
                                task=task,
                                log_file=self.log_file,
                                table_name="RotationsInfo"
                            )
                    # if success:
                    #     log_to_file(self.log_file, f"✅ Успешная вставка данных страницы {page_name}.")
                    # else:
                    #     log_to_file(self.log_file, f"❌ Ошибка вставки страницы {page_name}: {error}")

                    # log_separator(self.log_file)

                except Exception as e:
                    log_to_file(self.log_file, f"❌ Критическая ошибка страницы {page_name}: {e}")
                    for task in page_tasks:
                        task.update_after_upload(False)
                        update_task_update_fields(
                            session=self.session,
                            task=task,
                            log_file=self.log_file,
                            table_name="RotationsInfo"
                        )