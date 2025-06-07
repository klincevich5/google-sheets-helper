# scanners/sheetsinfo_scanner.py

import time
from datetime import datetime, timedelta
from collections import defaultdict

from tg_bot.utils.settings_access import is_scanner_enabled
from core.data import load_sheetsinfo_tasks
from utils.logger import log_to_file, log_separator, log_section
from utils.floor_resolver import get_floor_by_table_name
from database.session import SessionLocal

from database.db_models import MistakeStorage, FeedbackStorage,  ScheduleOT, TrackedTables
from utils.db_orm import get_max_last_row

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
    def __init__(self, token_map, doc_id_map):
        self.token_map = token_map
        self.doc_id_map = doc_id_map
        self.log_file = SHEETSINFO_LOG
        self.tasks = []

    def run(self):
        while True:
            try:
                if not is_scanner_enabled("sheets_scanner"):
                    time.sleep(10)
                    continue

                log_section("▶️ SheetsInfo Активен. Новый цикл сканирования", self.log_file)

                token_name = list(self.token_map.keys())[0]
                token_path = self.token_map[token_name]
                self.token_name = token_name

                with SessionLocal() as session:
                    self.service = load_credentials(token_path, self.log_file)
                    log_to_file(self.log_file, f"🔐 Используется токен: {self.token_name}")

                    for phase_name, method in [
                        ("загрузки задач", lambda: self.load_tasks(session)),
                        ("сканирования", lambda: self.scan_phase(session)),
                        ("обработки", lambda: self.process_phase(session)),
                        ("обновления", lambda: self.update_phase(session))
                    ]:
                        try:
                            method()
                        except Exception as e:
                            log_to_file(self.log_file, f"❌ Ошибка на этапе {phase_name}: {e}")
                            raise

            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка в основном цикле SheetsInfo: {e}")
                time.sleep(10)

            time.sleep(SHEETINFO_INTERVAL)

#############################################################################################
# загрузка задач из БД
#############################################################################################

    def load_tasks(self, session):
        log_section("📥 Загрузка задач из SheetsInfo", self.log_file)

        self.tasks = load_sheetsinfo_tasks(session, self.log_file)

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

    def scan_phase(self, session):
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
                # log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process} {task.source_page_name}] Не удалось сопоставить doc_id. Пропуск.")
                continue
            scan_groups[task.scan_group].append(task)

        for scan_group, group_tasks in scan_groups.items():
            # log_separator(self.log_file)
            # log_to_file(self.log_file, f"📘 Обработка scan_group: {scan_group} ({len(group_tasks)} задач)")

            if not group_tasks:
                # log_to_file(self.log_file, "⚪ В группе нет задач.")
                continue

            doc_id = group_tasks[0].source_doc_id
            unique_sheet_names = set(task.source_page_name for task in group_tasks)
            # log_to_file(self.log_file, f"Уникальные названия листов: {unique_sheet_names}")

            exists_map = {
                sheet_name: check_sheet_exists(self.service, doc_id, sheet_name, self.log_file, self.token_name)
                for sheet_name in unique_sheet_names
            }

            # for sheet_name, exists in exists_map.items():
            #     log_to_file(self.log_file, f"{'✅' if exists else '⚠️'} Лист '{sheet_name}' {'существует' if exists else 'не найден'}.")

            valid_tasks = []
            for task in group_tasks:
                sheet_name = task.source_page_name
                if exists_map.get(sheet_name):
                    # log_to_file(self.log_file, f"➡️ Используем '{sheet_name}' для задачи {task.name_of_process}.")
                    valid_tasks.append(task)
                else:
                    # log_to_file(self.log_file, f"⛔ Пропуск задачи {task.name_of_process}: лист '{sheet_name}' не найден.")
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")

            if not valid_tasks:
                # log_to_file(self.log_file, f"⚪ Все задачи группы {scan_group} отфильтрованы. Пропуск batchGet.")
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
                self.token_name
            )
            if not response_data:
                # log_to_file(self.log_file, "❌ Пустой ответ от batchGet. Все задачи будут отмечены как неудачные.")
                for task in valid_tasks:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")
                continue

            normalized_response = {}
            for k, v in response_data.items():
                clean_key = k.replace("'", "")
                if "!" in clean_key:
                    sheet_name, cells_range = clean_key.split("!", 1)
                    normalized_response[(sheet_name.strip(), cells_range.strip())] = v

            # log_to_file(self.log_file, "")
            # log_to_file(self.log_file, f"📥 Получены диапазоны: {list(normalized_response.keys())}")

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
                    log_to_file(self.log_file, f"✅ [Task {task.name_of_process} {task.source_page_name}] Найден диапазон {sheet_name}!{cells_range}, строк: {len(matched_values)}")
                else:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(session, task, self.log_file, table_name="SheetsInfo")
                    log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process} {task.source_page_name}] Диапазон {expected_sheet}!{task.source_page_area} не найден или пуст.")

        for task in self.tasks:
            log_to_file(
                self.log_file,
                f"⚪ [Task {task.name_of_process} {task.source_page_name}] Отсканировано: {task.scanned} | "
                f"Обработано: {task.proceed} | Изменено: {task.changed} | Загружено: {task.uploaded}"
            )
        log_to_file(self.log_file, "🔍 Фаза сканирования завершена.")

#############################################################################################
# Фаза обработки
#############################################################################################

    def process_phase(self, session):
        log_section("🛠️ Фаза обработки", self.log_file)

        if not self.tasks:
            log_to_file(self.log_file, "⚪ Нет задач для обработки.")
            return

        for task in self.tasks:
            if task.scanned == 0:
                continue

            try:
                # Обработка сырых значений
                try:
                    task.process_raw_value()
                except Exception as e:
                    log_to_file(self.log_file, f"❌ [Task {task.name_of_process} {task.source_page_name}] Ошибка в process_raw_value: {e}")
                    continue

                # Проверка изменений
                try:
                    task.check_for_update()
                except Exception as e:
                    log_to_file(self.log_file, f"❌ [Task {task.name_of_process} {task.source_page_name}] Ошибка в check_for_update: {e}")
                    continue

                # Обновление в БД, если данные изменились
                if task.changed:
                    try:
                        update_task_process_fields(session, task, self.log_file, table_name="SheetsInfo")
                    except Exception as e:
                        log_to_file(self.log_file, f"❌ Ошибка при сохранении изменений в БД: {e}")

            except Exception as e:
                log_to_file(self.log_file, f"❌ [Task {task.name_of_process} {task.source_page_name}] Неизвестная ошибка при обработке: {e}")

        # Итоговый отчёт
        for task in self.tasks:
            log_to_file(
                self.log_file,
                f"⚪ [Task {task.name_of_process} {task.source_page_name}] Отсканировано: {task.scanned} | "
                f"Обработано: {task.proceed} | Изменено: {task.changed} | Загружено: {task.uploaded}"
            )

#############################################################################################
# Фаза обновления
#############################################################################################

    def update_phase(self, session):
        log_section("🔼 Фаза обновления", self.log_file)

        # --- Категоризация задач ---
        tasks_to_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group not in {"update_mistakes_in_db", "feedback_status_update", "update_schedule_OT"}]
        mistakes_to_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group == "update_mistakes_in_db"]
        feedback_to_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group == "feedback_status_update"]
        schedule_OT_to_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group == "update_schedule_OT"]

        log_to_file(self.log_file, f"🔼 Задач для обновления: {len(tasks_to_update)}")
        for task in tasks_to_update:
            log_to_file(self.log_file, f"   • {task.name_of_process} ({task.update_group})\n")
        log_to_file(self.log_file, f"🔼 Ошибок для обновления: {len(mistakes_to_update)}")
        for task in mistakes_to_update:
            log_to_file(self.log_file, f"   • {task.name_of_process} ({task.update_group})\n")
        log_to_file(self.log_file, f"🔼 Фидбеков для обновления: {len(feedback_to_update)}")
        for task in feedback_to_update:
            log_to_file(self.log_file, f"   • {task.name_of_process} ({task.update_group})\n")
        log_to_file(self.log_file, f"🔼 Schedule OT для обновления: {len(schedule_OT_to_update)}")
        for task in schedule_OT_to_update:
            log_to_file(self.log_file, f"   • {task.name_of_process} ({task.update_group})\n")

        # # --- Обычные задачи ---
        log_to_file(self.log_file, f"🔼 Обновление обычных задач: {len(tasks_to_update)}")
        if tasks_to_update:
            try:
                self.import_tasks_to_update(tasks_to_update, session)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении задач: {e}")
            time.sleep(3)

        
        # --- Ошибки (MistakeStorage) ---
        log_to_file(self.log_file, f"🔼 Обновление ошибок: {len(mistakes_to_update)}")
        if mistakes_to_update:
            try:
                self.import_mistakes_to_update(mistakes_to_update, session)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении ошибок: {e}")
            time.sleep(3)

        # --- Фидбеки (FeedbackStorage) ---
        log_to_file(self.log_file, f"🔼 Обновление фидбеков: {len(feedback_to_update)}")
        if feedback_to_update:
            try:
                self.import_feedbacks_to_update(feedback_to_update, self.service, session)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении фидбеков: {e}")

        # --- Schedule GP ---
        log_to_file(self.log_file, f"🔼 Обновление графиков GP: {len(feedback_to_update)}")
        if schedule_OT_to_update:
            try:
                self.import_schedule_OT_to_update(schedule_OT_to_update, session)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении schedule OT: {e}")
            time.sleep(3)

        # --- Статистика по задачам ---
        log_to_file(self.log_file, "🔼 Итоговая статистика по задачам:")
        for task in self.tasks:
            log_to_file(
                self.log_file,
                f"⚪ [Task {task.name_of_process} {task.source_page_name}] Отсканировано: {task.scanned} | "
                f"Обработано: {task.proceed} | Изменено: {task.changed} | Загружено: {task.uploaded}"
            )

        if not (tasks_to_update or mistakes_to_update or feedback_to_update):
            # log_to_file(self.log_file, "⚪ Нет задач для обновления.")
            return

        log_section("🔼 Обновление завершено.", self.log_file)

##############################################################################################
# Импорт Обычных задач 
##############################################################################################

    def import_tasks_to_update(self, tasks_to_update, session):
        log_to_file(self.log_file, f"🔄 Начало фазы tasks_to_update. Всего задач: {len(tasks_to_update)}")

        # Группировка задач по update_group
        tasks_by_group = defaultdict(list)
        for task in tasks_to_update:
            tasks_by_group[task.update_group].append(task)

        for update_group, group_tasks in tasks_by_group.items():
            log_to_file(self.log_file, f"🔄 Обработка группы: {update_group} ({len(group_tasks)} задач)")

            doc_id = group_tasks[0].target_doc_id
            batch_data = []

            for task in group_tasks:
                if not task.values_json:
                    # log_to_file(self.log_file, f"⚪ [Task {task.name_of_process} {task.source_page_name}] Нет данных, пропуск.")
                    continue

                batch_data.append({
                    "range": f"{task.target_page_name}!{task.target_page_area}",
                    "values": task.values_json
                })

            if not batch_data:
                # log_to_file(self.log_file, f"⚪ Нет валидных данных для batchUpdate группы {update_group}.")
                continue
            # for data in batch_data:
            #     print(f"   • {data['range']} ({len(data['values'])} строк)")

            # Пакетная отправка
            success, error = batch_update(
                service=self.service,
                spreadsheet_id=doc_id,
                batch_data=batch_data,
                token_name=self.token_name,
                update_group=update_group,
                log_file=self.log_file
            )

            if success:
                log_to_file(self.log_file, f"✅ Пакетное обновление успешно для группы {update_group}")
                for task in group_tasks:
                    task.update_after_upload(success=True)
                    update_task_update_fields(
                        session=session,
                        task=task,
                        log_file=self.log_file,
                        table_name="SheetsInfo"
                    )
            else:
                log_to_file(self.log_file, f"❌ Ошибка при пакетной отправке: {error}")
                log_to_file(self.log_file, "🔁 Переход к одиночной отправке задач")

                for task in group_tasks:
                    if not task.values_json:
                        continue

                    single_data = [{
                        "range": f"{task.target_page_name}!{task.target_page_area}",
                        "values": task.values_json
                    }]

                    single_success, single_error = batch_update(
                        service=self.service,
                        spreadsheet_id=doc_id,
                        batch_data=single_data,
                        token_name=self.token_name,
                        update_group=update_group,
                        log_file=self.log_file
                    )

                    if single_success:
                        log_to_file(self.log_file, f"✅ [Task {task.name_of_process} {task.source_page_name}] Обновлена поштучно.")
                    else:
                        log_to_file(self.log_file, f"❌ [Task {task.name_of_process} {task.source_page_name}] Ошибка при обновлении: {single_error}")

                    task.update_after_upload(success=single_success)
                    update_task_update_fields(
                        session=session,
                        task=task,
                        log_file=self.log_file,
                        table_name="SheetsInfo"
                    )

###############################################################################################
# Импорт Ошибок в БД
###############################################################################################


    @staticmethod
    def parse_date(value):
        try:
            return datetime.strptime(value.strip(), "%d.%m.%Y").date()
        except Exception:
            return None

    @staticmethod
    def parse_time(value):
        try:
            return datetime.strptime(value.strip(), "%H:%M").time()
        except Exception:
            return None

    @staticmethod
    def parse_cancel(value):
        return 1 if str(value).strip().lower() == "cancel" else 0

    @staticmethod
    def determine_shift(hour: int) -> str:
        if 9 <= hour < 21:
            return "DAY"
        return "NIGHT"

    def import_mistakes_to_update(self, mistakes_to_update, session):

        try:
            for task in mistakes_to_update:
                sheet = task.raw_values_json
                page_name = task.source_page_name
                floor = get_floor_by_table_name(page_name, FLOORS)
                max_row_in_db = get_max_last_row(session, page_name)

                for row_index, row in enumerate(sheet[1:], start=2):  # пропуск заголовка
                    if row_index <= max_row_in_db:
                        continue

                    if not row or len(row) < 8:
                        continue

                    try:
                        date = self.parse_date(row[0])
                        time_ = self.parse_time(row[1])
                        shift = self.determine_shift(time_.hour) if time_ else None

                        mistake = MistakeStorage(
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
                        session.add(mistake)

                    except Exception as row_err:
                        log_to_file(self.log_file, f"❌ Ошибка при добавлении строки {row_index} из {page_name}: {row_err}. Строка: {row}")
                        continue

            session.commit()
            log_to_file(self.log_file, "✅ Все ошибки успешно импортированы.")

        except Exception as task_err:
            session.rollback()
            log_to_file(self.log_file, f"❌ Ошибка при обработке mistakes_to_update: {task_err}")


################################################################################################
# Импорт статуса фидбеков
################################################################################################

    # def update_gp_statuses_in_sheet(self, sheets_service, session):
    #     sheet_id = None
    #     target_range = None

    #     for task in self.tasks:
    #         if task.update_group == "feedback_status_update":
    #             sheet_id = task.target_doc_id
    #             target_range = f"{task.target_page_name}!{task.target_page_area}"
    #             break

    #     if not sheet_id or not target_range:
    #         # log_to_file(self.log_file, "⚠️ Не найден лист для обновления статусов GP.")
    #         return []

    #     try:
    #         records = session.query(
    #             FeedbackStorage.gp_name_surname,
    #             FeedbackStorage.reason,
    #             FeedbackStorage.forwarded_feedback
    #         ).filter(FeedbackStorage.gp_name_surname.isnot(None)).all()

    #         gp_status = {}
    #         for name, reason, forwarded in records:
    #             if reason and reason.strip():
    #                 gp_status[name] = "✅" if forwarded and forwarded.strip() else "❌"
    #                 # log_to_file(self.log_file, f"✅ Статус GP: {name} - {gp_status[name]}")
    #             else:
    #                 gp_status[name] = "❓"
    #                 # log_to_file(self.log_file, f"❓ Статус GP: {name} - {gp_status[name]}")

    #         result = sheets_service.spreadsheets().values().get(
    #             spreadsheetId=sheet_id,
    #             range=self.doc_id_map.get(sheet_id, target_range),
    #         ).execute()

    #         sheet_names = []
    #         for row in result.get("values", []):
    #             if isinstance(row, list) and row and isinstance(row[0], str) and row[0].strip():
    #                 sheet_names.append(row[0].strip())

    #         # --- Сохраняем статусы в FeedbackStatus ---
    #         for name in sheet_names:
    #             status = gp_status.get(name, "✅")  # Теперь по умолчанию ✅
    #             existing = session.query(FeedbackStatus).filter_by(name_surname=name).first()
    #             if existing:
    #                 existing.status = status
    #             else:
    #                 new_status = FeedbackStatus(name_surname=name, status=status)
    #                 session.add(new_status)
    #         session.commit()

    #         output = [[name, gp_status.get(name, "✅")] for name in sheet_names]  # По умолчанию ✅
    #         # for name in gp_status:
    #         #     print(f"GP: {name} - {gp_status[name]}")
    #         # log_to_file(self.log_file, f"✅ Статусы GP обновлены: {len(output)}")

    #         sheets_service.spreadsheets().values().update(
    #             spreadsheetId=sheet_id,
    #             range=target_range,
    #             valueInputOption="RAW",
    #             body={"values": output}
    #         ).execute()

    #         return output

    #     except Exception as e:
    #         session.rollback()
    #         log_to_file(self.log_file, f"❌ Ошибка при обновлении GP статусов: {e}")
    #         return []

    @staticmethod
    def safe_int(value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def import_feedbacks_to_update(self, feedback_to_update, sheets_service, session):

        try:
            for task in feedback_to_update:
                sheet = task.raw_values_json
                page_name = task.target_page_name
                empty_row_streak = 0

                for row_index, row in enumerate(sheet[1:], start=2):
                    if not row or not str(row[0]).isdigit():
                        continue

                    feedback_id = int(row[0])
                    data = row[1:]

                    expected_len = 13
                    data = (data + [None] * expected_len)[:expected_len]

                    essential_fields = data[:8] + data[9:]
                    if all((str(f or '').strip() == '') for f in essential_fields):
                        empty_row_streak += 1
                    else:
                        empty_row_streak = 0

                    if empty_row_streak >= 15:
                        break

                    try:
                        parsed_date = self.parse_date(data[0])
                        shift = data[1]  # You can optionally standardize shift values here

                        existing = session.query(FeedbackStorage).filter_by(id=feedback_id).first()
                        if existing:
                            existing.related_month = task.related_month
                            for attr, val in zip([
                                "related_date", "related_shift", "floor", "game", "dealer_name",
                                "sm_name", "reason", "total", "proof",
                                "explanation_of_the_reason", "action_taken",
                                "forwarded_feedback", "comment_after_forwarding"
                            ], [parsed_date, shift] + data[2:]):
                                if attr == "total":
                                    val = self.safe_int(val)
                                setattr(existing, attr, val)
                        else:
                            new_feedback = FeedbackStorage(
                                id=feedback_id,
                                related_month=task.related_month,
                                related_date=parsed_date,
                                related_shift=shift,
                                floor=data[2],
                                game=data[3],
                                dealer_name=data[4],
                                sm_name=data[5],
                                reason=data[6],
                                total=self.safe_int(data[7]),
                                proof=data[8],
                                explanation_of_the_reason=data[9],
                                action_taken=data[10],
                                forwarded_feedback=data[11],
                                comment_after_forwarding=data[12]
                            )
                            session.add(new_feedback)

                    except Exception as row_err:
                        log_to_file(
                            self.log_file,
                            f"❌ Ошибка при обработке строки {row_index} из {page_name}: {row_err}. Строка: {row}"
                        )

            session.commit()
            log_to_file(self.log_file, "✅ Все фидбеки успешно импортированы.")

        except Exception as e:
            session.rollback()
            log_to_file(self.log_file, f"❌ Ошибка при импорте фидбеков: {e}")

        finally:
            # self.update_gp_statuses_in_sheet(sheets_service, session)
            log_to_file(self.log_file, "🔄 Завершение фазы feedback_status_update.")

###############################################################################################
# Импорт Schedule OT
################################################################################################

    def import_schedule_OT_to_update(self, tasks, session):
        if not tasks:
            log_to_file(self.log_file, "⚠️ Пустой список задач в import_schedule_OT_to_update")
            return

        task = tasks[0]
        values = task.values_json
        related_month = task.related_month.replace(day=1)

        if not values or not isinstance(values, list):
            log_to_file(self.log_file, "❌ values_json пуст или некорректен (ожидается список)")
            return

        try:
            # Загружаем существующие записи
            existing_records = session.query(ScheduleOT).filter_by(related_month=related_month).all()
            existing_lookup = {
                (rec.dealer_name.strip(), rec.date): rec
                for rec in existing_records if rec.dealer_name
            }

            new_entries = 0
            updated_entries = 0

            for row in values:
                if not row or not isinstance(row, list) or len(row) < 2:
                    continue

                dealer_name = row[0]
                if not dealer_name or not isinstance(dealer_name, str):
                    continue

                dealer_name = dealer_name.strip()

                for col_idx, shift in enumerate(row[1:], start=1):  # День месяца
                    shift = (shift or "").strip()
                    if shift in {"", "-", "/"}:
                        continue

                    try:
                        shift_date = related_month.replace(day=col_idx)
                    except ValueError:
                        continue  # Например, 31 февраля

                    key = (dealer_name, shift_date)

                    if key in existing_lookup:
                        record = existing_lookup[key]
                        if record.shift_type != shift:
                            record.shift_type = shift
                            updated_entries += 1
                    else:
                        new_record = ScheduleOT(
                            date=shift_date,
                            dealer_name=dealer_name,
                            shift_type=shift,
                            related_month=related_month
                        )
                        session.add(new_record)
                        new_entries += 1

            session.commit()

            log_to_file(
                self.log_file,
                f"✅ ScheduleOT: {related_month.strftime('%B %Y')} — "
                f"{new_entries} новых, {updated_entries} обновлено."
            )

        except Exception as e:
            session.rollback()
            log_to_file(self.log_file, f"❌ Ошибка при импорте schedule OT: {e}")
