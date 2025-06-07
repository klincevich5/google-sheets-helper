# scanners/sheetsinfo_scanner.py

import time
from datetime import datetime, timedelta
from collections import defaultdict

from tg_bot.utils.settings_access import is_scanner_enabled
from core.data import load_sheetsinfo_tasks
from utils.logger import log_to_file, log_separator, log_section
from utils.floor_resolver import get_floor_by_table_name
from database.session import SessionLocal

from database.db_models import MistakeStorage, FeedbackStorage,  ScheduleOT, DealerMonthlyStatus
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
    def log_tasks_by_type(self, tasks, label):
        log_to_file(self.log_file, f"🔼 {label}: {len(tasks)}")
        for task in tasks:
            log_to_file(self.log_file, f"   • {task.name_of_process} ({task.update_group})")

    def update_phase(self, session):
        log_section("🔼 Фаза обновления", self.log_file)

        try:
            # --- Категоризация задач ---
            tasks_to_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group not in {
                "update_mistakes_in_db", "feedback_status_update", "update_schedule_OT", "update_qa_list_db"}]
            mistakes_to_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group == "update_mistakes_in_db"]
            feedback_to_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group == "feedback_status_update"]
            schedule_OT_to_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group == "update_schedule_OT"]
            qa_list_update = [t for t in self.tasks if t.values_json and t.changed and t.update_group == "update_qa_list_db"]

            # --- Логгирование категорий ---
            self.log_tasks_by_type(tasks_to_update, "Задач для обновления")
            self.log_tasks_by_type(mistakes_to_update, "Ошибок для обновления")
            self.log_tasks_by_type(feedback_to_update, "Фидбеков для обновления")
            self.log_tasks_by_type(schedule_OT_to_update, "Schedule OT для обновления")
            self.log_tasks_by_type(qa_list_update, "QA List для обновления")

            # --- Обычные задачи ---
            log_to_file(self.log_file, f"🔼 Обновление обычных задач: {len(tasks_to_update)}")
            if tasks_to_update:
                try:
                    self.import_tasks_to_update(tasks_to_update, session)
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка при обновлении задач: {e}")
                time.sleep(3)

            # --- Ошибки ---
            log_to_file(self.log_file, f"🔼 Обновление ошибок: {len(mistakes_to_update)}")
            if mistakes_to_update:
                try:
                    self.import_mistakes_to_update(mistakes_to_update, session)
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка при обновлении ошибок: {e}")
                time.sleep(3)

            # --- Фидбеки ---
            log_to_file(self.log_file, f"🔼 Обновление фидбеков: {len(feedback_to_update)}")
            if feedback_to_update:
                try:
                    self.import_feedbacks_to_update(feedback_to_update, self.service, session)
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка при обновлении фидбеков: {e}")

            # --- Schedule OT ---
            log_to_file(self.log_file, f"🔼 Обновление графиков OT: {len(schedule_OT_to_update)}")
            if schedule_OT_to_update:
                try:
                    self.import_schedule_OT_to_update(schedule_OT_to_update, session)
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка при обновлении schedule OT: {e}")
                time.sleep(3)

            # --- QA List ---
            log_to_file(self.log_file, f"🔼 Обновление QA List: {len(qa_list_update)}")
            if qa_list_update:
                try:
                    self.import_qa_list_to_update(qa_list_update, session)
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка при обновлении QA List: {e}")
                time.sleep(3)

            # --- Статистика по задачам ---
            log_to_file(self.log_file, "🔼 Итоговая статистика по задачам:")
            for task in self.tasks:
                log_to_file(
                    self.log_file,
                    f"⚪ [Task {task.name_of_process} {task.source_page_name}] "
                    f"Отсканировано: {task.scanned} | Обработано: {task.proceed} | "
                    f"Изменено: {task.changed} | Загружено: {task.uploaded}"
                )

            if not (tasks_to_update or mistakes_to_update or feedback_to_update or schedule_OT_to_update or qa_list_update):
                return

        finally:
            log_section("🔼 Обновление завершено.", self.log_file)

##############################################################################################
# Импорт Обычных задач 
##############################################################################################


    def import_tasks_to_update(self, tasks_to_update, session):
        log_to_file(self.log_file, f"🔄 Начало фазы tasks_to_update. Всего задач: {len(tasks_to_update)}")

        tasks_by_group = defaultdict(list)
        for task in tasks_to_update:
            tasks_by_group[task.update_group].append(task)

        for update_group, group_tasks in tasks_by_group.items():
            log_to_file(self.log_file, f"🔄 Обработка группы: {update_group} ({len(group_tasks)} задач)")

            doc_id = group_tasks[0].target_doc_id
            batch_data = self._build_batch_data(group_tasks)

            if not batch_data:
                log_to_file(self.log_file, f"⚠️ Нет валидных данных для batchUpdate группы {update_group}. Пропуск.")
                continue

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
                self._mark_tasks_uploaded(group_tasks, session)
            else:
                log_to_file(self.log_file, f"❌ Ошибка при пакетной отправке: {error}")
                self._fallback_single_upload(group_tasks, doc_id, update_group, session)

    def _build_batch_data(self, tasks):
        batch_data = []
        for task in tasks:
            if not task.values_json:
                continue
            batch_data.append({
                "range": f"{task.target_page_name}!{task.target_page_area}",
                "values": task.values_json
            })
        return batch_data

    def _mark_tasks_uploaded(self, tasks, session):
        for task in tasks:
            task.update_after_upload(success=True)
            update_task_update_fields(
                session=session,
                task=task,
                log_file=self.log_file,
                table_name="SheetsInfo"
            )

    def _fallback_single_upload(self, tasks, doc_id, update_group, session):
        for task in tasks:
            if not task.values_json:
                continue

            single_data = [{
                "range": f"{task.target_page_name}!{task.target_page_area}",
                "values": task.values_json
            }]

            success, error = batch_update(
                service=self.service,
                spreadsheet_id=doc_id,
                batch_data=single_data,
                token_name=self.token_name,
                update_group=update_group,
                log_file=self.log_file
            )

            if success:
                log_to_file(self.log_file, f"✅ [Task {task.name_of_process} {task.source_page_name}] Обновлена поштучно.")
            else:
                log_to_file(self.log_file, f"❌ [Task {task.name_of_process} {task.source_page_name}] Ошибка при обновлении: {error}")

            task.update_after_upload(success=success)
            update_task_update_fields(
                session=session,
                task=task,
                log_file=self.log_file,
                table_name="SheetsInfo"
            )

###############################################################################################
# Импорт Ошибок в БД
###############################################################################################

    def import_mistakes_to_update(self, mistakes_to_update, session):
        success_count = 0
        error_count = 0

        try:
            for task in mistakes_to_update:
                sheet = task.raw_values_json
                if not sheet or not isinstance(sheet, list):
                    log_to_file(self.log_file, f"⚠️ Пустой или некорректный sheet в задаче: {task.name_of_process}")
                    continue

                page_name = task.source_page_name
                floor = get_floor_by_table_name(page_name, FLOORS)
                max_row_in_db = get_max_last_row(session, page_name)

                for row_index, row in enumerate(sheet[1:], start=2):  # пропуск заголовка
                    if row_index <= max_row_in_db or not row or len(row) < 8:
                        continue

                    try:
                        mistake = self._parse_mistake_row(task, row, row_index, floor, page_name)
                        if mistake:
                            session.add(mistake)
                            success_count += 1
                    except Exception as row_err:
                        log_to_file(self.log_file, f"❌ Ошибка при добавлении строки {row_index} из {page_name}: {row_err}. Строка: {row}")
                        error_count += 1

            session.commit()
            log_to_file(self.log_file, f"✅ Импортировано ошибок: {success_count}, ошибок: {error_count}")

        except Exception as task_err:
            session.rollback()
            log_to_file(self.log_file, f"❌ Ошибка при обработке mistakes_to_update: {task_err}")

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
    def parse_date(value):
        try:
            return datetime.strptime(value.strip(), "%d.%m.%Y").date()
        except Exception:
            return None

    def import_feedbacks_to_update(self, feedback_to_update, sheets_service, session):
        success_count = 0
        error_count = 0

        try:
            for task in feedback_to_update:
                sheet = task.raw_values_json
                if not sheet or not isinstance(sheet, list):
                    log_to_file(self.log_file, f"⚠️ Пустой или некорректный sheet в задаче: {task.name_of_process}")
                    continue

                page_name = task.target_page_name
                empty_row_streak = 0

                for row_index, row in enumerate(sheet[1:], start=2):  # пропускаем заголовок
                    if not row or not str(row[0]).isdigit():
                        continue

                    feedback_id = int(row[0])
                    try:
                        parsed = self._parse_feedback_row(row[1:], task)
                        if parsed is None:
                            empty_row_streak += 1
                            if empty_row_streak >= 15:
                                break
                            continue
                        else:
                            empty_row_streak = 0

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
                        log_to_file(
                            self.log_file,
                            f"❌ Ошибка при обработке строки {row_index} из {page_name}: {row_err}. Строка: {row}"
                        )

            session.commit()
            log_to_file(self.log_file, f"✅ Фидбеки успешно импортированы: {success_count}, ошибок: {error_count}")

            # --- Обновление DealerMonthlyStatus ---
            log_to_file(self.log_file, "🔄 Обновление DealerMonthlyStatus по фидбекам...")

            dealers = session.query(DealerMonthlyStatus).filter_by(related_month=task.related_month).all()
            output_data = []

            for dealer in dealers:
                feedbacks = session.query(FeedbackStorage).filter_by(
                    dealer_name=dealer.dealer_name,
                    related_month=dealer.related_month
                ).all()

                if not feedbacks:
                    dealer.feedback_status = False
                    output_data.append([dealer.dealer_name, "❌"])
                    continue

                if any(f.forwarded_feedback is None for f in feedbacks):
                    dealer.feedback_status = False
                    output_data.append([dealer.dealer_name, "❌"])
                else:
                    dealer.feedback_status = True
                    output_data.append([dealer.dealer_name, "✅"])

            session.commit()
            log_to_file(self.log_file, f"✅ Обновлено DealerMonthlyStatus: {len(output_data)} записей")

            # --- Выгрузка в Google Sheets ---
            try:
                sheets_service.write_values(
                    task.target_page_name,
                    task.target_page_area,
                    output_data
                )
                log_to_file(self.log_file, f"📤 Выгрузка статусов в Google Sheet завершена: {task.target_page_name} ({task.target_page_area})")
            except Exception as gs_err:
                log_to_file(self.log_file, f"❌ Ошибка при выгрузке в Google Sheet: {gs_err}")

        except Exception as e:
            session.rollback()
            log_to_file(self.log_file, f"❌ Ошибка при импорте фидбеков: {e}")

        finally:
            log_to_file(self.log_file, "🔄 Завершение фазы feedback_status_update.")

    def _parse_feedback_row(self, data_row, task):
        expected_len = 13
        data = (data_row + [None] * expected_len)[:expected_len]

        essential_fields = data[:8] + data[9:]
        if all((str(f or '').strip() == '') for f in essential_fields):
            return None

        parsed_date = self.parse_date(data[0])
        shift = data[1]  # можно дополнительно нормализовать

        return {
            "related_date": parsed_date,
            "related_shift": shift,
            "floor": data[2],
            "game": data[3],
            "dealer_name": data[4],
            "sm_name": data[5],
            "reason": data[6],
            "total": self.safe_int(data[7]),
            "proof": data[8],
            "explanation_of_the_reason": data[9],
            "action_taken": data[10],
            "forwarded_feedback": data[11],
            "comment_after_forwarding": data[12]
        }

###############################################################################################
# Импорт Schedule OT
################################################################################################

    def import_schedule_OT_to_update(self, tasks, session):
        if not tasks:
            log_to_file(self.log_file, "⚠️ Пустой список задач в import_schedule_OT_to_update")
            return

        try:
            total_new = 0
            total_updated = 0

            for task in tasks:
                values = task.values_json
                if not values or not isinstance(values, list):
                    log_to_file(self.log_file, f"❌ values_json пуст или некорректен в задаче {task.name_of_process}")
                    continue

                related_month = task.related_month.replace(day=1)
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
                            session.add(ScheduleOT(
                                date=shift_date,
                                dealer_name=dealer_name,
                                shift_type=shift,
                                related_month=related_month
                            ))
                            new_entries += 1

                log_to_file(self.log_file, f"📅 ScheduleOT: {task.name_of_process} — новых: {new_entries}, обновлено: {updated_entries}")
                total_new += new_entries
                total_updated += updated_entries

            session.commit()
            log_to_file(self.log_file, f"✅ ScheduleOT итого — новых: {total_new}, обновлено: {total_updated}")

        except Exception as e:
            session.rollback()
            log_to_file(self.log_file, f"❌ Ошибка при импорте schedule OT: {e}")


###############################################################################################
# Импорт QA List в БД
###############################################################################################

    def import_qa_list_to_update(self, qa_list_update, session):
        success_count = 0
        error_count = 0

        try:
            for task in qa_list_update:
                sheet = task.raw_values_json
                if not sheet or not isinstance(sheet, list):
                    log_to_file(self.log_file, f"⚠️ Пустой или некорректный sheet в задаче: {task.name_of_process}")
                    continue

                page_name = task.source_page_name
                for row_index, row in enumerate(sheet[1:], start=2):  # пропуск заголовка
                    if not row or len(row) < 18:
                        log_to_file(self.log_file, f"⚠️ Пропущена неполная строка {row_index} в {page_name}")
                        continue

                    try:
                        qa_item = self._parse_qa_list_row(task, row, row_index, page_name)
                        if qa_item:
                            session.add(qa_item)
                            success_count += 1
                    except Exception as row_err:
                        log_to_file(self.log_file, f"❌ Ошибка при добавлении строки {row_index} из {page_name}: {row_err}. Строка: {row}")
                        error_count += 1

            session.commit()
            log_to_file(self.log_file, f"✅ Импортировано QA записей: {success_count}, ошибок: {error_count}")

        except Exception as task_err:
            session.rollback()
            log_to_file(self.log_file, f"❌ Ошибка при обработке qa_list_update: {task_err}")


    def _parse_qa_list_row(self, task, row, row_index, page_name):
        return QaList(
            dealer_name=row[0],
            VIP=row[1],
            GENERIC=row[2],
            LEGENDZ=row[3],
            GSBJ=row[4],
            TURKISH=row[5],
            TRISTAR=row[6],
            TritonRL=row[7],
            QA_comment=row[8],
            Male=row[9],
            BJ=row[10],
            BC=row[11],
            RL=row[12],
            DT=row[13],
            HSB=row[14],
            swBJ=row[15],
            swBC=row[16],
            swRL=row[17],
            SH=row[18],
            gsDT=row[19]
        )
