# scanners/sheetsinfo_scanner.py

import time
import sqlite3
from collections import defaultdict

from bot.settings_access import is_scanner_enabled
from core.config import SHEETSINFO_LOG, SHEETSINFO_TOKEN, SHEETINFO_INTERVAL, DB_PATH
from core.data import load_sheetsinfo_tasks
from database.database import insert_usage
from utils.logger import log_to_file, log_separator, log_section
from core.token_manager import TokenManager
from utils.utils import (
    load_credentials,
    check_sheet_exists,
    update_task_scan_fields,
    update_task_process_fields,
    update_task_update_fields,
    batch_get,
    batch_update,
)

class SheetsInfoScanner:
    def __init__(self, conn, token_map, doc_id_map):
        self.conn = conn
        self.token_map = token_map  # передаётся из main.py
        self.doc_id_map = doc_id_map
        self.log_file = SHEETSINFO_LOG
        self.tasks = []

    def run(self):
        manager = TokenManager(self.token_map)

        while True:
            try:
                
                if not is_scanner_enabled("sheets_scanner"):
                    log_to_file(self.log_file, "⏸ Сканер отключён (sheets_scanner). Ожидание...")
                    time.sleep(10)
                    continue
                
                log_section("▶️ SheetsInfo Активен. Новый цикл сканирования", self.log_file)

                try:
                    # 🔁 Выбор токена каждый цикл
                    self.token_name, token_path = manager.select_best_token(self.log_file)
                    log_to_file(self.log_file, f"🔑 Выбран{self.token_name}")
                    self.service = load_credentials(token_path, self.log_file)
                    log_to_file(self.log_file, f"🔐 Используется токен: {self.token_name}")
                except Exception as e:
                    log_to_file(self.log_file, f"❌ Ошибка при выборе токена: {e}")
                    time.sleep(10)
                    continue

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
                
                log_section(f"🔄 Цикл завершён. Следующее сканирование через {SHEETINFO_INTERVAL} секунд", self.log_file,)
                log_to_file(self.log_file, "")
                log_to_file(self.log_file, "")
                log_to_file(self.log_file, "")
                log_to_file(self.log_file, "")
                log_to_file(self.log_file, "")
                time.sleep(SHEETINFO_INTERVAL)

            except Exception as e:
                log_separator(self.log_file)
                log_to_file(self.log_file, f"❌ Критическая ошибка в основном цикле: {e}")
                time.sleep(10)

#############################################################################################
# загрузка задач из БД
#############################################################################################

    def load_tasks(self):
        # log_section("🧩 📥 Загрузка задач из SheetsInfo", self.log_file)
        self.tasks = load_sheetsinfo_tasks(self.conn, self.log_file)

        if not self.tasks:
            log_section("⚪ Нет задач для загрузки из SheetsInfo.", self.log_file)
            return

        # log_section(f"🔄 Загружено {len(self.tasks)} задач.", self.log_file)
        for task in self.tasks:
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

        # log_to_file(self.log_file, f"🔎 Найдено {len(ready_tasks)} задач, готовых к сканированию:")

        scan_groups = defaultdict(list)
        for task in ready_tasks:
            if not task.assign_doc_ids(self.doc_id_map):
                log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process}] Не удалось сопоставить doc_id. Пропуск.")
                continue
            scan_groups[task.scan_group].append(task)

        for scan_group, group_tasks in scan_groups.items():
            # log_separator(self.log_file)
            # log_to_file(self.log_file, f"📘 Обработка scan_group: {scan_group} ({len(group_tasks)} задач)")

            if not group_tasks:
                log_to_file(self.log_file, "⚪ В группе нет задач.")
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
                    log_to_file(self.log_file, f"⛔ Пропуск задачи {task.name_of_process}: лист '{sheet_name}' не найден.")
                    task.update_after_scan(success=False)
                    update_task_scan_fields(self.conn, task, self.log_file, table_name="SheetsInfo")


            # if not valid_tasks:
            #     log_to_file(self.log_file, f"⚪ Все задачи группы {scan_group} отфильтрованы. Пропуск batchGet.")
            #     continue

            range_to_tasks = defaultdict(list)
            for task in valid_tasks:
                range_str = f"{task.source_page_name}!{task.source_page_area}"
                range_to_tasks[range_str].append(task)

            ranges = list(range_to_tasks.keys())

            # log_to_file(self.log_file, "")
            # log_to_file(self.log_file, f"📤 Отправка batchGet на документ {task.source_table_type} с {len(ranges)} уникальными диапазонами:")
            
            # for r in ranges:
            #     log_to_file(self.log_file, f"   • {r}")

            response_data = batch_get(self.service, 
                                      doc_id, 
                                      ranges, 
                                      scan_group, 
                                      self.log_file,
                                      self.token_name)
            if not response_data:
                # log_to_file(self.log_file, "❌ Пустой ответ от batchGet. Все задачи будут отмечены как неудачные.")
                for task in valid_tasks:
                    task.update_after_scan(success=False) #Обновление в Классе
                    update_task_scan_fields(self.conn, task, self.log_file, table_name="SheetsInfo") #Обновление в БД
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
                    task.raw_values_json = matched_values #Сохранение необработанных данных в raw_values_json
                    task.update_after_scan(success=True) #Обновление в Классе
                    update_task_scan_fields(self.conn, task, self.log_file, table_name="SheetsInfo") #Обновление в БД
                    # log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Найден диапазон {sheet_name}!{cells_range}, строк: {len(matched_values)}")
                else:
                    task.update_after_scan(success=False)
                    update_task_scan_fields(self.conn, task, self.log_file, table_name="SheetsInfo")
                    # log_to_file(self.log_file, f"⚠️ [Task {task.name_of_process}] Диапазон {expected_sheet}!{task.source_page_area} не найден или пуст.")
        for task in self.tasks:
            log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Отсканировано: {task.scanned} | Обработано: {task.proceed} | Изменено: {task.changed} | Загружено: {task.uploaded}")

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
                continue
            try:
                # log_to_file(self.log_file, f"🔧 Обработка задачи [Task {task.name_of_process}]...")

                try:
                    task.process_raw_value() # Обработка данных и сохранение в values_json
                    
                    # log_to_file(self.log_file, f"📦 [Task {task.name_of_process}] После обработки: {len(task.values_json)} строк.")
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
                        # log_to_file(self.log_file, "🔁 Изменения обнаружены — задача будет обновлена.")
                        update_task_process_fields(self.conn, task, self.log_file, table_name="SheetsInfo")
                        # log_to_file(self.log_file, f"✅ [Task {task.name_of_process}] Успешно обработана и записана в БД.\n")
                    # else:
                    #     log_to_file(self.log_file, "⚪ Изменений нет — обновление не требуется.\n")
                except Exception as e:
                    log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка в check_for_update: {e}")
                    continue

            except Exception as e:
                log_to_file(self.log_file, f"❌ [Task {task.name_of_process}] Ошибка обработки: {e}")


        for task in self.tasks:
            log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Отсканировано: {task.scanned} | Обработано: {task.proceed} | Изменено: {task.changed} | Загружено: {task.uploaded}")

#############################################################################################
# Фаза обновления
#############################################################################################

    def update_phase(self):
        log_section("🔼 Фаза обновления", self.log_file)
        # return  # Закомментировано для тестирования

        
        has_tasks_changes = any(task.changed for task in self.tasks if task.update_group != "update_mistakes_in_db" and task.update_group != "feedback_status_update")
        log_to_file(self.log_file, f"🔼 Обнаружены изменения в задачах: {has_tasks_changes}")
        tasks_to_update = [task for task in self.tasks if task.values_json and task.update_group != "update_mistakes_in_db" and task.update_group != "feedback_status_update" and has_tasks_changes]
        log_to_file(self.log_file, f"🔼 Задач для обновления: {len(tasks_to_update)}")


        has_mistakes_changes = any(task.changed for task in self.tasks if task.update_group == "update_mistakes_in_db")
        log_to_file(self.log_file, f"🔼 Обнаружены изменения в ошибках: {has_mistakes_changes}")
        mistakes_to_update = [task for task in self.tasks if task.values_json and task.update_group == "update_mistakes_in_db" and has_mistakes_changes]
        log_to_file(self.log_file, f"🔼 Ошибок для обновления: {len(mistakes_to_update)}")


        has_feedback_changes = any(task.changed for task in self.tasks if task.update_group == "feedback_status_update")
        log_to_file(self.log_file, f"🔼 Обнаружены изменения в фидбеках: {has_feedback_changes}")
        feedback_to_update = [task for task in self.tasks if task.values_json and task.update_group == "feedback_status_update" and has_feedback_changes]
        log_to_file(self.log_file, f"🔼 Фидбеков для обновления: {len(feedback_to_update)}")

        if tasks_to_update:
            try:
                self.import_tasks_to_update(tasks_to_update)
                # log_section("🔼 Обновление tasks_to_update завершено.", self.log_file)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении tasks_to_update: {e}")

        time.sleep(5)

        if mistakes_to_update:
            try:
                self.import_mistakes_to_update(mistakes_to_update)
                # log_section("🔼 Обновление mistakes_to_update завершено.", self.log_file)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении mistakes_to_update: {e}")
                
        time.sleep(5)

        if feedback_to_update:
            # Получаем ID таблицы для обновления фидбеков
            try:
                self.import_feedbacks_to_update(feedback_to_update, self.service)
                # log_section("🔼 Обновление feedback_to_update завершено.", self.log_file)
            except Exception as e:
                log_to_file(self.log_file, f"❌ Ошибка при обновлении feedback_to_update: {e}")

        for task in self.tasks:
            log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Отсканировано: {task.scanned} | Обработано: {task.proceed} | Изменено: {task.changed} | Загружено: {task.uploaded}")

        if not tasks_to_update and not mistakes_to_update and not feedback_to_update:
            # log_to_file(self.log_file, "⚪ Нет задач для обновления. Пропуск.")
            return
        else:
            log_section("🔼 Обновление завершено.", self.log_file)

##############################################################################################
# Импорт Обычных задач 
##############################################################################################

    def import_tasks_to_update(self, tasks_to_update):

        # log_to_file(self.log_file, f"🔄 Начало фазы tasks_to_update. Задач для выгрузки: {len(tasks_to_update)}.")

        tasks_by_update_group = defaultdict(list)
        for task in tasks_to_update:
            tasks_by_update_group[task.update_group].append(task)

        for update_group, group_tasks in tasks_by_update_group.items():
            # log_section(f"🔄 Обработка группы обновления: {update_group} ({len(group_tasks)} задач).", self.log_file)

            doc_id = group_tasks[0].target_doc_id

            batch_data = []
            for task in group_tasks:
                if not task.values_json:
                    log_to_file(self.log_file, f"⚪ [Task {task.name_of_process}] Нет данных для отправки, пропуск.")
                    continue

                batch_data.append({
                    "range": f"{task.target_page_name}!{task.target_page_area}",
                    "values": task.values_json
                })

            if not batch_data:
                log_to_file(self.log_file, "⚪ Нет данных для batchUpdate в этой группе.")
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
                for task in group_tasks:
                    task.update_after_upload(success=True)
                    update_task_update_fields(
                        conn=self.conn,
                        task=task,
                        log_file=self.log_file,
                        table_name="SheetsInfo"
                    )
                    insert_usage(
                        token=SHEETSINFO_TOKEN,
                        count=1,
                        scan_group=update_group,
                        success=True
                    )
                # log_to_file(self.log_file, f"✅ Успешное обновление группы {update_group} ({len(group_tasks)} задач).")
            else:
                log_to_file(self.log_file, f"❌ Ошибка batchUpdate: {error}")
                log_to_file(self.log_file, "🔄 Переходим на поштучную отправку задач.")

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

                    insert_usage(
                        token=SHEETSINFO_TOKEN,
                        count=1,
                        scan_group=update_group,
                        success=single_success
                    )

                    if single_success:
                        task.update_after_upload(success=True)
                        # log_to_file(self.log_file, f"✅ Успешно обновлена задача [Task {task.name_of_process}] отдельно.")
                        # log_separator(self.log_file)
                        # log_to_file(self.log_file, "" * 100)
                    else:
                        task.update_after_upload(success=False)
                        log_to_file(self.log_file, f"❌ Ошибка обновления [Task {task.name_of_process}] отдельно: {single_error}")
                        log_separator(self.log_file)
                        log_to_file(self.log_file, "" * 100)

                    update_task_update_fields(
                        conn=self.conn,
                        task=task,
                        log_file=self.log_file,
                        table_name="SheetsInfo"
                    )

                    # log_to_file(self.log_file, f"💾 Обновлён values_json и hash для задачи {task.name_of_process}")

###############################################################################################
# Импорт Ошибок в БД
###############################################################################################

    @staticmethod
    def get_floor_by_table_name(table_name: str, floor_map: dict) -> str:
        for floor, tables in floor_map.items():
            if table_name in tables:
                return floor
        return "UNKNOWN"

    @staticmethod
    def get_max_last_row(conn, table_name):
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(last_row) FROM MistakeStorage WHERE table_name = ?", (table_name,))
        result = cursor.fetchone()
        return result[0] if result and result[0] is not None else 0

    def import_mistakes_to_update(self, mistakes_to_update):
        log_to_file(self.log_file, f"🔄 Начало фазы mistakes_to_update. Задач для выгрузки: {len(mistakes_to_update)}.")

        floor_list = {
            "VIP": ["vBJ2", "vBJ3", "gBC1", "vBC3", "vBC4", "vHSB1", "vDT1", "gsRL1", "swBC1", "swRL1"],
            "TURKISH": ["tBJ1", "tBJ2", "tRL1"],
            "GENERIC": ["gBJ1", "gBJ3", "gBJ4", "gBJ5", "gBC2", "gBC3", "gBC4", "gBC5", "gBC6", "gRL1", "gRL2"],
            "GSBJ": ["gsBJ1", "gsBJ2", "gsBJ3", "gsBJ4", "gsBJ5", "gRL3"],
            "LEGENDZ": ["lBJ1", "lBJ2", "lBJ3"]
        }

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        try:
            for task in mistakes_to_update:
                sheet = task.raw_values_json
                page_name = task.source_page_name
                floor = self.get_floor_by_table_name(page_name, floor_list)
                max_row_in_db = self.get_max_last_row(conn, page_name)

                for row_index, row in enumerate(sheet[1:], start=2):  # Пропускаем заголовок
                    if row_index <= max_row_in_db:
                        continue

                    # Защита от пустых строк
                    if not row or len(row) < 8:
                        log_to_file(self.log_file, f"⚠️ Пропущена пустая или неполная строка {row_index} из {page_name}: {row}")
                        continue

                    try:
                        is_cancel = 1 if str(row[5]).strip().lower() == "cancel" else 0

                        cursor.execute("""
                            INSERT INTO MistakeStorage (
                                floor, table_name, date, time, game_id, mistake, type,
                                is_cancel, dealer, sm, last_row
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            floor,
                            page_name,
                            row[0],  # date
                            row[1],  # time
                            row[2],  # game_id
                            row[3],  # mistake
                            row[4],  # type
                            is_cancel,
                            row[6],  # dealer
                            row[7],  # sm
                            row_index
                        ))

                    except Exception as row_err:
                        log_to_file(self.log_file, f"❌ Ошибка при добавлении строки {row_index} из {page_name}: {row_err}. Строка: {row}")
                        continue  # просто пропускаем строку и едем дальше

            conn.commit()
            log_to_file(self.log_file, "✅ Все ошибки успешно импортированы.")

        except Exception as task_err:
            log_to_file(self.log_file, f"❌ Ошибка при обработке mistakes_to_update: {task_err}")

        finally:
            conn.close()
            log_to_file(self.log_file, "🔄 Завершение фазы mistakes_to_update.")

################################################################################################
# Импорт статуса фидбеков
################################################################################################

    def update_gp_statuses_in_sheet(self, sheets_service):

        for task in self.tasks:
            if task.update_group == "feedback_status_update":
                sheet_id = task.target_doc_id
                targer_page_name = task.target_page_name
                target_page_area = task.target_page_area
                range = f"{targer_page_name}!{target_page_area}"
                break
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        try:
            # Получаем все строки с именем и причиной (игнорируем те, у кого пустое имя)
            cursor.execute("""
                SELECT GP_Name_Surname, Reason, Forwarded_Feedback
                FROM FeedbackStorage
                WHERE GP_Name_Surname IS NOT NULL AND TRIM(GP_Name_Surname) != ''
            """)
            rows = cursor.fetchall()

            # Заполняем статус только для тех, кто есть в БД
            gp_status = {}

            for name, reason, forwarded in rows:
                if reason and reason.strip():
                    if forwarded and forwarded.strip():
                        gp_status[name] = "✅"
                    else:
                        gp_status[name] = "❌"

            # Получаем список имен из Google Sheet
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range="Info!A1:A300"
            ).execute()

            sheet_names = [row[0].strip() for row in result.get("values", []) if row and row[0].strip()]

            # Составляем итоговый список со статусами
            output = []
            for name in sheet_names:
                status = gp_status.get(name, "...")
                output.append([name, status])
                # print(f"Имя: {name}, Статус: {status}")

            # Обновляем Google Sheet
            sheets_service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range,
                valueInputOption="RAW",
                body={"values": output}
            ).execute()

            # log_to_file(self.log_file, f"📋 Обновлены статусы GP в Info!A1:B300: {len(output)} записей.")
            return output

        except Exception as e:
            log_to_file(self.log_file, f"❌ Ошибка при обновлении GP статусов: {e}")
            return []

        finally:
            conn.close()

    def import_feedbacks_to_update(self, feedback_to_update, sheets_service):
        # log_to_file(self.log_file, f"🔄 Начало фазы feedback_status_update. Задач для выгрузки: {len(feedback_to_update)}.")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        try:
            for task in feedback_to_update:
                sheet = task.raw_values_json
                page_name = task.target_page_name
                empty_row_streak = 0  # счётчик подряд пустых строк

                for row_index, row in enumerate(sheet[1:], start=2):
                    if not row or not row[0].isdigit():
                        # log_to_file(self.log_file, f"⚠️ Пропущена строка {row_index} из {page_name} (невалидный id): {row}")
                        continue

                    feedback_id = int(row[0])
                    data = row[1:]

                    # Нормализуем длину строки до 13 полей
                    expected_len = 13
                    if len(data) < expected_len:
                        data += [None] * (expected_len - len(data))
                    elif len(data) > expected_len:
                        data = data[:expected_len]

                    # Проверка: все ли поля (кроме Proof, т.е. data[8]) и id — пусты?
                    essential_fields = data[:8] + data[9:]  # исключаем Proof (data[8])
                    if all((str(f or '').strip() == '') for f in essential_fields):
                        empty_row_streak += 1
                    else:
                        empty_row_streak = 0  # сброс счётчика если есть содержимое

                    # Прекращаем обработку, если встретили 5 подряд "пустых" строк
                    if empty_row_streak >= 15:
                        # log_to_file(self.log_file, f"⏹️ Импорт прерван после {empty_row_streak} подряд пустых строк (строка {row_index})")
                        break

                    try:
                        cursor.execute("SELECT id FROM FeedbackStorage WHERE id = ?", (feedback_id,))
                        exists = cursor.fetchone()

                        if exists:
                            cursor.execute("""
                                UPDATE FeedbackStorage SET
                                    Date = ?, Shift = ?, Floor = ?, Game = ?, GP_Name_Surname = ?,
                                    SM_Name_Surname = ?, Reason = ?, Total = ?, Proof = ?,
                                    Explanation_of_the_reason = ?, Action_taken = ?, Forwarded_Feedback = ?, Comment_after_forwarding = ?
                                WHERE id = ?
                            """, (*data, feedback_id))
                            # log_to_file(self.log_file, f"🔄 Обновлён фидбек id={feedback_id} из {page_name}")
                        else:
                            cursor.execute("""
                                INSERT INTO FeedbackStorage (
                                    id, Date, Shift, Floor, Game, GP_Name_Surname,
                                    SM_Name_Surname, Reason, Total, Proof,
                                    Explanation_of_the_reason, Action_taken,
                                    Forwarded_Feedback, Comment_after_forwarding
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (feedback_id, *data))
                            # log_to_file(self.log_file, f"➕ Добавлен фидбек id={feedback_id} из {page_name}")

                    except Exception as row_err:
                        log_to_file(self.log_file, f"❌ Ошибка при обработке строки {row_index} из {page_name}: {row_err}. Строка: {row}")

            conn.commit()
            # log_to_file(self.log_file, "✅ Все фидбеки успешно импортированы.")
        except Exception as e:
            log_to_file(self.log_file, f"❌ Ошибка при импорте фидбеков: {e}")
        finally:
            conn.close()
            # log_to_file(self.log_file, "🔄 Сохранение фидбеков в БД")
            self.update_gp_statuses_in_sheet(sheets_service)
            log_to_file(self.log_file, "🔄 Завершение фазы feedback_status_update.")
