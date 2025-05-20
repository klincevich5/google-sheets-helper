# scanners/monitoring_storage_scanner.py

from sqlalchemy import select
import re
import time
from datetime import datetime, timedelta
from core.config import MONITORING_LOG
from collections import defaultdict
from utils.logger import (
    log_to_file,
    log_separator,
    log_section
)
from utils.utils import (
    batch_get,
    batch_update
)
from database.db_models import (
    MonitoringStorage,
    RotationsInfo,
    FeedbackStorage,
    MistakeStorage,
    SheetsInfo
)
from core.monitoring_storage_model import MonitoringStorageTask

################################################################################################
# Функция для красивого вывода информации о дилере
################################################################################################

def pretty_print_dealer_info(info):
    log_to_file(MONITORING_LOG, f"\n{'=' * 50}")
    log_to_file(MONITORING_LOG, f"👤 Дилер: {info['dealer_name']} {'(' + ', '.join(info['dealer_nicknames']) + ')' if info['dealer_nicknames'] else ''}")
    log_to_file(MONITORING_LOG, f"📅 Дата смены: {info['report_date'].strftime('%Y-%m-%d')}")
    log_to_file(MONITORING_LOG, f"⏰ Смена: {info['shift_type']} ({info['shift_start'].strftime('%H:%M')} – {info['shift_end'].strftime('%H:%M')})")

    # Статусы смены
    log_to_file(MONITORING_LOG, "\n📌 Статусы:")
    log_to_file(MONITORING_LOG, f"  - Назначена: {'✅' if info['is_scheduled'] else '❌'}")
    log_to_file(MONITORING_LOG, f"  - Доп. смена: {'✅' if info['is_additional'] else '❌'}")
    log_to_file(MONITORING_LOG, f"  - Экстра смена: {'✅' if info['is_extra'] else '❌'}")
    log_to_file(MONITORING_LOG, f"  - Больничный: {'✅' if info['is_sickleave'] else '❌'}")
    log_to_file(MONITORING_LOG, f"  - Отпуск: {'✅' if info['is_vacation'] else '❌'}")
    log_to_file(MONITORING_LOG, f"  - Не пришёл: {'✅' if info['is_did_not_come'] else '❌'}")
    log_to_file(MONITORING_LOG, f"  - Ушёл со смены: {'✅' if info['is_left_the_shift'] else '❌'}")

    # Доступные этажи
    log_to_file(MONITORING_LOG, "\n🏢 Доступ к этажам:")
    for floor in ["VIP", "TURKISH", "GENERIC", "GSBJ", "LEGENDZ"]:
        allowed = info['floor_permits'].get(floor, '❓')
        if isinstance(allowed, str) and (allowed.startswith('[FLOOR_PERMITS ERROR]') or allowed.startswith('[FLOOR_PERMITS WARNING]')):
            log_to_file(MONITORING_LOG, f"  - {floor}: {allowed}")
        else:
            log_to_file(MONITORING_LOG, f"  - {floor}: {'✅' if allowed is True else '❌'}")

    # Разрешённые игры (жёстко по списку)
    log_to_file(MONITORING_LOG, "\n🎲 Разрешённые игры:")
    games = ["Male", "BJ", "BC", "RL", "DT", "HSB", "swBJ", "swBC", "swRL", "SH"]
    for game in games:
        allowed = info['game_permits'].get(game, '❓')
        if isinstance(allowed, str) and (allowed.startswith('[GAME_PERMITS ERROR]') or allowed.startswith('[GAME_PERMITS WARNING]') or allowed == 'NOT FOUND'):
            log_to_file(MONITORING_LOG, f"  - {game}: {allowed}")
        elif game == "SH":
            # SH — всегда текст, если None или пусто — выводим '❌', если строка — выводим как есть
            if allowed is None or allowed == '':
                log_to_file(MONITORING_LOG, f"  - {game}: ❌")
            else:
                log_to_file(MONITORING_LOG, f"  - {game}: {allowed}")
        else:
            log_to_file(MONITORING_LOG, f"  - {game}: {'✅' if allowed is True else '❌'}")

    # Ошибки и обратная связь
    log_to_file(MONITORING_LOG, "\n🚨 Ошибки и обратная связь:")
    log_to_file(MONITORING_LOG, f"  - Ошибки: {'✅' if info['has_mistakes'] else '❌'}")
    log_to_file(MONITORING_LOG, f"  - Фидбэки: {'✅' if info['has_feedbacks'] else '❌'}")
    log_to_file(MONITORING_LOG, f"{'=' * 50}\n")

#################################################################################################
# MonitoringStorageScanner
#################################################################################################

class MonitoringStorageScanner:
    def __init__(self, session, monitoring_tokens, doc_id_map):
        self.session = session
        self.doc_id_map = doc_id_map
        self.date = datetime.now().date()
        log_to_file(MONITORING_LOG, f"🌀 Дата: {self.date}")

        # --- Bulk load all needed data into memory for fast access ---
        import json
        from core.config import FLOORS as FLOORS_DICT, ROTATION_ORDER
        self.FLOORS_DICT = FLOORS_DICT
        self.ROTATION_ORDER = ROTATION_ORDER
        related_month = self.date.replace(day=1)

        # 1. Floor lists (permits) — имена без lower()
        self.floor_lists = {}
        for floor in FLOORS_DICT.keys():
            process_name = f"Floor list {floor}"
            sheets_row = self.session.query(SheetsInfo).filter_by(
                name_of_process=process_name,
                related_month=related_month
            ).first()
            names_set = set()
            if sheets_row and sheets_row.values_json:
                values = sheets_row.values_json
                if isinstance(values, str):
                    values = json.loads(values)
                for row in values:
                    if isinstance(row, str):
                        full_entry = row.strip()
                    elif isinstance(row, list) and row and isinstance(row[0], str):
                        full_entry = row[0].strip()
                    else:
                        continue
                    if full_entry:
                        names_set.add(full_entry)
            self.floor_lists[floor] = names_set

        # 2. Game permits (permits by floor) — имена без lower()
        self.game_permits = {}
        for floor in FLOORS_DICT.keys():
            process_name = f"Permits {floor}"
            sheets_row = self.session.query(SheetsInfo).filter_by(
                name_of_process=process_name,
                related_month=related_month
            ).first()
            floor_permits = {}
            if sheets_row and sheets_row.values_json:
                values = sheets_row.values_json
                if isinstance(values, str):
                    values = json.loads(values)
                if values and len(values) >= 3:
                    headers = values[0]
                    for row in values[2:]:
                        if not row or not row[0]:
                            continue
                        name = row[0].strip()
                        permits = {headers[i]: (row[i].strip().title() if headers[i]=="SH" else (row[i].strip().upper()=="TRUE"))
                                   for i in range(1, min(len(headers), len(row)))}
                        floor_permits[name] = permits
            self.game_permits[floor] = floor_permits

        # 3. Mistakes (by (dealer, date) tuple)
        self.mistakes_by_dealer_date = defaultdict(list)
        for entry in self.session.query(MistakeStorage).filter_by(date=self.date).all():
            self.mistakes_by_dealer_date[(entry.dealer, self.date)].append(entry)

        # 4. Feedbacks (by (dealer, date) tuple)
        self.feedbacks_by_dealer_date = defaultdict(list)
        for entry in self.session.query(FeedbackStorage).filter_by(date=self.date).all():
            self.feedbacks_by_dealer_date[(entry.gp_name_surname, self.date)].append(entry)

        # 5. Rotations (by (floor, date) tuple)
        self.rotations_by_floor_date = defaultdict(list)
        for rot in self.session.query(RotationsInfo).filter(
            RotationsInfo.related_month == self.date,
            RotationsInfo.name_of_process.in_(ROTATION_ORDER)
        ).all():
            floor = rot.source_page_name
            values = []
            try:
                values = json.loads(rot.values_json) if rot.values_json else []
            except Exception:
                values = []
            self.rotations_by_floor_date[(floor, self.date)].append(values)

    def run(self):
        log_to_file(MONITORING_LOG, f"🌀 Запуск MonitoringStorageScanner на {self.date}")

        import time as _time
        dealers = self._get_all_dealers()
        for idx, (dealer_name, nicknames) in enumerate(dealers, 1):
            try:
                t0 = _time.time()
                log_to_file(MONITORING_LOG, f"\n🌀 Обработка дилера {idx}/{len(dealers)}: {dealer_name} с никнеймами: {nicknames}\n")
                data = self._build_json(dealer_name, nicknames)
                t1 = _time.time()
                log_to_file(MONITORING_LOG, f"\n🌀 Получены данные для {dealer_name} (поиск занял {t1-t0:.3f} сек):")
                task = MonitoringStorageTask(self._convert_to_task_dict(dealer_name, nicknames, data))
                # pretty_print_dealer_info(task.__dict__)
                self._save(task)
            except Exception as e:
                log_to_file(MONITORING_LOG, f"⚠ Ошибка для {dealer_name}: {e}")
            # finally:
            #     time.sleep(15)  # Задержка между обработкой дилеров
    

    def _convert_to_task_dict(self, dealer_name, nicknames, data):
        # Преобразует результат _build_json в dict для MonitoringStorageTask
        shift = data["shifts"][0] if data.get("shifts") else {}
        return {
            "dealer_name": dealer_name,
            "dealer_nicknames": nicknames,
            "report_date": self.date,
            "shift_type": shift.get("type"),
            "shift_start": shift.get("start"),
            "shift_end": shift.get("end"),
            "break_number": 0,
            "is_scheduled": shift.get("is_scheduled", False),
            "is_additional": shift.get("is_additional", False),
            "is_extra": shift.get("is_extra", False),
            "is_sickleave": shift.get("is_sickleave", False),
            "is_vacation": shift.get("is_vacation", False),
            "is_did_not_come": shift.get("is_did_not_come", False),
            "is_left_the_shift": shift.get("is_left_the_shift", False),
            "assigned_floors": shift.get("assigned_floors", []),
            "floor_permits": data["permits"]["floors"] if data.get("permits") else {},
            "game_permits": data["permits"]["games"] if data.get("permits") else {},
            "has_mistakes": bool(data.get("mistakes")),
            "has_feedbacks": bool(data.get("feedbacks")),
            "rotation": shift.get("rotation", []),
            "mistakes": data.get("mistakes", []),
            "feedbacks": data.get("feedbacks", []),
            "raw_data": data,
        }

##################################################################################################
    # Получаем всех дилеров из SheetsInfo (Bonus BS live88)
####################################################################################################

    def _get_all_dealers(self):
        """
        Извлекает всех дилеров из SheetsInfo (Bonus BS live88),
        объединяет все никнеймы одного дилера
        Возвращает: [(dealer_name, [nickname1, nickname2, ...])]
        """
        rows = self.session.execute(
            select(SheetsInfo.values_json).where(
                SheetsInfo.name_of_process == "Bonus BS live88"
            )
        ).scalars().all()

        dealer_map = defaultdict(set)  # dealer_name -> set(nicknames)

        for values_json in rows:
            # Если values_json — строка, декодируем как JSON
            if isinstance(values_json, str):
                import json
                values_json = json.loads(values_json)
            for row in values_json:
                # row может быть строкой или списком
                if isinstance(row, str):
                    full_entry = row.strip()
                elif isinstance(row, list) and row and isinstance(row[0], str):
                    full_entry = row[0].strip()
                else:
                    continue  # пропуск пустых или некорректных строк
                if not full_entry:
                    continue
                match = re.match(r"^(.*?)\s*\((.+?)\)$", full_entry)
                if match:
                    dealer_name = match.group(1).strip()
                    nickname = match.group(2).strip()
                    dealer_map[dealer_name].add(nickname)
                else:
                    dealer_name = full_entry
                    dealer_map[dealer_name]  # создаётся с пустым set()

        # Конвертируем в список кортежей
        return [(name, list(nicks)) for name, nicks in dealer_map.items() if name]

###################################################################################################
    # Конструктор JSON для дилера
###################################################################################################

    def _build_json(self, dealer, nicknames):
        return {
            "date": str(self.date),
            "dealer": {
                "full_name": dealer,
                "nickname": nicknames
            },
            "permits": {
                "floors": self._get_floor_permits(dealer),
                "games": self._get_game_permits(dealer)
            },
            "shifts": [self._get_shift_info(dealer)],
            "feedbacks": self._get_feedbacks(dealer),
            "mistakes": self._get_mistakes(f"{dealer} ({nicknames[0]})") 
        }
    
    def _get_floor_permits(self, dealer_name):
        """
        Возвращает словарь по всем этажам: {"VIP": True/False/str, ...}
        Если ошибка — возвращает строку ошибки вместо статуса.
        """
        result = {}
        found_any = False
        for floor in ["VIP", "TURKISH", "GENERIC", "GSBJ", "LEGENDZ"]:
            try:
                found = dealer_name in self.floor_lists.get(floor, set())
                result[floor] = found
                marker = "✅" if found else "❌"
                log_to_file(MONITORING_LOG, f"[FLOOR_PERMITS] {marker} {dealer_name} на {floor}")
                if found:
                    found_any = True
            except Exception as e:
                error_msg = f"[FLOOR_PERMITS ERROR] {dealer_name} / {floor}: {e}"
                log_to_file(MONITORING_LOG, error_msg)
                result[floor] = error_msg
        if not found_any:
            log_to_file(MONITORING_LOG, f"[FLOOR_PERMITS WARNING] 🚫 Имя '{dealer_name}' не найдено ни в одном floor_list! Возможно, ошибка в написании имени.")
        return result

    def _get_game_permits(self, dealer_name):
        """
        Возвращает словарь по всем играм: {"Male": bool/str, ..., "SH": str/None}
        Если ошибка — возвращает строку ошибки вместо статуса.
        """
        games = ["Male", "BJ", "BC", "RL", "DT", "HSB", "swBJ", "swBC", "swRL", "SH"]
        result = {game: None for game in games}
        found = False
        error_msg = None
        for floor in ["VIP", "TURKISH", "GENERIC", "GSBJ", "LEGENDZ"]:
            try:
                floor_permits = self.game_permits.get(floor, {})
                if dealer_name in floor_permits:
                    permits = floor_permits[dealer_name]
                    for game in games:
                        if game == "SH":
                            val = permits.get(game)
                            result[game] = val if isinstance(val, str) and val else '❌'
                        else:
                            val = permits.get(game)
                            result[game] = True if val is True else False
                    found = True
                    log_to_file(MONITORING_LOG, f"[GAME_PERMITS] ✅ {dealer_name} найден в {floor}")
                    break
                else:
                    log_to_file(MONITORING_LOG, f"[GAME_PERMITS] ❌ {dealer_name} не найден в {floor}")
            except Exception as e:
                error_msg = f"[GAME_PERMITS ERROR] {dealer_name} / {floor}: {e}"
                log_to_file(MONITORING_LOG, error_msg)
        if not found:
            for game in games:
                result[game] = error_msg or '❌' if game != 'SH' else '❌'
            log_to_file(MONITORING_LOG, f"[GAME_PERMITS WARNING] 🚫 Имя '{dealer_name}' не найдено ни в одном game_permits! Возможно, ошибка в написании имени.")
        return result

    def _get_mistakes(self, dealer):
        """
        Быстрый поиск ошибок по in-memory self.mistakes_by_dealer.
        Если имя не найдено, пишет ошибку в лог.
        """
        try:
            entries = self.mistakes_by_dealer_date.get((dealer, self.date), [])
        except Exception as e:
            log_to_file(MONITORING_LOG, f"[MISTAKES ERROR] {dealer}: {e}")
            entries = []
        if not entries:
            log_to_file(MONITORING_LOG, f"[MISTAKES WARNING] Имя '{dealer}' не найдено в mistakes! Возможно, ошибка в написании имени.")
        table_map = {}
        for entry in entries:
            table = entry.table_name
            if table not in table_map:
                table_map[table] = []
            table_map[table].append({
                "time": entry.time.strftime("%H:%M") if entry.time else None,
                "issue": entry.mistake
            })
        return [
            {"table": table, "entries": ents} for table, ents in table_map.items()
        ]

    def _get_feedbacks(self, dealer):
        """
        Быстрый поиск фидбеков по in-memory self.feedbacks_by_dealer.
        Если имя не найдено, пишет ошибку в лог.
        """
        try:
            entries = self.feedbacks_by_dealer_date.get((dealer, self.date), [])
        except Exception as e:
            log_to_file(MONITORING_LOG, f"[FEEDBACKS ERROR] {dealer}: {e}")
            entries = []
        if not entries:
            log_to_file(MONITORING_LOG, f"[FEEDBACKS WARNING] Имя '{dealer}' не найдено в feedbacks! Возможно, ошибка в написании имени.")
        result = []
        for entry in entries:
            result.append({
                "id": str(entry.id),
                "topic": entry.reason,
                "floor": entry.floor,
                "game_type": entry.game,
                "reported_by": entry.sm_name_surname,
                "comment": entry.explanation_of_the_reason,
                "is_proof": bool(entry.proof) if hasattr(entry, 'proof') else False,
                "action_taken": entry.action_taken,
                "forwarded_by": entry.forwarded_feedback,
                "after_forward_comment": entry.comment_after_forwarding
            })
        return result

    def _get_shift_info(self, dealer):
        """
        Быстрый поиск ротаций по in-memory self.rotations_by_floor.
        """
        assigned_floors = []
        floor_rotations = {}
        for floor, all_rotations in self.rotations_by_floor_date.items():
            for values in all_rotations:
                found = False
                for row in values:
                    if isinstance(row, list) and dealer in row:
                        found = True
                        break
                    if isinstance(row, dict) and dealer in row.values():
                        found = True
                        break
                if found:
                    assigned_floors.append(floor)
                    floor_rotations[floor] = values
        # Формируем расписание на 12 часов с шагом 30 минут
        def build_schedule(floor, values):
            schedule = []
            from datetime import datetime, timedelta
            start_time = datetime.strptime("09:00", "%H:%M")
            for i in range(24):
                t = (start_time + timedelta(minutes=30*i)).strftime("%H:%M")
                table = ""
                for row in values:
                    if isinstance(row, dict) and row.get("time") == t and row.get("dealer") == dealer:
                        table = row.get("table", "")
                        break
                    if isinstance(row, list) and len(row) >= 3 and row[0] == t and row[1] == dealer:
                        table = row[2]
                        break
                schedule.append({"time": t, "table": table})
            return schedule
        rotation = []
        for floor, values in floor_rotations.items():
            rotation.append({
                "floor": floor,
                "schedule": build_schedule(floor, values)
            })
        # Используем текущий тип смены и время, если заданы (для поддержки day/night из scan_all_shifts_for_month)
        shift_type = getattr(self, 'current_shift_type', None) or "Day"
        start = getattr(self, 'current_shift_start', None) or "09:00"
        end = getattr(self, 'current_shift_end', None) or "21:00"
        extra_flags = {
            "is_scheduled": True,
            "is_additional": False,
            "is_extra": False,
            "is_sickleave": False,
            "is_vacation": False,
            "is_did_not_come": False,
            "is_left_the_shift": False
        }
        if shift_type == "Night":
            start = "21:00"
            end = "09:00"
        else:
            start = "09:00"
            end = "21:00"
        return {
            "type": shift_type,
            **extra_flags,
            "start": start,
            "end": end,
            "assigned_floors": assigned_floors,
            "rotation": rotation
        }
    

    def _save(self, task):
        entry = self.session.query(MonitoringStorage).filter_by(
            dealer_name=task.dealer_name, report_date=task.report_date
        ).first()

        if entry:
            entry.raw_data = task.raw_data
            entry.dealer_nicknames = task.dealer_nicknames
            entry.shift_type = task.shift_type
            entry.shift_start = task.shift_start
            entry.shift_end = task.shift_end
            entry.break_number = task.break_number
            entry.is_scheduled = task.is_scheduled
            entry.is_additional = task.is_additional
            entry.is_extra = task.is_extra
            entry.is_sickleave = task.is_sickleave
            entry.is_vacation = task.is_vacation
            entry.is_did_not_come = task.is_did_not_come
            entry.is_left_the_shift = task.is_left_the_shift
            entry.assigned_floors = task.assigned_floors
            entry.floor_permits = task.floor_permits
            entry.game_permits = task.game_permits
            entry.has_mistakes = task.has_mistakes
            entry.has_feedbacks = task.has_feedbacks
            entry.rotation = task.rotation
            entry.mistakes = task.mistakes
            entry.feedbacks = task.feedbacks
        else:
            entry = MonitoringStorage(
                dealer_name=task.dealer_name,
                dealer_nicknames=task.dealer_nicknames,
                report_date=task.report_date,
                shift_type=task.shift_type,
                shift_start=task.shift_start,
                shift_end=task.shift_end,
                break_number=task.break_number,
                is_scheduled=task.is_scheduled,
                is_additional=task.is_additional,
                is_extra=task.is_extra,
                is_sickleave=task.is_sickleave,
                is_vacation=task.is_vacation,
                is_did_not_come=task.is_did_not_come,
                is_left_the_shift=task.is_left_the_shift,
                assigned_floors=task.assigned_floors,
                floor_permits=task.floor_permits,
                game_permits=task.game_permits,
                has_mistakes=task.has_mistakes,
                has_feedbacks=task.has_feedbacks,
                rotation=task.rotation,
                mistakes=task.mistakes,
                feedbacks=task.feedbacks,
                raw_data=task.raw_data
            )
            self.session.add(entry)

        self.session.commit()

    def scan_all_shifts_for_month(self):
        """
        Пробегает по всем датам месяца до сегодняшней (включая Night shift текущего дня)
        и вызывает self.run() для каждой даты и смены.
        """
        today = self.date
        first_day = today.replace(day=1)
        last_day = today
        shift_types = [
            ("Day", "09:00", "21:00"),
            ("Night", "21:00", "09:00")
        ]
        current = first_day
        while current <= last_day:
            for shift_type, shift_start, shift_end in shift_types:
                self.date = current
                self.current_shift_type = shift_type
                self.current_shift_start = shift_start
                self.current_shift_end = shift_end
                log_to_file(MONITORING_LOG, f"\n=== {current} {shift_type} shift ===")
                self.run()
            current += timedelta(days=1)

if __name__ == "__main__":
    # Пример инициализации (замените на свою реальную инициализацию)
    from database.session import SessionLocal
    session = SessionLocal()
    monitoring_tokens = None  # или подставьте реальные токены
    doc_id_map = None        # или подставьте реальную карту id
    scanner = MonitoringStorageScanner(session, monitoring_tokens, doc_id_map)

    scanner.scan_all_shifts_for_month()
    print(f"Сканирование завершено: все смены с {scanner.date.replace(day=1)} по {scanner.date} (дата и shift для каждой записи сохранены в БД)")
