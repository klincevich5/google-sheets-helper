# core/task_model.py

import json
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from core.methods import PROCESSORS
from core.config import TIMEZONE


class Task:
    def __init__(self, data):
        self.id = data.get("id")
        self.name_of_process = data.get("name_of_process")
        self.source_table_type = data.get("source_table_type")
        self.source_page_name = data.get("source_page_name")
        self.source_page_area = data.get("source_page_area")
        self.scan_group = data.get("scan_group")
        self.last_scan = self._parse_datetime(data.get("last_scan"))
        self.scan_interval = data.get("scan_interval") or 1800
        self.scan_quantity = data.get("scan_quantity", 0)
        self.scan_failures = data.get("scan_failures", 0)
        self.hash = data.get("hash")
        self.process_data_method = data.get("process_data_method", "process_default")

        # 🔽 Блок для безопасного чтения values_json из базы
        raw_values = data.get("values_json")
        if isinstance(raw_values, str):
            try:
                self.values_json = json.loads(raw_values)
            except json.JSONDecodeError:
                self.values_json = None  # или [] если нужно по умолчанию
        else:
            self.values_json = raw_values

        self.target_table_type = data.get("target_table_type")
        self.target_page_name = data.get("target_page_name")
        self.target_page_area = data.get("target_page_area")
        self.update_group = data.get("update_group")
        self.last_update = self._parse_datetime(data.get("last_update"))
        self.update_quantity = data.get("update_quantity", 0)
        self.update_failures = data.get("update_failures", 0)
        self.is_active = data.get("is_active", 1)

        # Эти поля будут заполняться в сканере
        self.source_table = None # база данных, откуда берем данные
        self.target_table = None # база данных, откуда берем данные

        self.source_doc_id = None
        self.target_doc_id = None
        self.raw_values_json = None  # Данные из сканирования

        self.scanned = 0  # Флаг, что задача была просканирована
        self.proceed = 0  # Флаг, что задача была обработана
        self.changed = 0  # Флаг, что задача была изменена
        self.uploaded = 0  # Флаг, что задача была выгружена

    def _parse_datetime(self, value):
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo(TIMEZONE))
            return dt
        except Exception:
            return None
        
    def is_ready_to_scan(self):
        if not self.last_scan:
            return True
        next_scan_time = self.last_scan + timedelta(seconds=self.scan_interval)
        return datetime.now(ZoneInfo(TIMEZONE)) >= next_scan_time

    def assign_doc_ids(self, doc_id_map):
        self.source_doc_id = doc_id_map.get(self.source_table_type)

        if self.target_table_type == "nothing":
            self.target_doc_id = "nothing"
        else:
            self.target_doc_id = doc_id_map.get(self.target_table_type)
        return self.source_doc_id is not None and self.target_doc_id is not None
        
    def update_after_scan(self, success: bool):
        if success:
            self.last_scan = datetime.now(ZoneInfo(TIMEZONE))
            self.scan_quantity += 1
            self.scan_failures = 0
            self.scanned = 1  # флаг, что задачу нужно обрабатывать
        else:
            self.scan_failures += 1
            self.scanned = 0    # флаг, что задачу не нужно обрабатывать

    def process_raw_value(self):
        if not self.raw_values_json:
            return

        method_name = self.process_data_method or "process_default"
        process_func = PROCESSORS.get(method_name)

        if not process_func:
            raise ValueError(f"Неизвестный метод обработки: {method_name}")

        try:
            # отдельная обработка process_schedule_OT так как она требует source_doc_id для определения месяца
            processed_values = process_func(self.raw_values_json, self.source_page_area)
            if not isinstance(processed_values, list) or not all(isinstance(row, list) for row in processed_values):
                raise ValueError("❌ Обработанные данные не являются списком списков")
            self.values_json = processed_values
        except Exception as e:
            raise ValueError(f"❌ Ошибка при вызове {method_name}: {e}")

    def check_for_update(self):
        if not self.values_json:
            self.proceed = 0
            self.changed = 0
            return

        try:
            # 🔒 Стабильная сериализация
            serialized = json.dumps(self.values_json, separators=(",", ":"), ensure_ascii=False)
            processed = serialized.encode("utf-8")
            new_hash = hashlib.md5(processed).hexdigest()
        except Exception:
            # Если что-то пошло не так — безопасно пропустить
            self.proceed = 0
            self.changed = 0
            return

        if new_hash != self.hash:
            self.hash = new_hash
            self.proceed = 1
            self.changed = 1
        else:
            self.proceed = 1
            self.changed = 0

    def update_after_upload(self, success):
        if success:
            self.last_update = datetime.now(ZoneInfo(TIMEZONE))
            self.update_quantity += 1
            self.uploaded = 1  # флаг, что задача была выгружена
        else:
            self.update_failures += 1
            self.uploaded = 0  # флаг, что задача не была выгружена
