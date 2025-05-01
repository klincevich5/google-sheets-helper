from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

import hashlib

from logger import log_info, log_warning, log_error
from methods import PROCESSORS  # Предполагается, что здесь твои методы обработки


class Task:
    def __init__(self, data: Dict[str, Any]) -> None:
        # Базовые поля
        self.id: Optional[int] = data.get("id")
        self.name_of_process: Optional[str] = data.get("name_of_process")
        self.source_table_type: Optional[str] = data.get("source_table_type")
        self.source_page_name: Optional[str] = data.get("source_page_name")
        self.source_page_area: Optional[str] = data.get("source_page_area")
        self.scan_group: Optional[str] = data.get("scan_group")
        self.last_scan: Optional[datetime] = self.parse_datetime(data.get("last_scan"))
        self.scan_interval: int = data.get("scan_interval", 1800)
        self.scan_quantity: int = data.get("scan_quantity", 0)
        self.scan_failures: int = data.get("scan_failures", 0)
        self.hash: Optional[str] = data.get("hash")
        self.process_data_method: str = data.get("process_data_method", "process_default")
        self.values_json: Optional[Any] = data.get("values_json")
        self.target_table_type: Optional[str] = data.get("target_table_type")
        self.target_page_name: Optional[str] = data.get("target_page_name")
        self.target_page_area: Optional[str] = data.get("target_page_area")
        self.update_group: Optional[str] = data.get("update_group")
        self.last_update: Optional[datetime] = self.parse_datetime(data.get("last_update"))
        self.update_quantity: int = data.get("update_quantity", 0)
        self.update_failures: int = data.get("update_failures", 0)
        self.need_update: int = data.get("need_update", 0)

        # Специальные поля
        self.raw_values_json: Optional[Any] = None  # Сырые данные после batchGet
        self.source_doc_id: Optional[str] = None    # Назначаются позже через doc_map
        self.target_doc_id: Optional[str] = None

    @staticmethod
    def parse_datetime(value: Optional[Any]) -> Optional[datetime]:
        if isinstance(value, datetime):
            # Если без таймзоны — добавляем UTC
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                return None
        return None

    def is_ready_to_scan(self) -> bool:
        if not self.last_scan:
            return True
        next_scan_time = self.last_scan + timedelta(seconds=self.scan_interval)
        return datetime.now(timezone.utc) >= next_scan_time

    def assign_doc_ids(self, doc_id_map: Dict[str, str]) -> bool:
        """Назначить ID документов по типу таблиц."""
        self.source_doc_id = doc_id_map.get(self.source_table_type)
        self.target_doc_id = doc_id_map.get(self.target_table_type)
        return self.source_doc_id is not None and self.target_doc_id is not None

    def update_after_scan(self, success: bool) -> None:
        """Обновить метаданные задачи после сканирования."""
        if success:
            self.last_scan = datetime.now(timezone.utc)
            self.scan_quantity += 1
        else:
            self.scan_failures += 1

    def process_raw_value(self) -> None:
        """Преобразовать сырые данные через метод обработки."""
        if not self.raw_values_json:
            log_warning("Нет данных для обработки в raw_values_json.")
            return

        process_func = PROCESSORS.get(self.process_data_method)
        if not process_func:
            log_error(f"Неизвестный метод обработки: {self.process_data_method}")
            return

        try:
            processed = process_func(self.raw_values_json)
            self.values_json = processed
        except Exception as e:
            log_error(f"Ошибка обработки данных методом {self.process_data_method}: {e}")

    def process_values(self) -> Optional[str]:
        """Посчитать MD5-хэш данных для проверки изменений."""
        if not self.values_json:
            return None
        raw = str(self.values_json).encode("utf-8")
        return hashlib.md5(raw).hexdigest()

    def check_for_update(self) -> None:
        """Проверить, изменились ли данные."""
        new_hash = self.process_values()
        if new_hash and new_hash != self.hash:
            self.hash = new_hash
            self.need_update = 1
        else:
            self.need_update = 0

    def update_after_upload(self, success: bool) -> None:
        """Обновить метаданные задачи после отправки."""
        if success:
            self.last_update = datetime.now(timezone.utc)
            self.update_quantity += 1
        else:
            self.update_failures += 1
