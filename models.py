from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from methods import PROCESSORS
from config import TIMEZONE

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
        self.values_json = data.get("values_json")
        self.target_table_type = data.get("target_table_type")
        self.target_page_name = data.get("target_page_name")
        self.target_page_area = data.get("target_page_area")
        self.update_group = data.get("update_group")
        self.last_update = self._parse_datetime(data.get("last_update"))
        self.update_quantity = data.get("update_quantity", 0)
        self.update_failures = data.get("update_failures", 0)
        self.need_update = data.get("need_update", 0)

        # Эти поля будут заполняться в сканере
        self.source_table = None
        self.target_table = None

        self.source_doc_id = None
        self.target_doc_id = None
        self.raw_values_json = None  # Данные из сканирования

    def _parse_datetime(self, value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None

    def is_ready_to_scan(self):
        if not self.last_scan:
            return True
        next_scan_time = self.last_scan + timedelta(seconds=self.scan_interval)
        return datetime.now(ZoneInfo(TIMEZONE)) >= next_scan_time

    def assign_doc_ids(self, doc_id_map):
        self.source_doc_id = doc_id_map.get(self.source_table_type)
        self.target_doc_id = doc_id_map.get(self.target_table_type)
        return self.source_doc_id is not None and self.target_doc_id is not None
        
    def update_after_scan(self, success: bool):
        self.last_scan = datetime.now(ZoneInfo(TIMEZONE))

        if success:
            self.scan_quantity += 1
            self.scan_failures = 0
            self.need_update = 1  # флаг, что задачу нужно потом выгрузить
        else:
            self.scan_failures += 1
            self.need_update = 0  # сбрасываем, чтобы не выгружалась при ошибке
            self.raw_values_json = None  # очищаем полученные данные

    def process_raw_value(self):
        if not self.raw_values_json:
            return

        method_name = self.process_data_method or "process_default"
        process_func = PROCESSORS.get(method_name)

        if not process_func:
            raise ValueError(f"Неизвестный метод обработки: {method_name}")

        processed_values = process_func(self.raw_values_json)
        self.values_json = processed_values

    def check_for_update(self):
        if not self.values_json:
            self.need_update = 0
            return

        import hashlib
        processed = str(self.values_json).encode("utf-8")
        new_hash = hashlib.md5(processed).hexdigest()

        if new_hash != self.hash:
            self.hash = new_hash
            self.need_update = 1
        else:
            self.need_update = 0

    def update_after_upload(self, success):
        if success:
            self.last_update = datetime.now(ZoneInfo(TIMEZONE))
            self.update_quantity += 1
        else:
            self.update_failures += 1
