from datetime import datetime, timedelta
from typing import List

from scanner_base import BaseScanner
from database import get_rotations_tasks, list_tracked_documents
from task import Task
from logger import log_info, log_error
from utils import batch_get, batch_update
from config import ROTATIONS_LOG_FILE, ROTATIONS_SLEEP_SECONDS


class RotationsScanner(BaseScanner):
    def __init__(self) -> None:
        super().__init__(log_path=ROTATIONS_LOG_FILE, sleep_seconds=ROTATIONS_SLEEP_SECONDS)

    def get_active_tabs(self) -> List[str]:
        """Определение активных вкладок на основе времени суток."""
        now = datetime.now()
        tabs = []

        if 9 <= now.hour < 19:
            tabs.append(f"DAY {now.day}")
        elif 19 <= now.hour < 21:
            tabs.extend([f"DAY {now.day}", f"NIGHT {now.day}"])
        elif 21 <= now.hour <= 23:
            tabs.append(f"NIGHT {now.day}")
        elif 0 <= now.hour < 7:
            yesterday = now - timedelta(days=1)
            tabs.append(f"NIGHT {yesterday.day}")
        elif 7 <= now.hour < 9:
            yesterday = now - timedelta(days=1)
            tabs.extend([f"DAY {now.day}", f"NIGHT {yesterday.day}"])

        return tabs

    def scan_phase(self) -> None:
        """Фаза сканирования задач."""
        self.tasks = []

        raw_tasks = get_rotations_tasks()
        if not raw_tasks:
            log_info(self.log_path, "⚪ Нет задач в RotationsInfo.")
            return

        doc_map = {doc[1]: doc[2] for doc in list_tracked_documents()}
        active_tabs = self.get_active_tabs()

        tasks = [
            Task(data) for data in raw_tasks
            if data.get("source_page_name") in active_tabs
        ]

        ready_tasks = []
        for task in tasks:
            if task.is_ready_to_scan() and task.assign_doc_ids(doc_map):
                try:
                    response = batch_get(
                        service=self.service,
                        spreadsheet_id=task.source_doc_id,
                        ranges=[f"{task.source_page_name}!{task.source_page_area}"],
                        log_path=self.log_path
                    )
                    if response:
                        task.raw_values_json = next(iter(response[0].get("values", [])), [])
                        task.update_after_scan(success=True)
                        ready_tasks.append(task)
                    else:
                        task.update_after_scan(success=False)
                        log_error(self.log_path, f"⚠️ Пустой ответ при сканировании задачи {task.name_of_process}.")
                except Exception as e:
                    task.update_after_scan(success=False)
                    log_error(self.log_path, f"❌ Ошибка сканирования задачи {task.name_of_process}: {e}")

        self.tasks = ready_tasks

    def update_task(self, task: Task) -> None:
        """Обновление одной задачи."""
        data = [{
            "range": f"{task.target_page_name}!{task.target_page_area}",
            "values": task.values_json
        }]
        success = batch_update(
            service=self.service,
            spreadsheet_id=task.target_doc_id,
            data=data,
            log_path=self.log_path
        )
        task.update_after_upload(success)
