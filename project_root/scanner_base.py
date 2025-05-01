import time
from typing import List
from logger import log_info, log_error
from notifier import send_telegram_message
from google_service import GoogleSheetsService


class BaseScanner:
    def __init__(self, log_path: str, sleep_seconds: int) -> None:
        self.log_path: str = log_path
        self.sleep_seconds: int = sleep_seconds
        self.service = GoogleSheetsService(self.log_path).service
        self.tasks: List = []

    def scan_phase(self) -> None:
        """Фаза сканирования задач. Переопределяется в потомках."""
        raise NotImplementedError("Метод scan_phase должен быть переопределен в дочернем классе.")

    def process_phase(self) -> None:
        """Фаза обработки задач."""
        if not self.tasks:
            log_info(self.log_path, "⚪ Нет задач для обработки.")
            return

        for task in self.tasks:
            try:
                task.process_raw_value()
                task.check_for_update()
            except Exception as e:
                log_error(self.log_path, f"❌ Ошибка обработки задачи {getattr(task, 'name_of_process', 'Неизвестная задача')}: {e}")

    def update_phase(self) -> None:
        """Фаза обновления задач."""
        if not self.tasks:
            log_info(self.log_path, "⚪ Нет задач для обновления.")
            return

        tasks_to_update = [task for task in self.tasks if getattr(task, 'need_update', 0) == 1]

        if not tasks_to_update:
            log_info(self.log_path, "⚪ Нет задач, требующих обновления.")
            return

        for task in tasks_to_update:
            try:
                self.update_task(task)
            except Exception as e:
                log_error(self.log_path, f"❌ Ошибка обновления задачи {getattr(task, 'name_of_process', 'Неизвестная задача')}: {e}")
                send_telegram_message(self.log_path, f"❌ Ошибка обновления задачи {getattr(task, 'name_of_process', 'Неизвестная задача')}: {e}")

    def update_task(self, task) -> None:
        """Обновление одной задачи. Должно быть переопределено в потомках."""
        raise NotImplementedError("Метод update_task должен быть переопределен в дочернем классе.")

    def start(self) -> None:
        """Основной цикл."""
        log_info(self.log_path, "🚀 Запуск основного цикла сканирования.")
        while True:
            try:
                self.scan_phase()
                self.process_phase()
                self.update_phase()
            except KeyboardInterrupt:
                log_info(self.log_path, "🛑 Остановка системы сканирования по Ctrl+C.")
                break
            except Exception as e:
                log_error(self.log_path, f"❌ Фатальная ошибка в основном цикле: {e}")
                send_telegram_message(self.log_path, f"❌ Ошибка сканера: {e}")
            time.sleep(self.sleep_seconds)
