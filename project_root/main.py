# main.py

import threading
import time
from logger import log_info, log_error
from database import check_db_integrity
from scanner_sheets import SheetsScanner
from scanner_rotations import RotationsScanner
from config import SHEETS_LOG_FILE, ROTATIONS_LOG_FILE
from notifier import send_telegram_message

def main():
    log_info(SHEETS_LOG_FILE, "🚀 Запуск системы сканирования SheetsInfo...")
    log_info(ROTATIONS_LOG_FILE, "🚀 Запуск системы сканирования RotationsInfo...")

    try:
        check_db_integrity()
        log_info(SHEETS_LOG_FILE, "✅ База данных проверена и готова к работе.")
        log_info(ROTATIONS_LOG_FILE, "✅ База данных проверена и готова к работе.")
    except Exception as e:
        log_error(SHEETS_LOG_FILE, f"❌ Ошибка проверки базы данных: {e}")
        log_error(ROTATIONS_LOG_FILE, f"❌ Ошибка проверки базы данных: {e}")
        send_telegram_message(SHEETS_LOG_FILE, f"❌ Ошибка инициализации базы данных: {e}")
        return

    # Инициализация сканеров
    sheets_scanner = SheetsScanner()
    rotations_scanner = RotationsScanner()

    # Запуск потоков
    thread1 = threading.Thread(target=sheets_scanner.start, daemon=True)
    thread2 = threading.Thread(target=rotations_scanner.start, daemon=True)
    thread1.start()
    thread2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log_info(SHEETS_LOG_FILE, "🛑 Остановка системы сканирования по Ctrl+C.")
        log_info(ROTATIONS_LOG_FILE, "🛑 Остановка системы сканирования по Ctrl+C.")

if __name__ == "__main__":
    main()
