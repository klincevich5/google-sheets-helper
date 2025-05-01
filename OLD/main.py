import threading
from config import SHEETS_LOG_FILE, ROTATIONS_LOG_FILE
from logger import log_to_file
from database import check_db_integrity, list_tracked_documents
from scanner_sheets import SheetsInfo_scanner
from scanner_rotations import RotationsInfo_scanner
import time

def main():
    log_to_file(SHEETS_LOG_FILE, "🚀 Запуск системы сканирования...")
    log_to_file(ROTATIONS_LOG_FILE, "🚀 Запуск системы сканирования...")

    try:
        check_db_integrity()
        log_to_file(SHEETS_LOG_FILE, "📦 База данных найдена.")
        log_to_file(ROTATIONS_LOG_FILE, "📦 База данных найдена.")
    except Exception as e:
        log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка: {e}")
        log_to_file(ROTATIONS_LOG_FILE, f"❌ Ошибка: {e}")
        return

    docs = list_tracked_documents()
    log_to_file(SHEETS_LOG_FILE, f"✅ Найдено {len(docs)} актуальных документов:")
    log_to_file(ROTATIONS_LOG_FILE, f"✅ Найдено {len(docs)} актуальных документов:")
    for doc in docs:
        line = f"├─ {doc[0]} | {doc[1]} | ID: {doc[2]}"
        log_to_file(SHEETS_LOG_FILE, line)
        log_to_file(ROTATIONS_LOG_FILE, line)

    thread1 = threading.Thread(target=SheetsInfo_scanner, daemon=True)
    thread2 = threading.Thread(target=RotationsInfo_scanner, daemon=True)
    thread1.start()
    thread2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log_to_file(SHEETS_LOG_FILE, "🛑 Остановка по Ctrl+C")
        log_to_file(ROTATIONS_LOG_FILE, "🛑 Остановка по Ctrl+C")

if __name__ == "__main__":
    main()
