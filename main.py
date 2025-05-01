# main.py

import threading
import time
import signal
import sys
from rotationsinfo_scanner import RotationsInfoScanner
from sheetsinfo_scanner import SheetsInfoScanner
from database import connect_to_db
from data import return_tracked_tables
from utils import load_credentials, build_doc_id_map
from logger import log_to_file  # если логирование нужно
from config import DB_PATH, MAIN_LOG

stop_event = threading.Event()  # Событие для корректной остановки всех потоков

def start_rotations_scanner(conn, service, doc_id_map):
    while not stop_event.is_set():
        try:
            scanner = RotationsInfoScanner(conn, service, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в потоке RotationsInfoScanner: {e}")
            time.sleep(5)  # Пауза перед перезапуском

def start_sheets_scanner(conn, service, doc_id_map):
    while not stop_event.is_set():
        try:
            scanner = SheetsInfoScanner(conn, service, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в потоке SheetsInfoScanner: {e}")
            time.sleep(5)  # Пауза перед перезапуском

def signal_handler(sig, frame):
    print("\n🛑 Получен сигнал остановки. Завершение работы...")
    log_to_file(MAIN_LOG, "🛑 Скрипт остановлен пользователем.")
    stop_event.set()
    sys.exit(0)

def main():
    print("🚀 Инициализация скрипта...")

    # Подключение к базе данных и Google Sheets
    conn = connect_to_db(DB_PATH)
    service = load_credentials()

    # Построение карты документов для текущего месяца
    doc_id_map = build_doc_id_map(return_tracked_tables(conn))

    # Обработка Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Запуск сканеров в отдельных потоках
    print("🔄 Запуск потоков сканирования...")

    thread_rotations = threading.Thread(target=start_rotations_scanner, args=(conn, service, doc_id_map), daemon=True)
    # thread_sheets = threading.Thread(target=start_sheets_scanner, args=(conn, service, doc_id_map), daemon=True)

    thread_rotations.start()
    # thread_sheets.start()

    while not stop_event.is_set():
        time.sleep(1)  # Просто держим основной поток живым

    print("✅ Все потоки завершены.")

if __name__ == "__main__":
    main()
