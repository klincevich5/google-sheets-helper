# core/main.py

import threading
import time
import asyncio
import signal
import sys

from tg_bot.utils.settings_access import is_scanner_enabled
from tg_bot.main import main as telegram_main
from utils.logger import log_to_file

from core.config import (
    MAIN_LOG,
    SHEETSINFO_TOKEN,
    ROTATIONSINFO_TOKEN_1,
    ROTATIONSINFO_TOKEN_2
)
from database.session import SessionLocal
from core.data import return_tracked_tables
from scanners.rotationsinfo_scanner import RotationsInfoScanner
from scanners.sheetsinfo_scanner import SheetsInfoScanner
from scanners.monitoring_storage_scanner import MonitoringStorageScanner

# 🧾 Токены
rotation_tokens = {
    "RotationsInfo_scanner_1": ROTATIONSINFO_TOKEN_1,
    "RotationsInfo_scanner_2": ROTATIONSINFO_TOKEN_2
}

sheet_tokens = {
    "SheetsInfo_scanner_1": SHEETSINFO_TOKEN
}

# monitoring_tokens = {
#     "SheetsInfo_scanner_1": SHEETSINFO_TOKEN
# }

stop_event = threading.Event()
scanner_threads = []

# 🧠 Запуск Telegram-бота в отдельном потоке
def run_bot():
    asyncio.run(telegram_main())  # отдельно от основного цикла

# 🚦 Сканнеры
def start_rotations_scanner(rotation_tokens, doc_id_map):
    while not stop_event.is_set():
        try:
            session = SessionLocal()
            scanner = RotationsInfoScanner(session, rotation_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в RotationsInfoScanner: {e}")
            time.sleep(5)


def start_sheets_scanner(sheet_tokens, doc_id_map):
    while not stop_event.is_set():
        try:
            session = SessionLocal()
            scanner = SheetsInfoScanner(session, sheet_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в SheetsInfoScanner: {e}")
            time.sleep(5)


def start_monitoring_scanner(monitoring_tokens, doc_id_map):
    while not stop_event.is_set():
        try:
            session = SessionLocal()
            scanner = MonitoringStorageScanner(session, monitoring_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в MonitoringStorageScanner: {e}")
            time.sleep(5)

# 🛑 Завершение
def signal_handler(sig, frame):
    print("\n🛑 Получен сигнал остановки. Завершение работы...")
    log_to_file(MAIN_LOG, "🛑 Скрипт остановлен пользователем.")
    stop_event.set()
    for t in scanner_threads:
        t.join(timeout=2)
    sys.exit(0)


# 🚀 Основной асинхронный запуск
async def main():
    print("🚀 Инициализация...")
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Получение ID таблиц
    session = SessionLocal()
    doc_id_map = return_tracked_tables(session)

    t = threading.Thread(target=run_bot, daemon=True)
    t.start()
    scanner_threads.append(t)

    # Сканнеры
    if is_scanner_enabled("rotations_scanner"):
        t = threading.Thread(target=start_rotations_scanner, args=(rotation_tokens, doc_id_map), daemon=True)
        t.start()
        scanner_threads.append(t)

    if is_scanner_enabled("sheets_scanner"):
        t = threading.Thread(target=start_sheets_scanner, args=(sheet_tokens, doc_id_map), daemon=True)
        t.start()
        scanner_threads.append(t)

    # if is_scanner_enabled("monitoring_scanner"):
    #     t = threading.Thread(target=start_monitoring_scanner, args=(monitoring_tokens, doc_id_map), daemon=True)
    #     t.start()
    #     scanner_threads.append(t)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    finally:
        print("⛔ Остановка всех потоков...")
        log_to_file(MAIN_LOG, "🛑 Скрипт остановлен.")
        for t in scanner_threads:
            t.join(timeout=2)


if __name__ == "__main__":
    asyncio.run(main())
