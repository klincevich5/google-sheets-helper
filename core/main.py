# core/main.py

import threading
import time
import asyncio
import signal
from datetime import datetime
from zoneinfo import ZoneInfo

from tg_bot.utils.settings_access import is_scanner_enabled
from tg_bot.main import main as telegram_main
from utils.logger import log_to_file

from core.config import (
    MAIN_LOG,
    SHEETSINFO_TOKEN,
    ROTATIONSINFO_TOKEN_1,
    ROTATIONSINFO_TOKEN_2,
    TIMEZONE
)
from database.session import SessionLocal
from core.data import return_tracked_tables
from scanners.rotationsinfo_scanner import RotationsInfoScanner
from scanners.sheetsinfo_scanner import SheetsInfoScanner
from scanners.monitoring_storage_scanner import MonitoringStorageScanner

rotation_tokens = {
    "RotationsInfo_scanner_2": ROTATIONSINFO_TOKEN_2,
    "RotationsInfo_scanner_1": ROTATIONSINFO_TOKEN_1
}

sheet_tokens = {
    "SheetsInfo_scanner_1": SHEETSINFO_TOKEN
}

# monitoring_tokens = {
#     "SheetsInfo_scanner_1": SHEETSINFO_TOKEN
# }

# Проверка TIMEZONE
try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

stop_event = threading.Event()
scanner_threads = []

def run_bot():
    asyncio.run(telegram_main())

def start_rotations_scanner(rotation_tokens):
    while not stop_event.is_set():
        try:
            with SessionLocal() as session:
                doc_id_map = return_tracked_tables(session)
            scanner = RotationsInfoScanner(rotation_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в RotationsInfoScanner: {e}")
            time.sleep(5)

def start_sheets_scanner(sheet_tokens):
    while not stop_event.is_set():
        try:
            with SessionLocal() as session:
                doc_id_map = return_tracked_tables(session)
            scanner = SheetsInfoScanner(sheet_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в SheetsInfoScanner: {e}")
            time.sleep(5)

def start_monitoring_scanner(monitoring_tokens):
    while not stop_event.is_set():
        try:
            with SessionLocal() as session:
                doc_id_map = return_tracked_tables(session)
            scanner = MonitoringStorageScanner(monitoring_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в MonitoringStorageScanner: {e}")
            time.sleep(5)

def signal_handler(sig, frame):
    print("\n🛑 Получен сигнал остановки. Завершение работы...")
    log_to_file(MAIN_LOG, "🛑 Скрипт остановлен пользователем.")
    stop_event.set()

async def main():
    print(f"🟢 Скрипт запущен. Сейчас: {datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')}.")
    print("🚀 Инициализация...")
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    scanner_threads.append(bot_thread)

    if is_scanner_enabled("rotations_scanner"):
        t = threading.Thread(target=start_rotations_scanner, args=(rotation_tokens,), daemon=True)
        t.start()
        scanner_threads.append(t)

    if is_scanner_enabled("sheets_scanner"):
        t = threading.Thread(target=start_sheets_scanner, args=(sheet_tokens,), daemon=True)
        t.start()
        scanner_threads.append(t)

    try:
        while not stop_event.is_set():
            await asyncio.sleep(1)
    finally:
        print("⛔ Остановка всех потоков...")
        log_to_file(MAIN_LOG, "🛑 Скрипт остановлен.")
        for t in scanner_threads:
            t.join(timeout=2)
            if t.is_alive():
                print(f"⚠️ Поток {t.name} не завершился корректно.")
                log_to_file(MAIN_LOG, f"⚠️ Поток {t.name} не завершился корректно.")
        print("✅ Все потоки остановлены. Завершение работы.")

if __name__ == "__main__":
    asyncio.run(main())
