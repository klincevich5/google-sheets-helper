# core/main.py

import threading
import time
import signal
import sys

from utils.logger import log_to_file
from core.config import (
    MAIN_LOG,
    SHEETSINFO_TOKEN,
    ROTATIONSINFO_TOKEN_1,
    ROTATIONSINFO_TOKEN_2
)
from database.session import SessionLocal
from core.data import return_tracked_tables
from scanners.sheetsinfo_scanner import SheetsInfoScanner

# Токены сканеров
rotation_tokens = {
    "RotationsInfo_scanner_1": ROTATIONSINFO_TOKEN_1,
    "RotationsInfo_scanner_2": ROTATIONSINFO_TOKEN_2
}

sheet_tokens = {
    "SheetsInfo_scanner_1": SHEETSINFO_TOKEN
}

# Событие для остановки всех потоков
stop_event = threading.Event()

def start_sheets_scanner(sheet_tokens, doc_id_map):
    while not stop_event.is_set():
        try:
            session = SessionLocal()
            scanner = SheetsInfoScanner(session, sheet_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в потоке SheetsInfoScanner: {e}")
            time.sleep(5)

import os

def signal_handler(sig, frame):
    print("\n🛑 Получен сигнал остановки. Завершаю без разговоров.")
    log_to_file(MAIN_LOG, "🛑 Жесткое завершение.")
    os._exit(1)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    print("🚀 Инициализация...")

    session = SessionLocal()
    doc_id_map = return_tracked_tables(session)

    scanner_thread = threading.Thread(
        target=start_sheets_scanner,
        args=(sheet_tokens, doc_id_map)
    )
    scanner_thread.start()

    # Грубый цикл, живущий пока не прилетит Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)
