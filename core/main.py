# core/main.py

import threading
import time
import signal
import sys
import logging
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from utils.clean import clear_db

from database.database import connect_to_db
from utils.logger import log_to_file
from core.config import (
    DB_PATH,
    MAIN_LOG,
    BOT_TOKEN,
    SHEETSINFO_TOKEN,
    ROTATIONSINFO_TOKEN_1,
    ROTATIONSINFO_TOKEN_2
)
from core.data import return_tracked_tables
from bot.settings_access import is_scanner_enabled
from bot.handlers import menu
from scanners.rotationsinfo_scanner import RotationsInfoScanner
from scanners.sheetsinfo_scanner import SheetsInfoScanner

# Карта токенов для Rotation Scanner
rotation_tokens = {
    "RotationsInfo_scanner_1": ROTATIONSINFO_TOKEN_1,
    "RotationsInfo_scanner_2": ROTATIONSINFO_TOKEN_2
}

sheet_tokens = {
    "SheetsInfo_scanner_1": SHEETSINFO_TOKEN
}

stop_event = threading.Event()

def start_rotations_scanner(conn, rotation_tokens, doc_id_map):
    while not stop_event.is_set():
        try:
            scanner = RotationsInfoScanner(conn, rotation_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в потоке RotationsInfoScanner: {e}")
            time.sleep(5)

def start_sheets_scanner(conn, sheet_tokens, doc_id_map):
    while not stop_event.is_set():
        try:
            scanner = SheetsInfoScanner(conn, sheet_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в потоке SheetsInfoScanner: {e}")
            time.sleep(5)

def signal_handler(sig, frame):
    print("\n🛑 Получен сигнал остановки. Завершение работы...")
    log_to_file(MAIN_LOG, "🛑 Скрипт остановлен пользователем.")
    stop_event.set()
    sys.exit(0)

async def main():
    print("🚀 Инициализация...")

    # clear_db("SheetsInfo")
    # clear_db("RotationsInfo")

    conn = connect_to_db(DB_PATH)
    doc_id_map = return_tracked_tables(conn)

    # Telegram-бот
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # ✅ Запускаем сканеры
    if is_scanner_enabled("rotations_scanner"):
        threading.Thread(
            target=start_rotations_scanner,
            args=(conn, rotation_tokens, doc_id_map),
            daemon=True
        ).start()

    if is_scanner_enabled("sheets_scanner"):
        threading.Thread(
            target=start_sheets_scanner,
            args=(conn, sheet_tokens, doc_id_map),
            daemon=True
        ).start()

    # ✅ Запускаем Telegram-бота
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_routers(menu.router)

    logging.info("🚀 Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
