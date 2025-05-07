# main.py

import threading
import time
import signal
import sys
import logging
import asyncio
import platform
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from database import create_scanner_logs_table, connect_to_db, create_api_usage_table
from config import (
    DB_PATH,
    MAIN_LOG,
    BOT_TOKEN,
    SHEETSINFO_TOKEN,
    ROTATIONSINFO_TOKEN_1,
    ROTATIONSINFO_TOKEN_2
)
from utils import load_credentials
from logger import log_to_file
from data import return_tracked_tables
from settings_access import ensure_bot_settings_table, is_scanner_enabled
from handlers import menu
from rotationsinfo_scanner import RotationsInfoScanner
from sheetsinfo_scanner import SheetsInfoScanner

# ⬇️ Только для Windows
if platform.system() == "Windows":
    import winloop
    winloop.install()

# Загрузка .env
load_dotenv()

# Карта токенов для Rotation Scanner
rotation_tokens = {
    "RotationsInfo_scanner_1": ROTATIONSINFO_TOKEN_1,
    "RotationsInfo_scanner_2": ROTATIONSINFO_TOKEN_2
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

def start_sheets_scanner(conn, service, doc_id_map):
    while not stop_event.is_set():
        try:
            scanner = SheetsInfoScanner(conn, service, doc_id_map)
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

    # Подготовка БД
    create_scanner_logs_table() 
    ensure_bot_settings_table()
    create_api_usage_table()

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

    # if is_scanner_enabled("sheets_scanner"):
    #     sheets_service = load_credentials(SHEETSINFO_TOKEN)
    #     threading.Thread(
    #         target=start_sheets_scanner,
    #         args=(conn, sheets_service, doc_id_map),
    #         daemon=True
    #     ).start()

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
