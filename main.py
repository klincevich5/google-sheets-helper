# main.py

import threading
import time
import signal
import sys
import logging
import asyncio
import platform

from rotationsinfo_scanner import RotationsInfoScanner
from sheetsinfo_scanner import SheetsInfoScanner
from database import connect_to_db
from data import return_tracked_tables
from utils import load_credentials, build_doc_id_map
from logger import log_to_file
from config import DB_PATH, MAIN_LOG, BOT_TOKEN

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from handlers import menu

# ⬇️ Только для Windows
if platform.system() == "Windows":
    import winloop
    winloop.install()

stop_event = threading.Event()
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

async def main_bot():
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_routers(menu.router)

    logging.info("🚀 Бот запущен")
    await dp.start_polling(bot)

def start_rotations_scanner(conn, service, doc_id_map):
    while not stop_event.is_set():
        try:
            scanner = RotationsInfoScanner(conn, service, doc_id_map)
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

def main():
    print("🚀 Инициализация скрипта...")

    conn = connect_to_db(DB_PATH)
    service = load_credentials()
    doc_id_map = build_doc_id_map(return_tracked_tables(conn))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("🔄 Запуск потоков сканирования...")

    # thread_rotations = threading.Thread(target=start_rotations_scanner, args=(conn, service, doc_id_map), daemon=True)
    thread_sheets = threading.Thread(target=start_sheets_scanner, args=(conn, service, doc_id_map), daemon=True)

    # thread_rotations.start()
    thread_sheets.start()

    # Основной поток будет ждать, пока stop_event не будет установлен
    while not stop_event.is_set():
        time.sleep(1)

if __name__ == "__main__":
    # Можно переключать поведение по аргументу или переменной окружения
    asyncio.run(main_bot())
    main()
    # Или, если нужно запускать и потоки, и бота — можно объединить логики
