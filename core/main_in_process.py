# core/main.py

import threading
import time
import signal
import sys
import asyncio
import logging

# from aiogram import Bot, Dispatcher
# from aiogram.client.default import DefaultBotProperties
# from aiogram.enums import ParseMode
# from aiogram.fsm.storage.memory import MemoryStorage

# from bot.settings_access import is_scanner_enabled
# from bot.handlers import menu

from utils.logger import log_to_file
from core.config import (
    MAIN_LOG,
    # BOT_TOKEN,
    SHEETSINFO_TOKEN,
    ROTATIONSINFO_TOKEN_1,
    ROTATIONSINFO_TOKEN_2
)
from database.session import SessionLocal
from core.data import return_tracked_tables
# from scanners.rotationsinfo_scanner import RotationsInfoScanner
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
scanner_thread = None  # глобальная переменная для доступа в сигнале

# def start_rotations_scanner(rotation_tokens, doc_id_map):
#     while not stop_event.is_set():
#         try:
#             session = SessionLocal()
#             scanner = RotationsInfoScanner(session, rotation_tokens, doc_id_map)
#             scanner.run()
#         except Exception as e:
#             log_to_file(MAIN_LOG, f"❌ Ошибка в потоке RotationsInfoScanner: {e}")
#             time.sleep(5)

def start_sheets_scanner(sheet_tokens, doc_id_map):
    while not stop_event.is_set():
        try:
            session = SessionLocal()
            scanner = SheetsInfoScanner(session, sheet_tokens, doc_id_map)
            scanner.run()
        except Exception as e:
            log_to_file(MAIN_LOG, f"❌ Ошибка в потоке SheetsInfoScanner: {e}")
            time.sleep(5)


def signal_handler(sig, frame):
    print("\n🛑 Получен сигнал остановки. Завершение работы...")
    log_to_file(MAIN_LOG, "🛑 Скрипт остановлен пользователем.")
    stop_event.set()
    if scanner_thread and scanner_thread.is_alive():
        scanner_thread.join()
    sys.exit(0)

async def main():
    print("🚀 Инициализация...")

    # Обработка сигналов завершения
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Получаем карту таблиц один раз
    session = SessionLocal()
    doc_id_map = return_tracked_tables(session)

    # threading.Thread(
    #         target=start_rotations_scanner,
    #         args=(rotation_tokens, doc_id_map),
    #         daemon=True
    #     ).start()
    
    scanner_thread = threading.Thread(
        target=start_sheets_scanner,
        args=(sheet_tokens, doc_id_map)
    )
    scanner_thread.start()

    # Основной цикл — ждёт сигнала остановки
    while not stop_event.is_set():
        await asyncio.sleep(1)

    scanner_thread.join()

    # # Запуск сканеров в отдельных потоках
    # if is_scanner_enabled("rotations_scanner"):
    #     threading.Thread(
    #         target=start_rotations_scanner,
    #         args=(rotation_tokens, doc_id_map),
    #         daemon=True
    #     ).start()
    
    # if is_scanner_enabled("sheets_scanner"):
    #     threading.Thread(
    #         target=start_sheets_scanner,
    #         args=(sheet_tokens, doc_id_map),
    #         daemon=True
    #     ).start()

    # Запуск Telegram-бота
    # bot = Bot(
    #     token=BOT_TOKEN,
    #     default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    # )
    # dp = Dispatcher(storage=MemoryStorage())
    # dp.include_routers(menu.router)

    # logging.info("🚀 Бот запущен")
    # await dp.start_polling(bot)

# if __name__ == "__main__":
#     asyncio.run(main())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)