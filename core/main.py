# core/main.py

import threading
import time
import asyncio
import signal
import sys
import os
from datetime import datetime

from tg_bot.utils.settings_access import is_scanner_enabled
from tg_bot.main import main as telegram_main
from utils.logger import (
    log_info, log_success, log_warning, log_error, log_section, log_separator
)

from core.config import (
    MAIN_LOG,
    SHEETSINFO_LOG,
    ROTATIONSINFO_LOG,
    ROTATIONSINFO_RETRO_LOG,
    SHEETSINFO_RETRO_LOG,
    MONITORING_LOG,
    MONITORING_RETRO_LOG,
    SHEETSINFO_TOKEN,
    ROTATIONSINFO_TOKEN_1,
    ROTATIONSINFO_TOKEN_2,
)
from database.session import get_session, SessionLocal, engine  # 👈 импортируем сессию и engine
from core.data import return_tracked_tables
from scanners.rotationsinfo_scanner import RotationsInfoScanner
from scanners.sheetsinfo_scanner import SheetsInfoScanner
from scanners.monitoring_storage_scanner import MonitoringStorageScanner
from core.timezone import timezone, now
from core.time_provider import TimeProvider

rotation_tokens = {
    "RotationsInfo_scanner_1": ROTATIONSINFO_TOKEN_1
}

rotation_retro_tokens = {
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

def run_bot():
    asyncio.run(telegram_main())

def start_rotations_scanner(rotation_tokens):
    while not stop_event.is_set():
        try:
            scanner = RotationsInfoScanner(rotation_tokens)
            scanner.run()
        except Exception as e:
            log_error(MAIN_LOG, "main", None, "fail", "Ошибка в RotationsInfoScanner", exc=e)
            time.sleep(5)

def start_sheets_scanner(sheet_tokens):
    while not stop_event.is_set():
        try:
            scanner = SheetsInfoScanner(sheet_tokens)
            scanner.run()
        except Exception as e:
            log_error(MAIN_LOG, "main", None, "fail", "Ошибка в SheetsInfoScanner", exc=e)
            time.sleep(5)

def start_monitoring_scanner(monitoring_tokens):
    while not stop_event.is_set():
        try:
            scanner = MonitoringStorageScanner(monitoring_tokens)
            scanner.run()
        except Exception as e:
            log_error(MAIN_LOG, "main", None, "fail", "Ошибка в MonitoringStorageScanner", exc=e)
            time.sleep(5)

def signal_handler(sig, frame):
    print("\n🛑 Получен сигнал остановки. Завершение работы...")
    log_section(MAIN_LOG, "main", "🛑 Скрипт остановлен пользователем.")
    stop_event.set()

def run_retro_scanner(scanner_cls, token_map, log_path, start_date, end_date):
    from datetime import datetime, timedelta
    import time
    from utils.utils import load_credentials
    d = start_date
    while d <= end_date and not stop_event.is_set():
        scan_datetime = datetime.combine(d, datetime.min.time()).replace(hour=12)
        TimeProvider.set_time(scan_datetime)
        try:
            print(f"▶️ Ретро-сканирование {scanner_cls.__name__} на дату: {scan_datetime}")
            scanner = scanner_cls(token_map, log_file=log_path)
            from database.session import get_session
            from core.data import return_tracked_tables
            with get_session() as session:
                scanner.service = None
                token_name = list(scanner.token_map.keys())[0]
                token_path = scanner.token_map[token_name]
                scanner.token_name = token_name
                scanner.service = scanner.service or load_credentials(token_path, log_path)
                scanner.doc_id_map = return_tracked_tables(session)
                phase_methods = [
                    ("load_tasks", lambda: scanner.load_tasks(session)),
                    ("scan_phase", lambda: scanner.scan_phase(session)),
                    ("process_phase", lambda: scanner.process_phase(session)),
                    ("update_phase", lambda: scanner.update_phase(session)),
                ]
                for phase_name, method in list(phase_methods):
                    try:
                        from utils.logger import log_separator, log_info, log_success
                        log_separator(scanner.log_file, phase_name)
                        log_info(scanner.log_file, phase_name, None, "start", f"[RETRO] Старт этапа {phase_name} для {scan_datetime}")
                        method()
                        log_success(scanner.log_file, phase_name, None, "finish", f"[RETRO] Этап {phase_name} завершён для {scan_datetime}\n")
                    except Exception as e:
                        from utils.logger import log_error
                        log_error(scanner.log_file, phase_name, None, "fail", f"[RETRO] Ошибка на этапе {phase_name} для {scan_datetime}: {e}")
        except Exception as e:
            print(f"[RETRO][{scanner_cls.__name__}] Ошибка на {scan_datetime}: {e}")
        finally:
            TimeProvider.reset()
        d += timedelta(days=1)
        time.sleep(2)

def parse_date_arg(val):
    from datetime import datetime
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(val, fmt).date()
        except Exception:
            continue
    raise ValueError(f"Неверный формат даты: {val}")

async def main():
    print(f"🟢 Скрипт запущен. Сейчас: {now().strftime('%Y-%m-%d %H:%M:%S')}")
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

    # --- Обработка даты для сканеров ---
    scan_date = None
    scan_start = None
    scan_end = None
    for arg in sys.argv:
        if arg.startswith("--date="):
            scan_date = arg.split("=", 1)[1]
        if arg.startswith("--date-start="):
            scan_start = arg.split("=", 1)[1]
        if arg.startswith("--date-end="):
            scan_end = arg.split("=", 1)[1]
    if not scan_start:
        scan_start = os.environ.get("SCAN_DATE_START")
    if not scan_end:
        scan_end = os.environ.get("SCAN_DATE_END")
    if not scan_date:
        scan_date = os.environ.get("SCAN_DATE")
    # Если указан только scan_date — используем как start и end
    if scan_date and not scan_start and not scan_end:
        scan_start = scan_end = scan_date
    if scan_start and scan_end:
        from datetime import date
        start = parse_date_arg(scan_start)
        end = parse_date_arg(scan_end)
        print(f"⏳ Ретро-сканеры будут работать с интервалом: {start} — {end}")
        # Запуск ретро-потоков
        # t1 = threading.Thread(target=run_retro_scanner, args=(RotationsInfoScanner, rotation_tokens, ROTATIONSINFO_RETRO_LOG, start, end), daemon=True)
        # t1.start()
        # scanner_threads.append(t1)
        # t2 = threading.Thread(target=run_retro_scanner, args=(SheetsInfoScanner, sheet_tokens, SHEETSINFO_RETRO_LOG, start, end), daemon=True)
        # t2.start()
        # scanner_threads.append(t2)

        # Теперь run_retro_scanner поддерживает любой сканер с log_file
        # Пример запуска ретро-сканера мониторинга:
        # t3 = threading.Thread(target=run_retro_scanner, args=(MonitoringStorageScanner, monitoring_tokens, MONITORING_RETRO_LOG, start, end), daemon=True)
        # t3.start()
        # scanner_threads.append(t3)
    else:
        TimeProvider.reset()

    try:
        while not stop_event.is_set():
            await asyncio.sleep(1)
    finally:
        print("⛔ Остановка всех потоков...")
        log_section(MAIN_LOG, "main", "🛑 Скрипт остановлен.")

        for t in scanner_threads:
            t.join(timeout=2)
            if t.is_alive():
                print(f"⚠️ Поток {t.name} не завершился корректно.")
                log_warning(MAIN_LOG, "main", t.name, "not_stopped", f"⚠️ Поток {t.name} не завершился корректно.")

        # 👇 Закрытие всех сессий и соединений
        try:
            print("🔌 Завершение всех SQLAlchemy-сессий и соединений...")
            SessionLocal.close_all()
            engine.dispose()
            log_info(MAIN_LOG, "main", status="sql_cleanup", message="🔌 Все соединения SQLAlchemy закрыты.")
        except Exception as e:
            log_error(MAIN_LOG, "main", status="sql_cleanup_fail", message="❌ Ошибка при закрытии соединений", exc=e)

        print("✅ Все потоки остановлены. Завершение работы.")

if __name__ == "__main__":
    asyncio.run(main())

# Варианты запуска:
# python3 -m core.main
# python3 -m core.main --date-start=2025-05-01 --date-end=2025-06-12
# SCAN_DATE_START=2025-05-01 SCAN_DATE_END=2025-06-12 python3 -m core.main