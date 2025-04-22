import time
from datetime import datetime

# Импортируем основные настройки и зависимости
from config import SCAN_INTERVAL_SECONDS, WARSAW_TZ, SHEETS_LOG_FILE
from auth import load_credentials
from database import get_pending_scans, get_doc_id_map
from logger import log_to_file
from handlers_sheets import handle_fetched_data, perform_group_import
from utils import filter_valid_tasks, get_group_ranges, fetch_data_from_sheet

def SheetsInfo_scanner():
    log_to_file(SHEETS_LOG_FILE, "🟢 SheetsInfo_scanner запущен.")

    while True:
        # ────────────────────────────────
        # ЭТАП 1: Получение задач и документов из базы
        # ────────────────────────────────

        # Получаем словарь doc_id всех доступных документов по типам (например, VIP, QA и т.д.)
        doc_id_map = get_doc_id_map()
        if not doc_id_map:
            log_to_file(SHEETS_LOG_FILE, "⚠️ Нет актуальных документов для сканирования.")
            return
        log_to_file(SHEETS_LOG_FILE, f"🟢 Получены doc_id корневых документов: {len(doc_id_map)}")

        # Получаем список задач, которые требуют сканирования
        tasks = get_pending_scans("SheetsInfo")
        if not tasks:
            log_to_file(SHEETS_LOG_FILE, f"⏳ Нет задач на {datetime.now(WARSAW_TZ).strftime('%H:%M:%S')}")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue
        log_to_file(SHEETS_LOG_FILE, f"🟢 Обнаружено всего задач: {len(tasks)}")

        # Отфильтровываем задачи, у которых нет нужных doc_id (то есть они пока не готовы к обработке)
        tasks_for_scan = filter_valid_tasks(tasks, doc_id_map)
        if not tasks_for_scan:
            log_to_file(SHEETS_LOG_FILE, "⚠️ Нет задач для обработки.")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue
        log_to_file(SHEETS_LOG_FILE, f"🟢 Получены задачи для сканирования: {len(tasks_for_scan)}")

        # ────────────────────────────────
        # ЭТАП 2: Подключение к Google Sheets
        # ────────────────────────────────

        # Загружаем авторизационные данные и создаём объект клиента для работы с API
        sheet = load_credentials()
        if sheet is None:
            log_to_file(SHEETS_LOG_FILE, "⚠️ Ошибка загрузки учетных данных.")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue
        log_to_file(SHEETS_LOG_FILE, "🟢 Учетные данные загружены.")

        # Собираем уникальные группы задач по полю scan_group
        scan_groups = set(t["scan_group"] for t in tasks_for_scan if t.get("scan_group"))
        if not scan_groups:
            log_to_file(SHEETS_LOG_FILE, "⚠️ Нет групп для сканирования.")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue
        log_to_file(SHEETS_LOG_FILE, "===========================")
        log_to_file(SHEETS_LOG_FILE, f"🟢 Группы для сканирования: {len(scan_groups)}")
        
        # Эти списки будут заполняться по ходу выполнения:
        changed_update_groups = []  # Сюда добавим группы, где были изменения и нужно делать импорт
        scanned_tasks = []          # Сюда собираем все обработанные задачи

        # ────────────────────────────────
        # ЭТАПЫ 3, 4, 5: Сканирование, получение данных, обработка данных
        # ────────────────────────────────

        for group in scan_groups:
            log_to_file(SHEETS_LOG_FILE, "==========================")
            log_to_file(SHEETS_LOG_FILE, f"🟢 Сканирование группы {group}")

            # Выбираем все задачи, относящиеся к текущей группе
            group_tasks = [t for t in tasks_for_scan if t["scan_group"] == group]

            # Получаем ID документа из первой задачи (все задачи в группе — из одного документа)
            doc_id = group_tasks[0]["source_doc_id"]

            # Получаем список уникальных диапазонов (range) для всех задач этой группы
            range_map = get_group_ranges(group_tasks)

            # Отправляем batch-запрос в Google Sheets, чтобы получить все диапазоны за один раз
            fetched = fetch_data_from_sheet(sheet, doc_id, list(range_map.keys()))
            if fetched is None:
                continue  # если ошибка — переходим к следующей группе

            # Обрабатываем полученные данные (вычисляем хэши, отмечаем изменения и ошибки)
            handle_fetched_data(fetched, range_map, changed_update_groups)

            # Добавляем все задачи этой группы в список уже отсканированных
            scanned_tasks.extend(group_tasks)

        # ────────────────────────────────
        # ЭТАПЫ 6, 7, 8: Подготовка, импорт и обновление базы
        # ────────────────────────────────

        for group in set(changed_update_groups):
            # Для каждой группы, где были изменения, запускаем процесс импорта данных обратно в целевые документы
            group_tasks = [t for t in scanned_tasks if t.get("update_group") == group]
            perform_group_import(sheet, group_tasks)

        # После одного прохода сканирования и импорта — выходим из цикла
        break

    log_to_file(SHEETS_LOG_FILE, "🔴 SheetsInfo_scanner завершен.")

# Запускаем функцию, если скрипт запускается напрямую
if __name__ == "__main__":
    SheetsInfo_scanner()
