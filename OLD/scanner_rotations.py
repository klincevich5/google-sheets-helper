import time
from datetime import datetime, timedelta

# Импортируем основные настройки и зависимости
from config import SCAN_INTERVAL_SECONDS, WARSAW_TZ, ROTATIONS_LOG_FILE
from auth import load_credentials
from database import get_pending_scans, get_doc_id_map
from logger import log_to_file
from handlers import handle_fetched_data, perform_group_import, handle_main_rotations_group
from utils import filter_valid_tasks, get_group_ranges, fetch_data_from_sheet

from clean import clear_db

log_file = ROTATIONS_LOG_FILE
table_name = "RotationsInfo"  # Имя таблицы в базе данных

def RotationsInfo_scanner():
    log_to_file(log_file, "=" * 60)
    log_to_file(log_file, "🟢 RotationsInfo_scanner запущен.")
    log_to_file(log_file, "=" * 60)

    while True:
        # ────────────────────────────────
        # ЭТАП 1: Получение задач и документов из базы
        # ────────────────────────────────
        log_to_file(log_file, "" * 60)
        log_to_file(log_file, "Cканирование задач...")
        log_to_file(log_file, "" * 60)
        # Получаем словарь doc_id всех доступных документов по типам (например, VIP, QA и т.д.)
        doc_id_map = get_doc_id_map()
        if not doc_id_map:
            log_to_file(log_file, "⚠️ Нет актуальных документов для сканирования.")
            return
        log_to_file(log_file, f"🟢 Получены doc_id корневых документов: {len(doc_id_map)}")

        # Получаем список задач, которые требуют сканирования
        tasks = get_pending_scans(table_name)
        if not tasks:
            log_to_file(log_file, f"⏳ Нет задач на {datetime.now(WARSAW_TZ).strftime('%H:%M:%S')}")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue
        log_to_file(log_file, f"🟢 Обнаружено всего задач: {len(tasks)}")
        
        # Отфильтровываем задачи, у которых нет нужных doc_id (то есть они пока не готовы к обработке)
        tasks_for_scan = filter_valid_tasks(tasks, doc_id_map, log_file)
        if not tasks_for_scan:
            log_to_file(log_file, "⚠️ Нет задач для обработки.")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue
        log_to_file(log_file, f"🟢 Получены задачи для сканирования: {len(tasks_for_scan)}")

        now = datetime.now()
        hour = now.hour
        tab_list = []

        # Determine which tabs to process based on time
        if 9 <= hour < 19:
            day_tab = f"DAY {now.day}"
            night_tab = "missing"
            tab_list.append(day_tab)

        elif 19 <= hour < 21:
            day_tab = f"DAY {now.day}"
            night_tab = f"NIGHT {now.day}"
            tab_list.append(day_tab)
            tab_list.append(night_tab)

        elif 21 <= hour <= 23:
            day_tab = "missing"
            night_tab = f"NIGHT {now.day}"
            tab_list.append(night_tab)

        elif 0 <= hour < 7:
            yesterday = now - timedelta(days=1)
            day_tab = "missing"
            night_tab = f"NIGHT {yesterday.day}"
            tab_list.append(night_tab)

        elif 7 <= hour < 9:
            yesterday = now - timedelta(days=1)
            day_tab = f"DAY {now.day}"
            night_tab = f"NIGHT {yesterday.day}"
            tab_list.append(day_tab)
            tab_list.append(night_tab)
        log_to_file(log_file, f"📄 day_tab: {day_tab}, night_tab: {night_tab}")
        log_to_file(log_file, f"📂 Таблицы к проверке: {tab_list}")
        tasks_for_scan = [task for task in tasks_for_scan if task.get("source_page_name") in tab_list]

        if not tasks_for_scan:
            log_to_file(log_file, f"⚠️ Нет задач, соответствующих активным сменам: {tab_list}")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue

        log_to_file(log_file, f"🟢 Задач с подходящими вкладками: {len(tasks_for_scan)}")

        # ────────────────────────────────
        # ЭТАП 2: Подключение к Google Sheets
        # ────────────────────────────────

        # Загружаем авторизационные данные и создаём объект клиента для работы с API
        sheet = load_credentials()
        if sheet is None:
            log_to_file(log_file, "⚠️ Ошибка загрузки учетных данных.")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue
        log_to_file(log_file, "🟢 Учетные данные загружены.")

        # Собираем уникальные группы задач по полю scan_group
        scan_groups = set(t["scan_group"] for t in tasks_for_scan if t.get("scan_group"))
        if not scan_groups:
            log_to_file(log_file, "⚠️ Нет групп для сканирования.")
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue
        log_to_file(log_file, "#" * 30)
        log_to_file(log_file, f"🟢 Группы для сканирования: {len(scan_groups)}")
        
        # Эти списки будут заполняться по ходу выполнения:
        changed_update_groups = []  # Сюда добавим группы, где были изменения и нужно делать импорт
        scanned_tasks = []          # Сюда собираем все обработанные задачи

        # ────────────────────────────────
        # ЭТАПЫ 3, 4, 5: Сканирование, получение данных, обработка данных
        # ────────────────────────────────

        # Сканируем по группам (группы связаны с одним документом)
        for group in scan_groups:
            log_to_file(log_file, "==========================\n")
            log_to_file(log_file, f"🟢 Сканирование группы {group}\n")

            # Выбираем все задачи, относящиеся к текущей группе
            group_tasks = [t for t in tasks_for_scan if t["scan_group"] == group]
            for task in group_tasks:
                log_to_file(log_file, f"  ID={task['id']} | {task['process_data_method']}| {task['source_doc_id']}| {task['source_page_name']} | {task['source_page_area']}")

            # Получаем ID документа из первой задачи (все задачи в группе — из одного документа)
            doc_id = group_tasks[0]["source_doc_id"]

            # Получаем список уникальных диапазонов (range) для всех задач этой группы
            range_map = get_group_ranges(group_tasks, log_file)
            log_to_file(log_file, f"🟢 Получены диапазоны для группы {group}: {len(range_map)} диапазонов")

            # Отправляем batch-запрос в Google Sheets, чтобы получить все диапазоны за один раз
            fetched = fetch_data_from_sheet(sheet, doc_id, list(range_map.keys()), log_file)
            log_to_file(log_file, f"🟢 Получены данные из Google Sheets для группы {group}: {len(fetched)} диапазонов")
            for i, r in enumerate(fetched):
                log_to_file(log_file, f"  {i+1} | {r['range']} | {r['values']}")    
            if fetched is None:
                log_to_file(log_file, f"⚠️ Ошибка получения данных из Google Sheets для группы {group}.")
                continue  # если ошибка — переходим к следующей группе

            # Обрабатываем полученные данные (вычисляем хэши, отмечаем изменения и ошибки)
            handle_fetched_data(fetched, range_map, changed_update_groups, table_name, log_file)

            # Добавляем все задачи этой группы в список уже отсканированных
            scanned_tasks.extend(group_tasks)

        for task in scanned_tasks:
            log_to_file(log_file, f"  ID={task['id']} | {task['process_data_method']}| {task['source_doc_id']}| {task['source_page_name']} | {task['source_page_area']}")
        # 🔁 Обработка группы update_main — ПЕРВОЙ
        main_update_tasks = [t for t in scanned_tasks if t.get("update_group") == "update_main"]
        if main_update_tasks:
            log_to_file(table_name, f"🟡 Обработка группы update_main ({len(main_update_tasks)} задач)")
            handle_main_rotations_group(sheet, main_update_tasks, table_name, log_file)

        # # Убираем обработанную группу update_main из общего списка
        # tasks_for_scan = [t for t in tasks_for_scan if t.get("update_group") != "update_main"]

        # # 🔁 Обработка остальных групп update_*
        # remaining_update_groups = [g for g in set(changed_update_groups) if g != "update_main"]

        # for group in remaining_update_groups:
        #     group_tasks = [t for t in scanned_tasks if t.get("update_group") == group]
        #     log_to_file(log_file, f"🔄 Обработка группы {group} ({len(group_tasks)} задач)")
        #     perform_group_import(sheet, group_tasks, table_name, log_file)

        # После одного прохода сканирования и импорта — выходим из цикла

# Запускаем функцию, если скрипт запускается напрямую
if __name__ == "__main__":
    clear_db(table_name)
    RotationsInfo_scanner()
    log_to_file(log_file, "🔴 RotationsInfo_scanner завершен.")
