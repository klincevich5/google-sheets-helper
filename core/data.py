# core/data.py

from datetime import datetime, timedelta
from core.task_model import Task
from sqlalchemy import text

from database.db_models import TrackedTables, TaskTemplate, RotationsInfo, SheetsInfo
from core.task_model import Task
from database.session import get_session
from core.time_provider import TimeProvider
from utils.logger import (
    log_info, log_success, log_error, log_section, log_separator
)

def return_tracked_tables(session) -> dict:
    """Возвращает актуальные table_type -> spreadsheet_id из TrackedTables"""
    today = TimeProvider.now().date()
    tables = session.query(TrackedTables).all()
    return {
        table.table_type: table.spreadsheet_id
        for table in tables
        if table.valid_from <= today <= table.valid_to
    }

def get_active_tabs(current_time=None):
    """Определяет активные вкладки в зависимости от текущего времени"""
    current_time = current_time or TimeProvider.now()
    day = current_time.day
    hour = current_time.hour
    prev_day = (current_time - timedelta(days=1))

    if 9 <= hour < 19:
        return [f"DAY {day}"]
    elif 19 <= hour < 21:
        return [f"DAY {day}", f"NIGHT {day}"]
    elif 21 <= hour <= 23:
        return [f"NIGHT {day}"]
    elif 0 <= hour < 7:
        # Не возвращать NIGHT для предыдущего месяца
        if prev_day.month != current_time.month:
            return []
        return [f"NIGHT {prev_day.day}"]
    elif 7 <= hour < 9:
        # Не возвращать NIGHT для предыдущего месяца
        if prev_day.month != current_time.month:
            return [f"DAY {day}"]
        return [f"DAY {day}", f"NIGHT {prev_day.day}"]
    return []

def parse_datetime(value):
    """Парсит дату и время из строки или возвращает минимальное значение даты с учетом таймзоны"""
    tz = TimeProvider.timezone()
    if not value:
        return datetime.min.replace(tzinfo=tz)
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return datetime.min.replace(tzinfo=tz)
    if getattr(value, 'tzinfo', None) is None or isinstance(getattr(value, 'tzinfo', None), str):
        return value.replace(tzinfo=tz)
    return value

def build_task(row, now, source_table):
    """Создает задачу на основе строки данных и источника таблицы"""
    task = Task({
        "id": row.id,
        "is_active": row.is_active,
        "related_month": row.related_month,
        "name_of_process": row.name_of_process,
        "source_table_type": row.source_table_type,
        "source_page_name": getattr(row, "source_page_name", None),
        "source_page_area": row.source_page_area,
        "scan_group": row.scan_group,
        "last_scan": row.last_scan,
        "scan_interval": row.scan_interval,
        "scan_quantity": row.scan_quantity,
        "scan_failures": row.scan_failures,
        "hash": row.hash,
        "process_data_method": row.process_data_method,
        "values_json": row.values_json,
        "target_table_type": row.target_table_type,
        "target_page_name": row.target_page_name,
        "target_page_area": row.target_page_area,
        "update_group": row.update_group,
        "last_update": row.last_update,
        "update_quantity": row.update_quantity,
        "update_failures": row.update_failures
    })
    task.source_table = source_table
    return task

def get_related_month(now_time=None):
    """
    Возвращает related_month для задачи:
    - если сейчас первые 9 часов месяца и это ночная смена (0:00-8:59), related_month — предыдущий месяц
    - иначе related_month — текущий месяц
    """
    now_time = now_time or TimeProvider.now()
    if now_time.day == 1 and now_time.hour < 8:
        # Предыдущий месяц
        prev_month = (now_time.replace(day=1) - timedelta(days=1)).replace(day=1)
        return prev_month.date()
    return now_time.replace(day=1).date()

def load_rotationsinfo_tasks(session, log_file):
    log_section(log_file, "define_tasks", "🔼 Фаза определения задач (RotationsInfo)")
    now_time = TimeProvider.now()
    related_month = get_related_month(now_time)
    active_tabs = get_active_tabs(now_time)
    log_info(log_file, "define_tasks", None, "now", f"🕒 Сейчас: {now_time}. related_month: {related_month}")
    log_info(log_file, "define_tasks", None, "active_tabs", f"📄 Активные смены: {active_tabs}")

    try:
        templates = session.query(TaskTemplate).filter_by(source_table="RotationsInfo").all()
        template_names = [tmpl.name_of_process for tmpl in templates]
        log_info(log_file, "define_tasks", None, "templates", f"📚 Количество шаблонов: {len(templates)}")

        all_tasks = session.query(RotationsInfo).filter(
            RotationsInfo.related_month == related_month,
            RotationsInfo.name_of_process.in_(template_names),
            RotationsInfo.source_page_name.in_(active_tabs)
        ).all()
        log_info(log_file, "define_tasks", None, "db_tasks", f"📦 Количество задач в БД для related_month={related_month}, active_tabs={active_tabs}: {len(all_tasks)}")

        existing = {
            (t.name_of_process, t.source_page_name): t
            for t in all_tasks
        }

        new_tasks = []
        created_count = 0
        for tmpl in templates:
            for tab in active_tabs:
                key = (tmpl.name_of_process, tab)
                if key not in existing:
                    try:
                        new_task = RotationsInfo(
                            name_of_process=tmpl.name_of_process,
                            source_table_type=tmpl.source_table_type,
                            source_page_name=tab,
                            source_page_area=tmpl.source_page_area,
                            scan_group=tmpl.scan_group,
                            scan_interval=tmpl.scan_interval,
                            process_data_method=tmpl.process_data_method,
                            target_table_type=tmpl.target_table_type,
                            target_page_name=tab,
                            target_page_area=tmpl.target_page_area,
                            update_group=tmpl.update_group,
                            is_active=1,
                            related_month=related_month,
                            scan_quantity=0,
                            scan_failures=0,
                            update_quantity=0,
                            update_failures=0,
                            last_scan=None,
                            last_update=None,
                            hash=None,
                            values_json=None
                        )
                        new_tasks.append(new_task)
                        existing[key] = new_task
                        created_count += 1
                        log_success(log_file, "define_tasks", tmpl.name_of_process, "created", f"Задача '{tmpl.name_of_process}' для смены '{tab}' создана.")
                    except Exception as e:
                        log_error(log_file, "define_tasks", tmpl.name_of_process, "fail", f"Ошибка при создании задачи: {e}")
        log_info(log_file, "define_tasks", None, "new_tasks", f"Создано новых задач: {created_count}")

        if new_tasks:
            session.bulk_save_objects(new_tasks)
            session.commit()
            log_success(log_file, "define_tasks", None, "saved", f"📥 Сохранено новых задач: {len(new_tasks)}")

        active_tasks = session.query(RotationsInfo).filter(
            RotationsInfo.related_month == related_month,
            RotationsInfo.name_of_process.in_(template_names),
            RotationsInfo.source_page_name.in_(active_tabs),
            RotationsInfo.is_active == 1
        ).all()
        log_info(log_file, "define_tasks", None, "active", f"✅ Количество активных задач: {len(active_tasks)}")

        tasks = []
        for task in active_tasks:
            try:
                last_scan = parse_datetime(task.last_scan)
                next_scan = last_scan + timedelta(seconds=task.scan_interval)
                minutes_left = int((next_scan - now_time).total_seconds() / 60)
                log_info(log_file, "define_tasks", task.name_of_process, "ready",
                    f"[✅READY] Task '{task.name_of_process} {task.source_page_name}' | "
                    f"Last scan: {last_scan:%Y-%m-%d %H:%M:%S} | "
                    f"Interval: {task.scan_interval // 60} min | "
                    f"In: {minutes_left} min | "
                    f"Next scan at: {next_scan:%Y-%m-%d %H:%M} | "
                    f"Now: {now_time:%Y-%m-%d %H:%M:%S}"
                )
                built_task = build_task(task, now_time, "RotationsInfo")
                # Проверка assign_doc_ids (если используется)
                if hasattr(built_task, 'assign_doc_ids'):
                    doc_id_map = getattr(task, 'doc_id_map', None)
                    if doc_id_map:
                        ok = built_task.assign_doc_ids(doc_id_map)
                        log_info(log_file, "define_tasks", task.name_of_process, "assign_doc_ids", f"assign_doc_ids вернул: {ok}")
                tasks.append(built_task)
            except Exception as e:
                log_error(log_file, "define_tasks", task.name_of_process, "fail", f"Ошибка при обработке задачи: {e}")
        log_success(log_file, "define_tasks", None, "done", f"✅ Готово. Всего задач к запуску: {len(tasks)}")
        return tasks
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()
        
def load_sheetsinfo_tasks(session, log_file):
    """
    Загружает задачи из SheetsInfo, создаёт недостающие на основе шаблонов, фильтрует по времени сканирования.
    Возвращает список Task-объектов, готовых к выполнению.
    """
    log_section(log_file, "define_tasks", "🔼 Фаза определения задач (SheetsInfo)")
    now_time = TimeProvider.now()
    related_month = get_related_month(now_time)
    log_info(log_file, "define_tasks", None, "now", f"🕒 Сейчас: {now_time}. related_month: {related_month}")

    try:
        # Шаг 1: получаем шаблоны
        templates = session.query(TaskTemplate).filter_by(source_table="SheetsInfo").all()
        template_names = [tmpl.name_of_process for tmpl in templates]
        log_info(log_file, "define_tasks", None, "templates", f"📚 Количество шаблонов: {len(templates)}")

        # Шаг 2: получаем задачи из БД
        all_tasks = session.query(SheetsInfo).filter(
            SheetsInfo.related_month == related_month,
            SheetsInfo.name_of_process.in_(template_names)
        ).all()
        log_info(log_file, "define_tasks", None, "db_tasks",
                f"📦 Количество задач в БД для related_month={related_month}: {len(all_tasks)}")

        existing = {t.name_of_process: t for t in all_tasks}
        new_tasks = []
        created_count = 0

        # Шаг 3: создаём недостающие задачи
        for tmpl in templates:
            if tmpl.name_of_process not in existing:
                try:
                    new_task = SheetsInfo(
                        name_of_process=tmpl.name_of_process,
                        source_table_type=tmpl.source_table_type,
                        source_page_name=tmpl.source_page_name,
                        source_page_area=tmpl.source_page_area,
                        scan_group=tmpl.scan_group,
                        scan_interval=tmpl.scan_interval,
                        process_data_method=tmpl.process_data_method,
                        target_table_type=tmpl.target_table_type,
                        target_page_name=tmpl.target_page_name,
                        target_page_area=tmpl.target_page_area,
                        update_group=tmpl.update_group,
                        is_active=1,
                        related_month=related_month,
                        scan_quantity=0,
                        scan_failures=0,
                        update_quantity=0,
                        update_failures=0,
                        last_scan=None,
                        last_update=None,
                        hash=None,
                        values_json=None
                    )
                    new_tasks.append(new_task)
                    existing[tmpl.name_of_process] = new_task
                    created_count += 1
                    log_success(log_file, "define_tasks", tmpl.name_of_process, "created",
                                f"Задача '{tmpl.name_of_process}' создана.")
                except Exception as e:
                    log_error(log_file, "define_tasks", getattr(tmpl, 'name_of_process', None),
                            "fail", "Ошибка при создании задачи", exc=e)

        log_info(log_file, "define_tasks", None, "new_tasks", f"Создано новых задач: {created_count}")

        if new_tasks:
            try:
                session.bulk_save_objects(new_tasks, return_defaults=True)
                session.commit()
                log_success(log_file, "define_tasks", None, "saved",
                            f"📥 Сохранено новых задач: {len(new_tasks)}")
            except Exception as e:
                session.rollback()
                log_error(log_file, "define_tasks", None, "db_commit",
                        "Ошибка при сохранении новых задач", exc=e)

        # Шаг 4: фильтрация только активных
        active_tasks = session.query(SheetsInfo).filter(
            SheetsInfo.related_month == related_month,
            SheetsInfo.name_of_process.in_(template_names),
            SheetsInfo.is_active == 1
        ).order_by(SheetsInfo.name_of_process.asc()).all()
        log_info(log_file, "define_tasks", None, "active",
                f"✅ Количество активных задач: {len(active_tasks)}")

        # Шаг 5: отбор по интервалу + формирование Task
        tasks = []
        for task_obj in active_tasks:
            try:
                built_task = Task(task_obj.__dict__)

                if not built_task.is_ready_to_scan():
                    next_scan = built_task.last_scan + timedelta(seconds=built_task.scan_interval)
                    minutes_left = int((next_scan - now_time).total_seconds() / 60)
                    log_info(log_file, "define_tasks", built_task.name_of_process, "skip",
                            f"[⏳SKIP] Слишком рано. Next scan at: {next_scan:%Y-%m-%d %H:%M}, in {minutes_left} min.")
                    continue

                log_info(log_file, "define_tasks", built_task.name_of_process, "ready",
                        f"[✅READY] Task '{built_task.name_of_process} {built_task.source_page_name}' | "
                        f"Last scan: {built_task.last_scan} | "
                        f"Interval: {built_task.scan_interval // 60} min")

                tasks.append(built_task)

            except Exception as e:
                log_error(log_file, "define_tasks", getattr(task_obj, 'name_of_process', None),
                        "fail", "Ошибка при обработке задачи", exc=e)

        log_success(log_file, "define_tasks", None, "done",
                    f"✅ Готово. Всего задач к запуску: {len(tasks)}")
        return tasks

    except Exception as e:
        session.rollback()
        log_error(log_file, "define_tasks", None, "fatal",
                "Не удалось определить задачи", exc=e)
        raise

    finally:
        session.close()


def refresh_materialized_views(session, updated_groups: set, log_file=None):
    """Обновляет материализованные представления при наличии изменений."""

    from sqlalchemy import text
    views = {
        "update_qa_list_db": "mv_qa_list",
        "update_mistakes_in_db": "mv_mistakes",
        "feedback_status_update": "mv_feedbacks",
        "update_schedule_OT": "mv_schedule_ot",
    }

    for group_name in updated_groups:
        view_name = views.get(group_name)
        if view_name:
            try:
                session.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}"))
                log_info(log_file, "refresh_views", view_name, "success", f"🔄 Обновлён: {view_name}")
            except Exception as e:
                log_error(log_file, "refresh_views", view_name, "fail", f"❌ Ошибка при обновлении {view_name}: {e}")