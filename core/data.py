# core/data.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session
from sqlalchemy import text

from database.db_models import TrackedTables, TaskTemplate, RotationsInfo, SheetsInfo
from core.task_model import Task
from utils.logger import log_to_file, log_section
from core.config import TIMEZONE

try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

def return_tracked_tables(session: Session) -> dict:
    """Возвращает актуальные table_type -> spreadsheet_id из TrackedTables"""
    today = datetime.now(timezone).date()
    tables = session.query(TrackedTables).all()
    return {
        table.table_type: table.spreadsheet_id
        for table in tables
        if table.valid_from <= today <= table.valid_to
    }

def get_active_tabs(now=None):
    now = now or datetime.now(timezone)
    day = now.day
    hour = now.hour

    if 9 <= hour < 19:
        return [f"DAY {day}"]
    elif 19 <= hour < 21:
        return [f"DAY {day}", f"NIGHT {day}"]
    elif 21 <= hour <= 23:
        return [f"NIGHT {day}"]
    elif 0 <= hour < 7:
        return [f"NIGHT {(now - timedelta(days=1)).day}"]
    elif 7 <= hour < 9:
        return [f"DAY {day}", f"NIGHT {(now - timedelta(days=1)).day}"]
    return []

def parse_datetime(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return datetime.min.replace(tzinfo=timezone)
    if not value:
        return datetime.min.replace(tzinfo=timezone)
    if value.tzinfo is None or isinstance(value.tzinfo, str):
        return value.replace(tzinfo=timezone)
    return value

def build_task(row, now, source_table):
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

def load_rotationsinfo_tasks(session: Session, log_file):
    log_section("🔼 Фаза определения задач (RotationsInfo)", log_file)
    now = datetime.now(timezone)
    related_month = now.replace(day=1).date()
    active_tabs = get_active_tabs(now)

    log_to_file(log_file, f"🕒 Сейчас: {now}")
    log_to_file(log_file, f"📄 Активные смены: {active_tabs}")

    tasks = []
    templates = session.query(TaskTemplate).filter_by(source_table="RotationsInfo").all()
    template_names = [tmpl.name_of_process for tmpl in templates]
    log_to_file(log_file, f"📚 Найдено шаблонов: {len(templates)}")

    existing_tasks = session.query(RotationsInfo).filter(
        RotationsInfo.is_active == 1,
        RotationsInfo.related_month == related_month,
        RotationsInfo.source_page_name.in_(active_tabs)
    ).all()
    log_to_file(log_file, f"📦 Найдено активных задач: {len(existing_tasks)}")

    existing = {
        (t.name_of_process, t.source_page_name): t
        for t in existing_tasks
    }

    new_tasks = []
    for tmpl in templates:
        for tab in active_tabs:
            key = (tmpl.name_of_process, tab)
            if key not in existing:
                new_task = RotationsInfo(
                    name_of_process=tmpl.name_of_process,
                    source_table_type=tmpl.source_table_type,
                    source_page_name=tab,
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
                    update_failures=0
                )
                new_tasks.append(new_task)
                existing[key] = new_task
                log_to_file(log_file, f"[✅ CREATE] Задача '{tmpl.name_of_process}' для смены '{tab}' подготовлена.")

    if new_tasks:
        session.bulk_save_objects(new_tasks)
        session.commit()
        log_to_file(log_file, f"📥 Сохранено новых задач: {len(new_tasks)}")

    for task in existing.values():
        last_scan = parse_datetime(task.last_scan)
        next_scan = last_scan + timedelta(seconds=task.scan_interval)
        minutes_left = int((next_scan - now).total_seconds() / 60)

        log_to_file(log_file, (
            f"[✅READY] Task '{task.name_of_process} {task.source_page_name}' | "
            f"Last scan: {last_scan:%Y-%m-%d %H:%M:%S} | "
            f"Interval: {task.scan_interval // 60} min | "
            f"In: {minutes_left} min | "
            f"Next scan at: {next_scan:%Y-%m-%d %H:%M} | "
            f"Now: {now:%Y-%m-%d %H:%M:%S}"
        ))
        tasks.append(build_task(task, now, "RotationsInfo"))

    log_to_file(log_file, f"✅ Готово. Всего задач к запуску: {len(tasks)}")
    return tasks


def load_sheetsinfo_tasks(session: Session, log_file):
    log_section("🔼 Фаза определения задач (SheetsInfo)", log_file)
    now = datetime.now(timezone)
    related_month = now.replace(day=1).date()
    log_to_file(log_file, f"🕒 Сейчас: {now}")

    tasks = []
    templates = session.query(TaskTemplate).filter_by(source_table="SheetsInfo").all()
    template_names = [tmpl.name_of_process for tmpl in templates]
    log_to_file(log_file, f"📚 Найдено шаблонов: {len(templates)}")

    existing_tasks = session.query(SheetsInfo).filter(
        SheetsInfo.is_active == 1,
        SheetsInfo.related_month == related_month
    ).all()
    log_to_file(log_file, f"📦 Найдено активных задач: {len(existing_tasks)}")

    existing = {
        t.name_of_process: t
        for t in existing_tasks
    }

    new_tasks = []
    for tmpl in templates:
        if tmpl.name_of_process not in existing:
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
                update_failures=0
            )
            new_tasks.append(new_task)
            existing[tmpl.name_of_process] = new_task
            log_to_file(log_file, f"[✅ CREATE] Задача '{tmpl.name_of_process}' подготовлена.")

    if new_tasks:
        session.bulk_save_objects(new_tasks)
        session.commit()
        log_to_file(log_file, f"📥 Сохранено новых задач: {len(new_tasks)}")

    for task in existing.values():
        last_scan = parse_datetime(task.last_scan)
        next_scan = last_scan + timedelta(seconds=task.scan_interval)
        minutes_left = int((next_scan - now).total_seconds() / 60)

        log_to_file(log_file, (
            f"[✅READY] Task '{task.name_of_process} {task.source_page_name}' | "
            f"Last scan: {last_scan:%Y-%m-%d %H:%M:%S} | "
            f"Interval: {task.scan_interval // 60} min | "
            f"In: {minutes_left} min | "
            f"Next scan at: {next_scan:%Y-%m-%d %H:%M} | "
            f"Now: {now:%Y-%m-%d %H:%M:%S}"
        ))
        tasks.append(build_task(task, now, "SheetsInfo"))

    log_to_file(log_file, f"✅ Готово. Всего задач к запуску: {len(tasks)}")
    return tasks
