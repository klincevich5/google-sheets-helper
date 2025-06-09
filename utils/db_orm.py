# utils/db_orm.py

import json
from sqlalchemy.orm import Session
from sqlalchemy import func
from database.db_models import SheetsInfo, RotationsInfo, MistakeStorage
from core.timezone import timezone

MODEL_MAP = {
    "SheetsInfo": SheetsInfo,
    "RotationsInfo": RotationsInfo,
}

def get_model_by_table_name(table_name: str):
    model = MODEL_MAP.get(table_name)
    if model is None:
        raise ValueError(f"Модель для таблицы '{table_name}' не найдена.")
    return model


def update_task_scan_fields(session: Session, task, log_file=None, table_name: str = "SheetsInfo"):
    model = get_model_by_table_name(table_name)
    session.query(model).filter(model.id == task.id).update({
        "last_scan": task.last_scan.isoformat() if task.last_scan else None,
        "scan_quantity": task.scan_quantity,
        "scan_failures": task.scan_failures
    })
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def update_task_process_fields(session: Session, task, log_file=None, table_name: str = "SheetsInfo"):
    model = get_model_by_table_name(table_name)
    session.query(model).filter(model.id == task.id).update({
        "hash": task.hash,
        "values_json": json.dumps(task.values_json) if task.values_json else None
    })
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def update_task_update_fields(session: Session, task, log_file=None, table_name: str = "SheetsInfo"):
    model = get_model_by_table_name(table_name)
    session.query(model).filter(model.id == task.id).update({
        "last_update": task.last_update.isoformat() if task.last_update else None,
        "update_quantity": task.update_quantity,
        "update_failures": task.update_failures
    })
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def get_max_last_row(session: Session, table_name: str) -> int:
    """
    Возвращает максимальный last_row из MistakeStorage для заданной таблицы.
    """
    if table_name is None:
        raise ValueError("table_name не может быть пустым.")

    max_row = session.query(func.max(MistakeStorage.last_row))\
        .filter(MistakeStorage.table_name == table_name)\
        .scalar()

    return max_row if max_row is not None else 0