# bot/db_access.py

from sqlalchemy.orm import Session
from sqlalchemy import func, select
from database.db_models import RotationsInfo, SheetsInfo, TrackedTables
from database.session import SessionLocal


def get_rotations_stats(session: Session = None):
    session = session or SessionLocal()
    total = session.query(func.count(RotationsInfo.id)).scalar()
    errors = session.query(func.count(RotationsInfo.id)).filter(RotationsInfo.scan_failures > 0).scalar()
    return {"total": total, "errors": errors}


def get_rotations_tasks_by_tab(tab_name, session: Session = None):
    session = session or SessionLocal()
    tasks = session.query(RotationsInfo.id, RotationsInfo.name_of_process).filter(
        RotationsInfo.source_page_name == tab_name
    ).order_by(RotationsInfo.name_of_process).all()

    return [{"id": t[0], "name": t[1]} for t in tasks]


def get_task_by_id(task_id, session: Session = None):
    session = session or SessionLocal()
    task = session.query(RotationsInfo).filter_by(id=task_id).first()
    if task:
        return {
            "name": task.name_of_process,
            "source": f"{task.source_page_name}!{task.source_page_area}",
            "hash": task.hash,
            "last_scan": task.last_scan or "—",
            "last_update": task.last_update or "—",
            "scan_quantity": task.scan_quantity,
            "update_quantity": task.update_quantity,
            "scan_failures": task.scan_failures,
            "update_failures": task.update_failures,
            "scan_interval": task.scan_interval
        }
    return None


def get_sheets_tasks(session: Session = None):
    session = session or SessionLocal()
    rows = session.query(SheetsInfo).order_by(SheetsInfo.name_of_process).all()

    return [
        {
            "id": row.id,
            "name": row.name_of_process,
            "source": f"{row.source_page_name}!{row.source_page_area}",
            "target": f"{row.target_page_name}!{row.target_page_area}",
            "failures": row.scan_failures,
            "hash": row.hash,
            "last_scan": row.last_scan or "—",
            "last_update": row.last_update or "—"
        } for row in rows
    ]


def get_sheet_by_id(sheet_id, session: Session = None):
    session = session or SessionLocal()
    row = session.query(SheetsInfo).filter_by(id=sheet_id).first()

    if row:
        return {
            "name": row.name_of_process,
            "source": f"{row.source_page_name}!{row.source_page_area}",
            "hash": row.hash,
            "last_scan": row.last_scan or "—",
            "last_update": row.last_update or "—",
            "scan_quantity": row.scan_quantity,
            "update_quantity": row.update_quantity,
            "scan_failures": row.scan_failures,
            "update_failures": row.update_failures,
            "scan_interval": row.scan_interval
        }
    return None


def get_all_tracked_tables(session: Session = None):
    session = session or SessionLocal()
    return session.query(TrackedTables).all()


def get_sheets_stats(session: Session = None):
    session = session or SessionLocal()
    total = session.query(func.count(SheetsInfo.id)).scalar()
    errors = session.query(func.count(SheetsInfo.id)).filter(SheetsInfo.scan_failures > 0).scalar()
    return {"total": total, "errors": errors}


def get_top_error_tasks(source="rotations", limit=5, session: Session = None):
    session = session or SessionLocal()
    model = RotationsInfo if source == "rotations" else SheetsInfo

    rows = session.query(
        model.name_of_process,
        model.scan_quantity,
        model.scan_failures
    ).filter(model.scan_failures > 0).order_by(model.scan_failures.desc()).limit(limit).all()

    return [
        {
            "name": row[0],
            "ok": row[1] - row[2],
            "fail": row[2]
        } for row in rows
    ]
