from sqlalchemy.dialects.postgresql import insert
from database.session import get_session
# from database.db_models import MistakeStorage, FeedbackStorage, QaList
from typing import List, Dict
import json
from utils.logger import log_info, log_error


# 🔁 Словари для переименования полей из JSONB
MISTAKE_FIELDS_MAP = {
    "DATE": "related_date",
    "TIME": "event_time",
    "GAME ID": "game_id",
    "ERROR DESCRIPTION": "mistake",
    "ERROR TYPE": "mistake_type",
    "SYSTEM": "is_cancel",
    "DEALER": "dealer_name",
    "SM": "sm_name"
}

FEEDBACK_FIELDS_MAP = {
    "Nr": "feedback_nr",
    "Date": "related_date",
    "Game": "game",
    "Floor": "floor",
    "Proof": "proof",
    "Shift": "related_shift",
    "Total": "total",
    "Reason": "reason",
    "Action taken": "action_taken",
    "GP Name Surname": "dealer_name",
    "SM Name Surname": "sm_name",
    "Forwarded Feedback": "forwarded_feedback",
    "Explanation of the reason": "explanation_of_the_reason"
}

QA_FIELDS_MAP = {
    "name": "name",
    "VIP": "vip",
    "GENERIC": "generic",
    "LEGENDZ": "legendz",
    "TURKISH": "turkish",
    "GSBJ": "gsbj",
    "TRISTAR": "tristar",
    "Game Show": "gameshow",
    "Note": "qa_comment",
    "Male": "male",
    "BJ": "bj",
    "BC": "bc",
    "RL": "rl",
    "DT": "dt",
    "HSB": "hsb",
    "swBJ": "swbj",
    "swBC": "swbc",
    "swRL": "swrl",
    "SH": "sh",
    "gsDT": "gsdt",
    "TritonRL": "tritonrl",
    "RRR": "rrr"
}


def remap_keys(data: List[Dict], mapping: Dict[str, str]) -> List[Dict]:
    result = []
    for row in data:
        remapped = {}
        for key, value in row.items():
            if key in mapping:
                remapped[mapping[key]] = value
        result.append(remapped)
    return result


def deduplicate_by_keys(data: List[Dict], key_fields: List[str]) -> List[Dict]:
    seen = set()
    result = []
    for row in data:
        key = tuple(row.get(k) for k in key_fields)
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def filter_valid_rows(data: List[Dict], required_fields: List[str]) -> List[Dict]:
    return [row for row in data if all(field in row and row[field] for field in required_fields)]


def upsert_jsonb_records(
    session,
    data: List[Dict],
    model,
    conflict_keys: List[str],
    skip_cols: List[str] = None,
    log_file=None,
    phase=None,
    task=None
):
    if not data:
        log_info(log_file, phase or "upsert", task.name_of_process, None, f"Пустой список — пропуск для {model.__tablename__}")
        return

    skip_cols = set(skip_cols or [])
    table = model.__table__

    # Дедупликация и фильтрация пустых строк
    data = deduplicate_by_keys(data, conflict_keys)
    data = [row for row in data if any(row.values())]

    if not data:
        log_info(log_file, phase or "upsert", task.name_of_process, None, f"Все строки пустые или дубликаты — пропуск для {model.__tablename__}")
        return

    stmt = insert(table).values(data)
    update_dict = {
        col.name: stmt.excluded[col.name]
        for col in table.columns
        if col.name not in skip_cols and col.name not in conflict_keys
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=conflict_keys,
        set_=update_dict
    )

    try:
        session.execute(stmt)
        log_info(log_file, phase or "upsert", task.name_of_process, None, f"Вставлено/обновлено {len(data)} строк в {model.__tablename__}")
        session.commit()
    except Exception as e:
        log_error(log_file, phase or "upsert", task.name_of_process, None, f"UPSERT в {model.__tablename__} не удался", exc=e)
        session.rollback()
        raise


def safe_get_task_values(task):
    """
    Безопасно извлекает values_json из задачи, проверяя на корректность.
    """
    if isinstance(task, list):
        raise ValueError("Ожидался объект Task, но передан список.")
    values = task.values_json
    if isinstance(values, str):
        values = json.loads(values)
    if not isinstance(values, list):
        raise ValueError(f"Некорректный формат values_json: {type(values)}")
    return values


def import_mistakes_to_update(log_file, task, session):
    log_info(log_file, "update_mistakes_in_db", task.name_of_process, None, f"Обработка task.id={getattr(task, 'id', '?')}")
    try:
        values = safe_get_task_values(task)
        raw_data = remap_keys(values, MISTAKE_FIELDS_MAP)
        required = ["dealer_name", "related_date", "mistake"]
        valid_data = filter_valid_rows(raw_data, required)

        # upsert_jsonb_records(
        #     session=session,
        #     data=valid_data,
        #     model=MistakeStorage,
        #     conflict_keys=required,
        #     skip_cols=["id"],
        #     log_file=log_file,
        #     phase="update_mistakes_in_db",
        #     task=task
        # )
    except Exception as e:
        log_error(log_file, "update_mistakes_in_db", task.name_of_process, None, f"Ошибка при обработке ошибок task.id={getattr(task, 'id', '?')}", exc=e)


def import_feedbacks_to_update(log_file, task, session):
    log_info(log_file, "feedback_status_update", task.name_of_process, None, f"Обработка task.id={getattr(task, 'id', '?')}")
    try:
        values = safe_get_task_values(task)
    #     raw_data = remap_keys(values, FEEDBACK_FIELDS_MAP)
    #     required = ["dealer_name", "feedback_nr"]
    #     valid_data = filter_valid_rows(raw_data, required)

    #     upsert_jsonb_records(
    #         session=session,
    #         data=valid_data,
    #         model=FeedbackStorage,
    #         conflict_keys=required,
    #         skip_cols=["id"],
    #         log_file=log_file,
    #         phase="feedback_status_update",
    #         task=task
    #     )
    except Exception as e:
        log_error(log_file, "feedback_status_update", task.name_of_process, None, f"Ошибка при обработке feedback task.id={getattr(task, 'id', '?')}", exc=e)


def import_qa_list_to_update(log_file, task, session):
    log_info(log_file, "update_qa_list_db", task.name_of_process, None, f"Обработка task.id={getattr(task, 'id', '?')}")
    try:
        values = safe_get_task_values(task)
        # raw_data = remap_keys(values, QA_FIELDS_MAP)
        # required = ["name"]
        # valid_data = filter_valid_rows(raw_data, required)

        # upsert_jsonb_records(
        #     session=session,
        #     data=valid_data,
        #     model=QaList,
        #     conflict_keys=required,
        #     skip_cols=["id"],
        #     log_file=log_file,
        #     phase="update_qa_list_db",
        #     task=task
        # )
    except Exception as e:
        log_error(log_file, "update_qa_list_db", task.name_of_process, None, f"Ошибка при обработке QA List task.id={getattr(task, 'id', '?')}", exc=e)