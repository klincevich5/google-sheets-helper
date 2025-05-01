
from collections import defaultdict
from googleapiclient.errors import HttpError
from logger import log_to_file

import json
import hashlib
from registry import proc_func


def process_data_by_method(method: str, values: list, log_file) -> tuple[str, str]:
    try:
        processed_values = proc_func(method, values)
        values_json = json.dumps(processed_values, ensure_ascii=False, default=str)
        values_hash = hashlib.md5(values_json.encode("utf-8")).hexdigest()
        return values_json, values_hash
    except Exception as e:
        log_to_file(log_file, f"❌ Ошибка обработки методом '{method}': {str(e)}")
        return "[]", ""


def filter_valid_tasks(tasks, doc_id_map, log_file):
    valid = []
    for t in tasks:
        source_id = doc_id_map.get(t.get("source_table_type"))
        target_id = doc_id_map.get(t.get("target_table_type"))
        if source_id and target_id:
            t["source_doc_id"] = source_id
            t["target_doc_id"] = target_id
            valid.append(t)
        else:
            log_to_file(log_file, f"⚠️ Пропуск задачи {t.get('id')} без doc_id")
    return valid

def get_group_ranges(group_tasks, log_file):
    mapping = defaultdict(list)
    for task in group_tasks:
        key = f"'{task['source_page_name']}'!{task['source_page_area']}"
        mapping[key].append(task)
    return mapping

def fetch_data_from_sheet(sheet, doc_id, ranges, log_file):
    try:
        result = sheet.values().batchGet(spreadsheetId=doc_id, ranges=ranges).execute()
        return result.get("valueRanges", [])
    except HttpError as e:
        log_to_file(log_file, f"❌ Ошибка получения данных из {doc_id}: {str(e)}")
        return None
