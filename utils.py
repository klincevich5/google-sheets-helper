import json
import hashlib
from collections import defaultdict
from googleapiclient.errors import HttpError
from logger import log_to_file
from config import SHEETS_LOG_FILE
from methods import proc_func

def process_data_by_method(method, values):
    processed = proc_func(method, values)
    values_json = json.dumps(processed, ensure_ascii=False)
    new_hash = hashlib.md5(values_json.encode()).hexdigest()
    return values_json, new_hash

def filter_valid_tasks(tasks, doc_id_map):
    valid = []
    for t in tasks:
        source_id = doc_id_map.get(t.get("source_table_type"))
        target_id = doc_id_map.get(t.get("target_table_type"))
        if source_id and target_id:
            t["source_doc_id"] = source_id
            t["target_doc_id"] = target_id
            valid.append(t)
        else:
            log_to_file(SHEETS_LOG_FILE, f"⚠️ Пропуск задачи {t.get('id')} без doc_id")
    return valid

def get_group_ranges(group_tasks):
    mapping = defaultdict(list)
    for task in group_tasks:
        key = f"'{task['source_page_name']}'!{task['source_page_area']}"
        mapping[key].append(task)
    return mapping

def fetch_data_from_sheet(sheet, doc_id, ranges):
    try:
        result = sheet.values().batchGet(spreadsheetId=doc_id, ranges=ranges).execute()
        return result.get("valueRanges", [])
    except HttpError as e:
        log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка получения данных из {doc_id}: {str(e)}")
        return None
