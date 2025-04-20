# batch_tools.py 

from typing import List, Dict, Set
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from logger import log_to_file
from database import list_tracked_documents
from datetime import datetime
from config import WARSAW_TZ
import os


# Путь к сохранённому OAuth2-токену
TOKEN_PATH = "token.json"

def fetch_batch_values(spreadsheet_id: str, ranges: List[str]) -> Dict[str, List[List[str]]]:
    """
    Выполняет batchGet-запрос к Google Sheets API и возвращает values_map:
    {page_name: [[row1], [row2], ...]}

    Если страница запрашивается как 'DAY 1!D:D', то вернётся:
        {'DAY 1': [[...], [...], ...]}
    """
    if not os.path.exists(TOKEN_PATH):
        raise Exception("❌ Token.json не найден")

    creds = Credentials.from_authorized_user_file(TOKEN_PATH)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    try:
        result = sheet.values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            majorDimension="ROWS"
        ).execute()

        value_ranges = result.get("valueRanges", [])
        values_map = {}

        for entry in value_ranges:
            range_str = entry.get("range", "")  # Например: 'DAY 1!D:D'
            values = entry.get("values", [])
            if "!" in range_str:
                page = range_str.split("!")[0]
                values_map[page] = values

        return values_map

    except Exception as e:
        print(f"❌ Ошибка batchGet: {e}")
        return {}

def build_batchget_ranges(processes: List[dict], log_file) -> Dict[str, Set[str]]:
    """
    Принимает список процессов (например, из одной scan_group) и возвращает словарь:
    {spreadsheet_id: set(ranges)} — список всех диапазонов, которые нужно получить.

    get_default → используется area напрямую (например A1:B300)
    остальные методы → используем только колонку D (например D:D)
    """
    now_str = datetime.now(WARSAW_TZ).strftime("[%Y-%m-%d %H:%M:%S]")

    # 1. Строим карту table_type → spreadsheet_id
    type_to_doc_id = {}
    for table_type, _, doc_id in list_tracked_documents():
        type_to_doc_id[table_type] = doc_id

    doc_to_ranges: Dict[str, Set[str]] = {}

    for row in processes:
        doc_id = row.get("spreadsheet_id")
        if not doc_id:
            doc_id = type_to_doc_id.get(row.get("source_table_type"))

        page = row.get("source_page_name")
        method = row.get("get_data_method")
        area = row.get("source_page_area")

        if not doc_id or not page:
            if log_file:
                log_to_file(log_file, f"{now_str} ⚠️ Пропущен процесс без doc_id или page: ID={row.get('id')}")
            continue

        if method == "get_default":
            range_str = f"{page}!{area}"
        else:
            range_str = f"{page}!D:D"

        if log_file:
            log_to_file(
                log_file,
                f"{now_str} 📦 Обнаружен диапазон → ID={row.get('id')} | 🆔 {doc_id} | 📑 {page} | ⚙ {method} | 🔲 {range_str}"
            )

        doc_to_ranges.setdefault(doc_id, set()).add(range_str)

    return doc_to_ranges