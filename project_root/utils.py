from typing import List, Dict, Any, Optional
from googleapiclient.discovery import Resource

from logger import log_info, log_error
from notifier import send_telegram_message


def batch_get(
    service: Resource,
    spreadsheet_id: str,
    ranges: List[str],
    log_path: str
) -> List[Dict[str, Any]]:
    """Получить данные из Google Sheets с помощью batchGet."""
    try:
        result = service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            majorDimension="ROWS"
        ).execute()
        value_ranges = result.get("valueRanges", [])
        log_info(log_path, f"✅ batchGet: получено {len(value_ranges)} диапазонов.")
        return value_ranges
    except Exception as e:
        error_message = f"❌ Ошибка batchGet: {e}"
        log_error(log_path, error_message)
        send_telegram_message(log_path, error_message)
        return []


def batch_update(
    service: Resource,
    spreadsheet_id: str,
    batch_data: List[Dict[str, Any]],
    log_path: str
) -> bool:
    """Отправить данные в Google Sheets с помощью batchUpdate."""
    try:
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": batch_data
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        log_info(log_path, f"✅ batchUpdate: успешно обновлено {len(batch_data)} диапазонов.")
        return True
    except Exception as e:
        error_message = f"❌ Ошибка batchUpdate: {e}"
        log_error(log_path, error_message)
        send_telegram_message(log_path, error_message)
        return False
