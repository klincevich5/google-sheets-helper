from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from config import SHEETS_LOG_FILE
from logger import log_to_file

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def load_sheet_api():
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
        service = build("sheets", "v4", credentials=creds)
        return service.spreadsheets()
    except Exception as e:
        log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка авторизации: {str(e)}")
        return None


def batch_get_ranges(sheet, spreadsheet_id, ranges):
    """
    Получить значения из Google Sheets с помощью batchGet.
    Возвращает список valueRanges, соответствующих запрошенным ranges.
    """
    try:
        response = sheet.values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            majorDimension="ROWS"
        ).execute()
        return [r.get("values", []) for r in response.get("valueRanges", [])]
    except Exception as e:
        log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка batchGet: {str(e)}")
        return [[] for _ in ranges]


def batch_update_ranges(sheet, spreadsheet_id, data):
    """
    Обновить значения в Google Sheets с помощью batchUpdate.
    Аргумент `data` должен содержать список словарей:
    [{ "range": ..., "values": ... }, ...]
    """
    try:
        sheet.values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data}
        ).execute()
    except Exception as e:
        log_to_file(SHEETS_LOG_FILE, f"❌ Ошибка batchUpdate: {str(e)}")
