# utils/utils.py

import time
import os
import json
import datetime

from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from core.time_provider import TimeProvider
from core.config import RETRIES, DELAY_SECONDS

from utils.logger import (
    log_info, log_success, log_warning, log_error, log_section, log_separator
)
# from utils.db_orm import insert_usage

##################################################################################
# Авторизация
##################################################################################

def load_credentials(token_path, log_file):
    """Загружает учетные данные из файла токена и выполняет авторизацию в Google Sheets API.

    Args:
        token_path (str): Путь к файлу токена.
        log_file (str): Путь к файлу журнала.

    Returns:
        service: Объект службы для взаимодействия с Google Sheets API.

    Raises:
        FileNotFoundError: Если файл токена не найден.
        RuntimeError: Если токен недействителен.
    """
    if not os.path.exists(token_path):
        log_error(log_file, "load_credentials", None, "file_not_found", f"❌ Файл токена не найден: {token_path}")
        raise FileNotFoundError(f"❌ Файл токена не найден: {token_path}")

    token_name = os.path.basename(token_path).replace("_token.json", "")

    try:
        with open(token_path, encoding="utf-8") as f:
            token = json.load(f)
    except json.JSONDecodeError as e:
        log_error(log_file, "load_credentials", None, "json_decode_error", f"❌ Ошибка чтения токена: {token_path}", exc=e)
        raise RuntimeError(f"❌ Ошибка чтения токена: {token_path}") from e

    try:
        creds = Credentials(
            token=token.get("access_token"),
            refresh_token=token.get("refresh_token"),
            token_uri=token.get("token_uri"),
            client_id=token.get("client_id"),
            client_secret=token.get("client_secret"),
            scopes=token.get("scopes", ["https://www.googleapis.com/auth/spreadsheets"])
        )
    except Exception as e:
        log_error(log_file, "load_credentials", None, "credentials_creation_error", f"❌ Ошибка создания объекта Credentials: {token_path}", exc=e)
        raise RuntimeError(f"❌ Ошибка создания объекта Credentials: {token_path}") from e

    # Логирование информации о токене
    log_info(log_file, "load_credentials", None, "token_info", f"🔍 Токен: {token.get('access_token')}")
    log_info(log_file, "load_credentials", None, "token_info", f"🔄 Refresh Token: {token.get('refresh_token')}")
    log_info(log_file, "load_credentials", None, "token_info", f"🌐 Token URI: {token.get('token_uri')}")
    log_info(log_file, "load_credentials", None, "token_info", f"🆔 Client ID: {token.get('client_id')}")
    log_info(log_file, "load_credentials", None, "token_info", f"🔑 Client Secret: {token.get('client_secret')}")
    log_info(log_file, "load_credentials", None, "token_info", f"📜 Scopes: {token.get('scopes')}")

    # Проверка срока действия токена
    # expiry = token.get("expiry")
    # if expiry:
    #     try:
    #         expiry_datetime = datetime.datetime.fromisoformat(expiry)
    #         time_left = (expiry_datetime - datetime.datetime.utcnow()).total_seconds()
    #         log_info(log_file, "load_credentials", None, "expiry_info", f"⏳ Время жизни токена: {time_left} секунд")
    #         if time_left <= 0:
    #             log_warning(log_file, "load_credentials", None, "token_expired", "🔄 Токен истёк. Требуется обновление.")
    #             creds.expired = True
    #     except ValueError as e:
    #         log_error(log_file, "load_credentials", None, "expiry_format_error", f"⚠️ Неверный формат поля expiry: {expiry}", exc=e)
    # else:
    #     log_warning(log_file, "load_credentials", None, "expiry_missing", "⏳ Время жизни токена: Неизвестно")

    # # Обновление токена, если он истёк
    # if creds.expired and creds.refresh_token:
    #     try:
    #         creds.refresh(Request())
    #         with open(token_path, "w", encoding="utf-8") as token_file:
    #             token_file.write(creds.to_json())
    #         log_success(log_file, "load_credentials", None, "token_refresh", f"🔄 Токен обновлён: {token_path}")
    #     except Exception as e:
    #         log_error(log_file, "load_credentials", None, "token_refresh_fail", f"❌ Ошибка обновления токена {token_path}", exc=e)
    #         raise RuntimeError(f"❌ Не удалось обновить токен: {token_path}") from e
    # elif creds.expired:
    #     raise RuntimeError(f"❌ Токен истёк и отсутствует refresh_token: {token_path}")

    # # Проверка валидности токена
    # if not creds.valid:
    #     log_error(log_file, "load_credentials", None, "invalid_token", f"❌ Недействительный токен: {token_path}")
    #     raise RuntimeError(f"❌ Недействительный токен: {token_path}")

    # Создание службы Google Sheets API
    try:
        service = build("sheets", "v4", credentials=creds)
        log_success(log_file, "load_credentials", None, "auth", f"✅ Авторизация выполнена: {token_name}")
    except Exception as e:
        log_error(log_file, "load_credentials", None, "auth_fail", f"❌ Ошибка авторизации: {token_name}", exc=e)
        raise RuntimeError(f"❌ Ошибка создания службы Google Sheets API: {token_name}") from e

    return service

##################################################################################
# Проверка существования листа
##################################################################################

def check_sheet_exists(service, spreadsheet_id, sheet_name, log_file, token_name):
    """Проверяет, существует ли лист с заданным именем в таблице Google Sheets.

    Args:
        service: Объект службы для взаимодействия с Google Sheets API.
        spreadsheet_id (str): ID таблицы.
        sheet_name (str): Имя листа для проверки.
        log_file (str): Путь к файлу журнала.
        token_name (str): Имя файла токена.

    Returns:
        bool: True, если лист существует, иначе False.
    """
    success = False
    try:
        metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = metadata.get('sheets', [])
        for sheet in sheets:
            title = sheet.get('properties', {}).get('title')
            if title == sheet_name:
                success = True
                return True
        return False

    except Exception as e:
        log_error(log_file, "check_sheet_exists", None, "fail", f"❌ Ошибка при проверке {sheet_name}", exc=e)
        return False

    finally:
        token_name = os.path.basename(token_name).replace("_token.json", "")
 
##################################################################################
# Получение данных из Google Sheets
##################################################################################

def batch_get(service, spreadsheet_id, ranges, scan_group, log_file, token_name, retries=RETRIES, delay_seconds=DELAY_SECONDS):
    """Получает данные из указанных диапазонов таблицы Google Sheets.

    Args:
        service: Объект службы для взаимодействия с Google Sheets API.
        spreadsheet_id (str): ID таблицы.
        ranges (list): Список диапазонов для получения данных.
        scan_group: ?
        log_file (str): Путь к файлу журнала.
        token_name (str): Имя файла токена.
        retries (int): Количество попыток в случае ошибки (по умолчанию из конфигурации).
        delay_seconds (int): Задержка между попытками в секундах (по умолчанию из конфигурации).

    Returns:
        dict: Словарь с полученными данными, где ключи - это диапазоны, а значения - списки строк данных.
    """
    attempt = 0
    success = False
    data = {}

    while attempt < retries:
        try:
            response = service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges,
                majorDimension="ROWS"
            ).execute()

            value_ranges = response.get("valueRanges", [])
            if not value_ranges:
                log_warning(log_file, "batch_get", None, "empty", "⚠️ batchGet вернул пустые valueRanges.")
                attempt += 1
                time.sleep(delay_seconds)
                continue

            data = {vr.get("range", ""): vr.get("values", []) for vr in value_ranges}
            success = True
            break

        except HttpError as e:
            status_code = e.resp.status
            log_error(log_file, "batch_get", None, "http_error", f"❌ HttpError {status_code} при batchGet", exc=e)
            if status_code in (429, 500, 503):
                attempt += 1
                time.sleep(delay_seconds)
            elif status_code == 401:
                break
            else:
                break

        except Exception as e:
            msg = str(e).lower()
            if any(term in msg for term in ["ssl", "handshake", "decryption", "timed out"]):
                attempt += 1
                time.sleep(delay_seconds)
            else:
                log_error(log_file, "batch_get", None, "fail", f"❌ Ошибка batchGet", exc=e)
                break

    return data if success else {}

##################################################################################
# Обновление данных в Google Sheets
##################################################################################

def batch_update(service, spreadsheet_id, batch_data, token_name, update_group, log_file, retries=RETRIES, delay_seconds=DELAY_SECONDS):
    """Обновляет данные в таблице Google Sheets, записывая их в указанные диапазоны.

    Args:
        service: Объект службы для взаимодействия с Google Sheets API.
        spreadsheet_id (str): ID таблицы.
        batch_data (list): Список данных для записи в таблицу.
        token_name (str): Имя файла токена.
        update_group: ?
        log_file (str): Путь к файлу журнала.
        retries (int): Количество попыток в случае ошибки (по умолчанию из конфигурации).
        delay_seconds (int): Задержка между попытками в секундах (по умолчанию из конфигурации).

    Returns:
        tuple: Кортеж из двух элементов, где первый - булево значение успеха операции,
               а второй - сообщение об ошибке или None в случае успеха.
    """
    success = False
    attempt = 0

    token_name = os.path.basename(token_name).replace("_token.json", "")

    # Предварительная очистка
    try:
        clear_ranges = [entry["range"] for entry in batch_data if "range" in entry]
        if clear_ranges:
            service.spreadsheets().values().batchClear(
                spreadsheetId=spreadsheet_id,
                body={"ranges": clear_ranges}
            ).execute()
    except Exception as e:
        log_warning(log_file, "batch_update", None, "clear_fail", f"⚠️ Ошибка очистки диапазонов", message=str(e))

    # Попытки записи
    while attempt < retries:
        try:
            response = service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": batch_data
                }
            ).execute()

            if response and 'responses' in response:
                success = True
                break
            else:
                log_warning(log_file, "batch_update", None, "empty", "⚠️ batchUpdate вернул пустой или некорректный ответ.")
                attempt += 1
                time.sleep(delay_seconds)

        except HttpError as e:
            status = e.resp.status
            log_error(log_file, "batch_update", None, "http_error", f"❌ HttpError {status} при batchUpdate", exc=e)
            if status in [429, 500, 503]:
                attempt += 1
                time.sleep(delay_seconds)
            elif status == 401:
                break
            else:
                break

        except Exception as e:
            log_error(log_file, "batch_update", None, "fail", f"❌ Ошибка batchUpdate", exc=e)
            break
    
    return (True, None) if success else (False, "Превышено число попыток" if attempt == retries else "Ошибка запроса")
