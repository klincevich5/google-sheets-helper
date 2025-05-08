import os
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from core.config import ROTATIONSINFO_TOKEN_1
import sqlite3
from core.config import DB_PATH  # или укажи путь напрямую

def log_to_console(message):
    """Prints a message to the console."""
    print(message)

def load_credentials(token_path):
    """
    Загружает токен авторизации из указанного пути,
    при необходимости обновляет, и возвращает Google Sheets service.
    """
    creds = None
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"❌ Файл токена не найден: {token_path}")

    creds = Credentials.from_authorized_user_file(token_path)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            log_to_console(f"🔄 Токен в {token_path} был обновлен.")

            with open(token_path, "w", encoding="utf-8") as token_file:
                token_file.write(creds.to_json())
        except Exception as e:
            log_to_console(f"❌ Ошибка при обновлении токена {token_path}: {e}")
            raise

    if not creds or not creds.valid:
        raise RuntimeError(f"❌ Невалидный токен: {token_path}")

    service = build("sheets", "v4", credentials=creds)
    return service

def create_feedback_storage(DB_PATH):
    """Creates the FeedbackStorage table if it does not exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS FeedbackStorage (
            id INTEGER PRIMARY KEY,
            Date TEXT,
            Shift TEXT,
            Floor TEXT,
            Game TEXT,
            GP_Name_Surname TEXT,
            SM_Name_Surname TEXT,
            Reason TEXT,
            Total INTEGER,
            Proof TEXT,
            Explanation_of_the_reason TEXT,
            Action_taken TEXT,
            Forwarded_Feedback TEXT,
            Comment_after_forwarding TEXT
        )
    """)
    conn.commit()
    conn.close()

def insert_or_update_feedback(db_path, feedback):
    """
    Inserts a new feedback row or updates an existing one based on the id.
    :param db_path: Path to the SQLite database.
    :param feedback: List containing feedback data.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if the row with the same id already exists
    cursor.execute("SELECT id FROM FeedbackStorage WHERE id = ?", (feedback[0],))
    existing_row = cursor.fetchone()

    if existing_row:
        # Update the existing row
        cursor.execute("""
            UPDATE FeedbackStorage
            SET Date = ?, Shift = ?, Floor = ?, Game = ?, GP_Name_Surname = ?, SM_Name_Surname = ?,
                Reason = ?, Total = ?, Proof = ?, Explanation_of_the_reason = ?, Action_taken = ?,
                Forwarded_Feedback = ?, Comment_after_forwarding = ?
            WHERE id = ?
        """, feedback[1:] + [feedback[0]])
    else:
        # Insert a new row
        cursor.execute("""
            INSERT INTO FeedbackStorage (
                id, Date, Shift, Floor, Game, GP_Name_Surname, SM_Name_Surname, Reason, Total,
                Proof, Explanation_of_the_reason, Action_taken, Forwarded_Feedback, Comment_after_forwarding
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, feedback)

    conn.commit()
    conn.close()

def get_feedbacks():
    # Define paths
    token_path = ROTATIONSINFO_TOKEN_1  # Update with the actual token path

    # Initialize the Google Sheets API client
    service = load_credentials(token_path)

    # Specify the spreadsheet ID
    spreadsheet_id = "1DzSJqySS2J9GvuNcJKvNT00kJ8QBLYnUPV4frAhe7wU"

    # Fetch the spreadsheet metadata
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', [])

    # Print the sheet names
    for sheet in sheets:
        print(sheet.get("properties", {}).get("title"))

    # Fetch the first 5 rows of the "Feedbacks List" sheet
    sheet_name = "Feedbacks List(PASTE ONLY HERE)"  # Ensure this matches the exact name in the Google Sheets document
    range_name = f"'{sheet_name}'!A1:Z5"  # Enclose the sheet name in single quotes
    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        rows = result.get('values', [])

        print("\nFirst 5 rows of 'Feedbacks List':")
        for row in rows:
            print(len(row), row)
    except Exception as e:
        print(f"❌ Error fetching data from range {range_name}: {e}")

def shift_and_insert_sheetinfo_task():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Сдвигаем все id >= 17 на +1
        cursor.execute("""
            SELECT id FROM SheetsInfo
            WHERE id >= ?
            ORDER BY id DESC
        """, (17,))
        rows = cursor.fetchall()

        for (row_id,) in rows:
            cursor.execute("""
                UPDATE SheetsInfo
                SET id = ?
                WHERE id = ?
            """, (row_id + 1, row_id))

        # Вставляем новую запись с id = 17
        cursor.execute("""
            INSERT INTO SheetsInfo (
                id, is_active, name_of_process, source_table_type, source_page_name,
                source_page_area, scan_group, last_scan, scan_interval,
                scan_quantity, scan_failures, hash, process_data_method, values_json,
                target_table_type, target_page_name, target_page_area,
                update_group, last_update, update_quantity, update_failures
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            17, 1, 'feedbacks_review', 'feedbacks', 'Feedbacks List(PASTE ONLY HERE)',
            'A:N', 'feedbacks_review', None, 1800,
            0, 0, None, 'process_default', None,
            'feedbacks', 'Info', 'A1:B300',
            'feedback_status_update', None, 0, 0
        ))

        conn.commit()
        print("✅ Задача с id=17 успешно добавлена, предыдущие сдвинуты.")
    except Exception as e:
        print(f"❌ Ошибка при сдвиге или вставке: {e}")
    finally:
        conn.close()

# Вызов


if __name__ == "__main__":
    # db_path = "scheduler.db"
    # create_feedback_storage(db_path)

    # # Example feedback data
    # feedback = [
    #     '2', '01.05.2025', 'Day shift', 'VIP', 'SH', 'Hleb Semichau', 'Ramil Ibragimov',
    #     '❗Poor shuffle quality.', '7', 'YES',
    #     '⚠️ BAD SHUFFLE ALERT ⚠️\nShoe: 2025-05-0 01 03:26:09.719\nTable:hsbac-001\nRating: 1/10\nTime frame: 03:09:39 - 03:26:09\nClumps:6\nLocation:Poland',
    #     '', 'Nazar Shaulouski', ''
    # ]
    # insert_or_update_feedback(db_path, feedback)
    # get_feedbacks()
    create_feedback_storage(DB_PATH)