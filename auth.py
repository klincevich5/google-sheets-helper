from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from config import TOKEN_PATH

def load_credentials():
    creds = Credentials.from_authorized_user_file(TOKEN_PATH)
    service = build("sheets", "v4", credentials=creds)
    return service.spreadsheets()
