import os
from dotenv import load_dotenv

load_dotenv()

# --- Telegram ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
AUTHORIZED_USERS = [
    int(uid.strip()) for uid in os.getenv("AUTHORIZED_USERS", "").split(",") if uid.strip().isdigit()
]

# --- Пути к логам ---
MAIN_LOG = os.getenv("MAIN_LOG", "logs/scanner.log")
ROTATIONSINFO_LOG = os.getenv("ROTATIONSINFO_LOG", "logs/scanner_rotationsinfo.log")
SHEETSINFO_LOG = os.getenv("SHEETSINFO_LOG", "logs/scanner_sheetsinfo.log")

# --- БД ---
DB_PATH = os.getenv("DB_PATH", "scheduler.db")


# --- Параметры Google API ---
# TOKEN_PATH = os.getenv("TOKEN_PATH", "token.json")
SHEETSINFO_TOKEN = os.getenv("SHEETSINFO_TOKEN", "tokens/SheetsInfo_scanner_1_token.json")
ROTATIONSINFO_TOKEN_1 = os.getenv("ROTATIONSINFO_TOKEN_1", "tokens/RotationsInfo_scanner_1_token.json")
ROTATIONSINFO_TOKEN_2 = os.getenv("ROTATIONSINFO_TOKEN_2", "tokens/RotationsInfo_scanner_2_token.json")

REFRESH_TOKEN_TIME = int(os.getenv("REFRESH_TOKEN_TIME", 3600))
API_LIMIT_PER_DAY = int(os.getenv("API_LIMIT_PER_DAY", 10000))
THRESHOLD = int(os.getenv("THRESHOLD", 9000))

# --- Время ---
TIMEZONE = os.getenv("TIMEZONE", "Europe/Warsaw")
