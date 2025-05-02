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

# --- БД и Google API ---
DB_PATH = os.getenv("DB_PATH", "scheduler.db")
TOKEN_PATH = os.getenv("TOKEN_PATH", "token.json")
REFRESH_TOKEN_TIME = int(os.getenv("REFRESH_TOKEN_TIME", 3600))

# --- Время ---
TIMEZONE = os.getenv("TIMEZONE", "Europe/Warsaw")
