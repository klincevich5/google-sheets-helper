# core/config.py

import os
import json
from dotenv import load_dotenv

# Загружаем .env до вызова os.getenv()ующих переменных окружения
load_dotenv(override=True)

# --- Telegram ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
AUTHORIZED_USERS = [
    int(uid.strip()) for uid in os.getenv("AUTHORIZED_USERS", "").split(",") if uid.strip().isdigit()
]

# --- Пути к логам ---
MAIN_LOG = os.getenv("MAIN_LOG", "logs/scanner.log")
ROTATIONSINFO_LOG = os.getenv("ROTATIONSINFO_LOG", "logs/scanner_rotationsinfo.log")
SHEETSINFO_LOG = os.getenv("SHEETSINFO_LOG", "logs/scanner_sheetsinfo.log")
MONITORING_LOG = os.getenv("MONITORING_LOG", "logs/scanner_monitoring.log")

# --- Параметры Google API ---
SHEETSINFO_TOKEN = os.getenv("SHEETSINFO_TOKEN", "tokens/current/SheetsInfo_scanner_1_token.json")
ROTATIONSINFO_TOKEN_1 = os.getenv("ROTATIONSINFO_TOKEN_1", "tokens/current/RotationsInfo_scanner_1_token.json")
ROTATIONSINFO_TOKEN_2 = os.getenv("ROTATIONSINFO_TOKEN_2", "tokens/current/RotationsInfo_scanner_2_token.json")

# --- Прочее ---
REFRESH_TOKEN_TIME = int(os.getenv("REFRESH_TOKEN_TIME", 3600))
API_LIMIT_PER_DAY = int(os.getenv("API_LIMIT_PER_DAY", 10000))
THRESHOLD = int(os.getenv("THRESHOLD", 9000))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Warsaw")
SHEETINFO_INTERVAL = int(os.getenv("SHEETINFO_INTERVAL", 300))
ROTATIONSINFO_INTERVAL = int(os.getenv("ROTATIONSINFO_INTERVAL", 60))

# --- sqalchemy ---
SQLALCHEMY_DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL", "postgresql+psycopg2://postgres:qweqwe@localhost:5432/scheduler")

# --- Deviding by floors ---
FLOORS = json.loads(os.getenv("FLOORS", "{}"))

try:
    ROTATION_ORDER = json.loads(os.getenv("ROTATION_ORDER"))
except Exception:
    ROTATION_ORDER = ["SHUFFLE Main", "VIP Main", "TURKISH Main", "GENERIC Main", "GSBJ Main", "LEGENDZ Main", "TRI-STAR Main", "TritonRL Main"]