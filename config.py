# config.py

import os
from dotenv import load_dotenv

# Загрузка переменных из .env файла
load_dotenv()

# Настройки базы данных и логов
DB_PATH = os.getenv("DB_PATH", "scheduler.db")

# Настройки логов
MAIN_LOG = os.getenv("MAIN_LOG", "scanner.log")
ROTATIONSINFO_LOG = os.getenv("ROTATIONSINFO_LOG", "scanner_rotationsinfo.log")
SHEETSINFO_LOG = os.getenv("SHEETSINFO_LOG", "scanner_sheetsinfo.log")

# Настройки Google API
TOKEN_PATH = os.getenv("TOKEN_PATH", "token.json")

# Настройки времени
TIMEZONE = os.getenv("TIMEZONE", "Europe/Warsaw")

# Время рефреша токена
REFRESH_TOKEN_TIME = os.getenv("REFRESH_TOKEN_TIME", 3600)  # 1 час по умолчанию
