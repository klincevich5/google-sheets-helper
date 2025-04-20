import os
from zoneinfo import ZoneInfo
WARSAW_TZ = ZoneInfo("Europe/Warsaw")

DB_PATH = "scheduler.db"
LOG_DIR = "logs"
SCAN_INTERVAL_SECONDS = 10

SHEETS_LOG_FILE = os.path.join(LOG_DIR, "SheetsInfo_scanner_logs.txt")
ROTATIONS_LOG_FILE = os.path.join(LOG_DIR, "RotationInfo_scanner_logs.txt")

