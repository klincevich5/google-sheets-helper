from datetime import datetime

def log_to_file(filepath, message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} {message}\n")
