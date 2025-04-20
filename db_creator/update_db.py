import sqlite3
import pandas as pd

# Путь к SQLite-базе
db_path = "scheduler.db"
conn = sqlite3.connect(db_path)

# Загружаем TrackedTables.csv
try:
    df_tracked = pd.read_csv("TrackedTables.csv")
    df_tracked.to_sql("TrackedTables", conn, if_exists="replace", index=False)
    print(f"✅ TrackedTables загружена: {len(df_tracked)} строк")
except Exception as e:
    print(f"⚠️ Ошибка при загрузке TrackedTables: {e}")

# Загружаем SheetsInfo.csv
try:
    df_sheets = pd.read_csv("SheetsInfo.csv")
    df_sheets.to_sql("SheetsInfo", conn, if_exists="replace", index=False)
    print(f"✅ SheetsInfo загружена: {len(df_sheets)} строк")
except Exception as e:
    print(f"⚠️ Ошибка при загрузке SheetsInfo: {e}")

# Загружаем RotationsInfo.csv
try:
    df_rotations = pd.read_csv("RotationsInfo.csv")
    df_rotations.to_sql("RotationsInfo", conn, if_exists="replace", index=False)
    print(f"✅ RotationsInfo загружена: {len(df_rotations)} строк")
except Exception as e:
    print(f"⚠️ Ошибка при загрузке RotationsInfo: {e}")

conn.commit()
conn.close()
print("🎉 Все данные успешно загружены в базу данных.")
