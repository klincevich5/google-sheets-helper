from sqlalchemy import MetaData
from database.session import engine
from database.db_models import MonitoringStorage

metadata = MetaData()
metadata.reflect(bind=engine)

print("🔁 Создание MonitoringStorage (новая таблица)")
MonitoringStorage.__table__.create(bind=engine, checkfirst=True)
print("✅ MonitoringStorage создана")
