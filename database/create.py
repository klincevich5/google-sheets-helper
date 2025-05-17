from sqlalchemy import MetaData
from database.session import engine
from database.db_models import NameStatusStorage

metadata = MetaData()
metadata.reflect(bind=engine)

print("🔁 Создание FeedbackStatus (новая таблица)")
NameStatusStorage.__table__.create(bind=engine, checkfirst=True)
print("✅ FeedbackStatus создана")
