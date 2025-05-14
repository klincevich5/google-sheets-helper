from sqlalchemy import MetaData, Table, select
from datetime import datetime
from zoneinfo import ZoneInfo

from database.session import engine, SessionLocal
from database.db_models import (
    Base,
    ApiUsage,
    FeedbackStorage,
    MistakeStorage,
    RotationsInfo,
    SheetsInfo,
)

TZ = ZoneInfo("Europe/Warsaw")
metadata = MetaData()
metadata.reflect(bind=engine)

# Таблицы: что делать
RECREATE_WITHOUT_DATA = {
    "ApiUsage": ApiUsage,
    "FeedbackStorage": FeedbackStorage,
}

RECREATE_WITH_DATA = {
    "MistakeStorage": MistakeStorage,
    "RotationsInfo": RotationsInfo,
    "SheetsInfo": SheetsInfo,
}

# Парсинг даты d.m.Y
def parse_date(d):
    try:
        if isinstance(d, str):
            return datetime.strptime(d, "%d.%m.%Y").date()
        return d
    except Exception:
        return None

# ISO datetime + таймзона
def parse_datetime(dt):
    try:
        if isinstance(dt, str):
            return datetime.fromisoformat(dt).astimezone(TZ)
        return dt.astimezone(TZ)
    except Exception:
        return None

# === УДАЛЯЕМ + СОЗДАЁМ пустые таблицы ===
for name, model in RECREATE_WITHOUT_DATA.items():
    print(f"🔁 Пересоздание {name} (без данных)")
    model.__table__.drop(bind=engine, checkfirst=True)
    Base.metadata.create_all(bind=engine, tables=[model.__table__])

# === МИГРИРУЕМ данные ===
for name, model in RECREATE_WITH_DATA.items():
    print(f"🔁 Обработка {name} (с переносом данных)")
    old_table = Table(name, metadata, autoload_with=engine)

    with engine.connect() as conn:
        raw_data = conn.execute(select(old_table)).fetchall()

    # Пересоздание таблицы
    model.__table__.drop(bind=engine, checkfirst=True)
    Base.metadata.create_all(bind=engine, tables=[model.__table__])

    with SessionLocal() as session:
        for row in raw_data:
            row_dict = dict(row._mapping)

            if name == "MistakeStorage":
                row_dict["date"] = parse_date(row_dict.get("date"))

            elif name in {"RotationsInfo", "SheetsInfo"}:
                row_dict["related_month"] = parse_date(row_dict.get("related_month"))
                row_dict["last_scan"] = parse_datetime(row_dict.get("last_scan"))
                row_dict["last_update"] = parse_datetime(row_dict.get("last_update"))

            session.add(model(**row_dict))

        session.commit()
        print(f"✅ {name} пересоздана и данные сохранены")
