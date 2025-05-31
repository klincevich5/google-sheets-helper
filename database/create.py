from sqlalchemy import MetaData, select
from database.session import engine, SessionLocal
from database.db_models import SheetsInfo

metadata = MetaData()
metadata.reflect(bind=engine)

print("🔁 Копирование строк в SheetsInfo")

with SessionLocal() as session:
    rows = session.execute(select(SheetsInfo)).scalars().all()

    for row in rows:
        data = row.__dict__.copy()
        data.pop('_sa_instance_state', None)
        data.pop('id', None)
        new_row = SheetsInfo(**data)
        session.add(new_row)
    session.commit()

print("✅ Копии строк добавлены в SheetsInfo")