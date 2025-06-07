from datetime import datetime
from sqlalchemy.orm import Session
from database.session import SessionLocal
from database.db_models import MonitoringStorage, QaList, Base

from database.session import engine

print("🚧 Создание таблицы QaList...")
Base.metadata.create_all(bind=engine, tables=[QaList.__table__])
print("✅ Таблица QaList успешно создана.")


# now = datetime.now()
# related_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

# def transfer_unique_dealers():
#     with SessionLocal() as session:
#         # 1. Собираем уникальные пары
#         unique_dealers = (
#             session.query(
#                 MonitoringStorage.dealer_name,
#                 MonitoringStorage.dealer_nicknames
#             )
#             .distinct()
#             .all()
#         )

#         count = 0

#         for dealer_name, nicknames in unique_dealers:
#             # Проверка: уже есть в QaList?
#             exists = session.query(QaList).filter_by(
#                 dealer_name=dealer_name,
#                 related_month=related_month
#             ).first()

#             if not exists:
#                 new_entry = QaList(
#                     dealer_name=dealer_name,
#                     dealer_nicknames=nicknames or [],
#                     related_month=related_month,
#                     # поля по умолчанию:
#                     schedule=False,
#                     bonus=False,
#                     qa_list=False,
#                     feedback_status=False
#                 )
#                 session.add(new_entry)
#                 count += 1

#         session.commit()
#         print(f"✅ Добавлено {count} новых записей в QaList")

# if __name__ == "__main__":
#     transfer_unique_dealers()
