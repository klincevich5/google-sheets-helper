from database.db_models import Base
from database.session import engine

# Создаёт таблицу QaList (и другие, если их нет) в базе данных
Base.metadata.create_all(bind=engine)
