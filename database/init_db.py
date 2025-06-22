# database/init_db.py

from database.db_models import Base
from database.session import engine

def create_all_tables_if_not_exist():

    Base.metadata.create_all(bind=engine)
