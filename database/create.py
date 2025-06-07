from database.db_models import Base
from database.session import engine

Base.metadata.create_all(bind=engine)
