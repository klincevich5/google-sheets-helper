# database/session.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ Не задана переменная окружения SQLALCHEMY_DATABASE_URL")

try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    raise RuntimeError(f"❌ Ошибка при создании engine: {e}")

SessionLocal = sessionmaker(bind=engine)

@contextmanager
def get_session():
    """
    Контекстный менеджер для работы с сессией БД.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
