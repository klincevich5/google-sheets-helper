# database/session.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ Environment variable SQLALCHEMY_DATABASE_URL is not set")

try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    raise RuntimeError(f"❌ Error creating engine: {e}")

SessionLocal = sessionmaker(bind=engine)

@contextmanager
def get_session():
    """
    Context manager for working with the database session.
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
