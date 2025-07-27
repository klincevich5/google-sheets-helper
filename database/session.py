import os
import threading
import traceback
import datetime
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ Environment variable SQLALCHEMY_DATABASE_URL is not set")

try:
    engine = create_engine(
        DATABASE_URL,
        pool_size=10,
        max_overflow=5,
        pool_timeout=30,
        pool_recycle=1800,
    )
except Exception as e:
    raise RuntimeError(f"❌ Error creating engine: {e}")

SessionLocal = sessionmaker(bind=engine)

# Словарь сессий по потокам
SESSION_STACK = {}


@contextmanager
def get_session():
    """
    Контекстный менеджер для работы с сессией SQLAlchemy.
    Использовать только через `with get_session() as session:`.
    """
    session = SessionLocal()
    thread_id = threading.get_ident()
    thread_name = threading.current_thread().name
    session_id = id(session)
    created_at = datetime.datetime.now()

    stack_info = {
        'id': session_id,
        'trace': ''.join(traceback.format_stack(limit=5)),
        'created_at': created_at,
        'thread_id': thread_id,
        'thread_name': thread_name
    }

    SESSION_STACK.setdefault(thread_id, []).append(stack_info)

    # print(f"🛜 [SESSION] Открыта сессия {session_id} в потоке '{thread_name}'")

    try:
        yield session
        session.commit()
    except Exception as exc:
        session.rollback()
        print(f"❗️❗️❗️ [SESSION] Ошибка в сессии {session_id}: {exc}")
        raise
    finally:
        SESSION_STACK[thread_id] = [
            s for s in SESSION_STACK.get(thread_id, [])
            if s['id'] != session_id
        ]
        if not SESSION_STACK[thread_id]:
            del SESSION_STACK[thread_id]
