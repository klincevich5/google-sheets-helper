# bot/settings_access.py

from sqlalchemy import select
from database.session import SessionLocal
from database.db_models import BotSettings


def is_scanner_enabled(scanner_name: str) -> bool:
    """
    Проверяет, включён ли сканер. По умолчанию — включён, если записи нет.
    """
    with SessionLocal() as session:
        result = session.execute(
            select(BotSettings.value).where(BotSettings.key == scanner_name)
        ).scalar_one_or_none()
        return result is None or result == '1'


def set_scanner_enabled(scanner_name: str, enabled: bool):
    """
    Устанавливает флаг включения или отключения сканера.
    """
    with SessionLocal() as session:
        existing = session.query(BotSettings).filter_by(key=scanner_name).first()

        if existing:
            existing.value = '1' if enabled else '0'
        else:
            session.add(BotSettings(key=scanner_name, value='1' if enabled else '0'))

        session.commit()


def ensure_bot_settings_table():
    """
    Создаёт таблицу BotSettings, если её нет. (для SQLite устарело, для Alembic не нужно)
    """
    from database.db_models import Base
    from database.session import engine
    Base.metadata.create_all(bind=engine)
