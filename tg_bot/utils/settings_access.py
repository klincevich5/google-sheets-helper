# tg_bot.utils.settings_access.py

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

