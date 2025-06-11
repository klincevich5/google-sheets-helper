# tg_bot/services/db.py

from sqlalchemy.future import select

from database.db_models import User, UserStatus
from database.session import get_session


async def get_user_role(user_id: int) -> str:
    with get_session() as session:
        user = session.scalar(
            select(User).where(User.telegram_id == user_id)
        )
        if user and user.role:
            return user.role.value.lower()
        return "stranger"  # default role

async def get_or_create_user(telegram_id: int, dealer_name: str = None) -> dict:
    with get_session() as session:
        from database.db_models import UserRole, UserStatus
        user = session.scalar(
            select(User).where(User.telegram_id == telegram_id)
        )
        if not user:
            user = User(
                telegram_id=telegram_id,
                dealer_name=dealer_name or f"user_{telegram_id}",
                role=UserRole.stranger,
                status=UserStatus.requested_access
            )
            session.add(user)
            session.commit()
            session.refresh(user)
        # Save required values before closing the session
        user_data = {
            "id": user.id,
            "telegram_id": user.telegram_id,
            "dealer_name": user.dealer_name,
            "role": user.role.value if user.role else None,
            "status": user.status.value if user.status else None,
            "photo_fileID": user.photo_fileID,
            "approved_by_id": user.approved_by_id,
            "approved_at": user.approved_at,
        }
        return user_data

# Импорты сервисов, связанных с БД, статистикой, отчетами и т.д., должны быть только из tg_bot/services/.
# Проверить, что сервисы не зависят от Telegram.
