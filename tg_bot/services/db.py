# tg_bot/services/db.py

# Временно — имитация БД пользователей
MOCK_USERS = {
    111111111: "dealer",
    222222222: "service_manager",
    333333333: "architect",
}

async def get_user_role(user_id: int) -> str:
    return MOCK_USERS.get(user_id, "architect")  # default роль
