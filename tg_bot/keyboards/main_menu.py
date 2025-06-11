# tg_bot/keyboards/main_menu.py

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# --- Access level mapping ---
ROLE_ACCESS_LEVEL = {
    "stranger": 0,
    "shuffler": 1,
    "dealer": 1,
    "manager": 2,
    "qa_manager": 2,
    "hr_manager": 2,
    "chief_sm_manager": 2,
    "trainer_manager": 2,
    "floor_manager": 2,
    "admin": 2,
    "architect": 3,
}

def get_access_level(role: str) -> int:
    return ROLE_ACCESS_LEVEL.get(role.lower().replace(" ", "_"), 0)

# --- Универсальный селектор клавиатуры ---
def get_main_menu_keyboard_by_role(role: str):
    from tg_bot.keyboards.dealer import get_dealer_keyboard
    from tg_bot.keyboards.manager import get_manager_keyboard
    from tg_bot.keyboards.architect import get_architect_keyboard
    level = get_access_level(role)
    if level == 1:
        return get_dealer_keyboard()
    elif level == 2:
        return get_manager_keyboard()
    elif level == 3:
        return get_architect_keyboard()
    else:
        return None
