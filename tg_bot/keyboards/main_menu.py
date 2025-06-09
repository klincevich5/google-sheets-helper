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

# --- Keyboards ---

def get_dealer_keyboard():
    # Level 1: Dealer, Shuffler
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Change shift", callback_data="select_shift"),
            InlineKeyboardButton(text="📍 Current shift", callback_data="select_current_shift")
        ],
        [
            InlineKeyboardButton(text="💬 My feedbacks", callback_data="view_my_feedback"),
            InlineKeyboardButton(text="⚠️ My mistakes", callback_data="view_my_mistakes")
        ],
        [InlineKeyboardButton(text="📞 Contacts", callback_data="contact_info")]
    ])

def get_manager_keyboard():
    # Level 2: Manager, QA_Manager, HR_Manager, Chief_SM_Manager, Trainer_Manager, Floor_Manager, Admin
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Change shift", callback_data="select_shift"),
            InlineKeyboardButton(text="📍 Current shift", callback_data="select_current_shift")
        ],
        [
            InlineKeyboardButton(text="🧠 Check_user_status", callback_data="select_users")
        ],
        [
            InlineKeyboardButton(text="📋 Shift report", callback_data="select_report"),
            InlineKeyboardButton(text="🧑‍🤝‍🧑 Dealer list", callback_data="view_dealers_list")
        ],
        [
            InlineKeyboardButton(text="💬 Feedbacks", callback_data="view_shift_feedbacks"),
            InlineKeyboardButton(text="⚠️ Mistakes", callback_data="view_shift_mistakes")
        ],
        [
            InlineKeyboardButton(text="🔁 Rotation by floor", callback_data="select_rotation")
        ],
        [
            InlineKeyboardButton(text="📞 Contacts", callback_data="contact_info")
        ]
    ])

def get_architect_keyboard():
    # Level 3: Architect
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Change shift", callback_data="select_shift"),
            InlineKeyboardButton(text="📍 Current shift", callback_data="select_current_shift")
        ],
        [
            InlineKeyboardButton(text="🧠 View tasks", callback_data="select_tasks"),
            InlineKeyboardButton(text="🧠 Check_user_status", callback_data="select_users")
        ],
        [
            InlineKeyboardButton(text="📋 Shift report", callback_data="select_report"),
            InlineKeyboardButton(text="🧑‍🤝‍🧑 Dealer list", callback_data="view_dealers_list")
        ],
        [
            InlineKeyboardButton(text="💬 Feedbacks", callback_data="view_shift_feedbacks"),
            InlineKeyboardButton(text="⚠️ Mistakes", callback_data="view_shift_mistakes")
        ],
        [
            InlineKeyboardButton(text="🔁 Rotation by floor", callback_data="select_rotation")
        ],
        [
            InlineKeyboardButton(text="📞 Contacts", callback_data="contact_info")
        ]
    ])

def get_main_menu_keyboard_by_role(role: str):
    level = get_access_level(role)
    if level == 1:
        return get_dealer_keyboard()
    elif level == 2:
        return get_manager_keyboard()
    elif level == 3:
        return get_architect_keyboard()
    else:
        return None
