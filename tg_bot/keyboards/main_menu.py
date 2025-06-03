# tg_bot/keyboards/main_menu.py

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# Dealer keyboard

def get_dealer_keyboard():
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

# Service Manager keyboard

def get_service_manager_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Change shift", callback_data="select_shift"),
            InlineKeyboardButton(text="📍 Current shift", callback_data="select_current_shift")
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

# Architect keyboard

def get_architect_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Change shift", callback_data="select_shift"),
            InlineKeyboardButton(text="📍 Current shift", callback_data="select_current_shift")
        ],
        [
            InlineKeyboardButton(text="🧠 View tasks", callback_data="select_tasks")
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
