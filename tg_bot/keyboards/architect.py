from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

def get_architect_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Change shift", callback_data="select_shift"),
            InlineKeyboardButton(text="📍 Current shift", callback_data="select_current_shift")
        ],
        [
            InlineKeyboardButton(text="🧠 View tasks", callback_data="select_tasks"),
            InlineKeyboardButton(text="🧠 Check user status", callback_data="select_users")
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
