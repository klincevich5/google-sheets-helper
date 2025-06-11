from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

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
