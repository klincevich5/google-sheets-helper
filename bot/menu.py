# bot/handlers/menu.py

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramBadRequest
from bot.states import MenuStates
from zoneinfo import ZoneInfo
from core.config import AUTHORIZED_USERS, TIMEZONE
from datetime import datetime
from database.session import SessionLocal


router = Router()

def generate_main_menu_text():

    return (
        f"<b>👋 Привет! Я — бот мониторинга Google Sheets.</b>\n\n"
        f"🔐 <b>Токен:</b> активен\n"
        f"🕒 <i>{now}</i>"
    )
