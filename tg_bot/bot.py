# tg_bot/bot.py

from aiogram import Bot, Dispatcher
from core.config import BOT_TOKEN

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def setup_bot():
    return dp, bot
