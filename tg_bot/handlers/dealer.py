from aiogram import Router
from aiogram.types import Message
from aiogram.filters import CommandStart  # 👈 новый способ

router = Router()

@router.message(CommandStart())  # 👈 вот так теперь фильтруется команда /start
async def start_handler(message: Message):
    await message.answer("👋 Привет! Вы зарегистрированы в системе.")
