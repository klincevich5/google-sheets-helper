# utils/decorators.py

from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from typing import Callable, Awaitable

def track_message(handler: Callable[[CallbackQuery, FSMContext], Awaitable[None]]):
    async def wrapper(callback: CallbackQuery, state: FSMContext, *args, **kwargs):
        await state.update_data(chat_id=callback.message.chat.id, message_id=callback.message.message_id)
        return await handler(callback, state, *args, **kwargs)
    return wrapper
