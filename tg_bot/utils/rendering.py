# utils/rendering.py

from aiogram import Bot
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message
from aiogram.fsm.context import FSMContext

def markup_equal(m1: InlineKeyboardMarkup | None, m2: InlineKeyboardMarkup | None) -> bool:
    if m1 is None and m2 is None:
        return True
    if m1 is None or m2 is None:
        return False
    def rows_to_dicts(markup):
        return [[button.__dict__ for button in row] for row in markup.inline_keyboard]
    return rows_to_dicts(m1) == rows_to_dicts(m2)

async def safe_edit_or_send(
    event: CallbackQuery | Message,
    bot: Bot,
    text: str,
    markup: InlineKeyboardMarkup | None,
    state: FSMContext
) -> None:
    if isinstance(event, CallbackQuery):
        msg = event.message
        chat_id = msg.chat.id
        message_id = msg.message_id
        current_text = msg.text or msg.caption
        current_markup = msg.reply_markup

        if current_text == text and markup_equal(current_markup, markup):
            await event.answer("Уже на этой странице", show_alert=True)
            return

        try:
            await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
                parse_mode="HTML"
            )
            await state.update_data(message_id=message_id, chat_id=chat_id)
            return
        except Exception as e:
            print(f"[safe_edit_or_send] edit failed: {e}")

    # Fallback: отправить новое сообщение
    chat_id = event.chat.id
    sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup, parse_mode="HTML")
    await state.update_data(message_id=sent.message_id, chat_id=chat_id)
