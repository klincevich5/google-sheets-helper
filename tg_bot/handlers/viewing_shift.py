# tg_bot/handlers/viewing_shift.py

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from tg_bot.states.shift_navigation import ShiftNavigationState
from utils.roles import ROLE_PERMISSIONS
from utils.utils import get_current_shift_and_date

router = Router()


async def render_shift_dashboard(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    role = data.get("role", "dealer")
    date = data.get("selected_date")
    shift_type = data.get("selected_shift_type")

    text = (
        f"📊 <b>Shift Dashboard</b>\n"
        f"🗓 <b>{date.strftime('%d %b %Y')}</b> — <b>{'🌞 Day' if shift_type == 'day' else '🌙 Night'} shift</b>\n\n"
        f"👥 Dealers: 50\n"
        f"✅ On shift: 43\n"
        f"💬 Feedbacks: 5 | ⚠️ Mistakes: 3"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Select shift", callback_data="select_shift")
    kb.button(text="➡️ Next shift", callback_data="next_shift")

    if ROLE_PERMISSIONS.get(role, {}).get("view_reports"):
        kb.button(text="📄 Get shift report", callback_data="get_shift_report")

    kb.button(text="🔙 Back", callback_data="return_to_today")
    kb.adjust(2)

    chat_id = data.get("chat_id")
    message_id = data.get("message_id")

    if not chat_id or not message_id:
        new_msg = await message.answer(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
        await state.update_data(chat_id=new_msg.chat.id, message_id=new_msg.message_id)
        return

    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        # В случае ошибки — отправить новое сообщение
        new_msg = await message.answer(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
        await state.update_data(chat_id=new_msg.chat.id, message_id=new_msg.message_id)


@router.callback_query(F.data == "return_to_today")
async def return_to_today(callback: CallbackQuery, state: FSMContext, bot: Bot):
    shift_type, shift_date = get_current_shift_and_date()

    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
    await state.update_data(
        selected_shift_type=shift_type,
        selected_date=shift_date,
        is_current_shift=True
    )

    await render_shift_dashboard(callback.message, state, bot)


@router.callback_query(F.state == ShiftNavigationState.VIEWING_SHIFT)
async def show_shift_dashboard(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await render_shift_dashboard(callback.message, state, bot)
