# tg_bot/handlers/architect/tasks.py

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
import httpx

router = Router()

@router.callback_query(F.data == "select_tasks")
async def select_tasks(callback: CallbackQuery, state: FSMContext, bot: Bot):
    print("[architect/tasks] select_tasks")
    try:
        from tg_bot.handlers.common_callbacks import push_state
        await push_state(state, ShiftNavigationState.SELECT_TASKS)
        await state.set_state(ShiftNavigationState.SELECT_TASKS)

        kb = InlineKeyboardBuilder()
        kb.button(text="🖥 Server Info", callback_data="task:server")
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)

        await callback.message.edit_text(
            text="🧠 Select task category:",
            reply_markup=kb.as_markup()
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)


@router.callback_query(F.data.startswith("task:"))
async def view_tasks(callback: CallbackQuery, state: FSMContext, bot: Bot):
    print("[architect/tasks] view_tasks")
    try:
        from tg_bot.handlers.common_callbacks import push_state
        await push_state(state, ShiftNavigationState.VIEW_TASKS)
        await state.set_state(ShiftNavigationState.VIEW_TASKS)

        _, task_type = callback.data.split(":")
        task_text = await get_task_text(task_type)

        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back to tasks", callback_data="select_tasks")
        kb.adjust(1)

        await callback.message.edit_text(
            text=task_text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)


@router.callback_query(F.data == "return_shift")
async def proxy_return_shift(callback: CallbackQuery, state: FSMContext, bot):
    from tg_bot.handlers.common_callbacks import return_to_dashboard
    await return_to_dashboard(callback, state, bot)


async def get_task_text(task_type: str) -> str:
    if task_type == "server":
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                r = await client.get("http://localhost:8888/status")
                data = r.json()

            cpu = data.get("cpu_percent", [])
            mem = data.get("memory", {})
            net = data.get("network", {})

            return (
                "<b>🖥 Server Info</b>\n\n"
                f"<b>CPU:</b> {', '.join([f'{x}%' for x in cpu])}\n"
                f"<b>Memory:</b> {mem.get('used') // (1024**2)}MB / {mem.get('total') // (1024**2)}MB ({mem.get('percent')}%)\n"
                f"<b>Network:</b>\n"
                f"• Sent: {net.get('bytes_sent') // (1024**2)}MB\n"
                f"• Recv: {net.get('bytes_recv') // (1024**2)}MB"
            )
        except Exception as e:
            return f"❌ Ошибка при получении данных: {e}"

    return "❌ Unknown task type"
