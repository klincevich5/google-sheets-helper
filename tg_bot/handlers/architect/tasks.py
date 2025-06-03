from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState

router = Router()

@router.callback_query(F.data == "select_tasks")
async def select_tasks(callback: CallbackQuery, state: FSMContext, bot: Bot):
    print("[architect/tasks] select_tasks")
    try:
        from tg_bot.handlers.common_callbacks import push_state
        await push_state(state, ShiftNavigationState.SELECT_TASKS)
        await state.set_state(ShiftNavigationState.SELECT_TASKS)

        kb = InlineKeyboardBuilder()
        kb.button(text="⚙️ Studio optimization", callback_data="task:optimize")
        kb.button(text="📊 KPI monitoring", callback_data="task:kpi")
        kb.button(text="🛠 Tech audits", callback_data="task:tech")
        # Кнопка возврата на дашборд
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
        # Кнопка возврата к выбору задач
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
    if task_type == "optimize":
        return (
            "<b>⚙️ Studio Optimization Tasks</b>\n"
            "• Review rotation times\n"
            "• Analyze table load per hour\n"
            "• Identify underutilized stations"
        )
    elif task_type == "kpi":
        return (
            "<b>📊 KPI Monitoring</b>\n"
            "• Check avg rounds per hour\n"
            "• Compare SM vs. Floor stats\n"
            "• Track absenteeism rate"
        )
    elif task_type == "tech":
        return (
            "<b>🛠 Technical Audit Tasks</b>\n"
            "• Review camera coverage\n"
            "• Check dealer mic issues\n"
            "• Confirm lighting standards"
        )
    else:
        return "❌ Unknown task type"
