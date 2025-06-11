from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.keyboards.architect import get_architect_keyboard
from tg_bot.formatting.architect import get_architect_main_view
from tg_bot.services.db import get_user_role
from tg_bot.keyboards.main_menu import get_main_menu_keyboard_by_role
from tg_bot.handlers.common_callbacks import check_stranger_callback, contact_info
from core.timezone import now
from tg_bot.utils.utils import get_current_shift_and_date
from tg_bot.handlers.architect.tasks import select_tasks

router = Router()

@router.callback_query(F.data == "select_shift", ShiftNavigationState.VIEWING_SHIFT)
async def select_shift_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    from tg_bot.handlers.calendar_navigation import open_calendar
    await open_calendar(callback, state, bot)

@router.callback_query(F.data == "select_current_shift", ShiftNavigationState.VIEWING_SHIFT)
async def select_current_shift(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    current_time = now()
    current_shift_type, current_shift_date = get_current_shift_and_date(current_time)
    data = await state.get_data()
    selected_date = data.get("selected_date")
    selected_type = data.get("selected_shift_type")
    if selected_date == current_shift_date and selected_type == current_shift_type:
        await callback.answer("Current shift is already selected", show_alert=True)
        return
    await state.update_data(
        selected_date=current_shift_date,
        selected_shift_type=current_shift_type,
        is_current_shift=True
    )
    await render_architect_dashboard(callback, state, bot)

@router.callback_query(F.data == "contact_info", ShiftNavigationState.VIEWING_SHIFT)
async def contact_info_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    await contact_info(callback, state, bot)

@router.callback_query(F.data == "view_tasks", ShiftNavigationState.VIEWING_SHIFT)
async def view_tasks_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    await select_tasks(callback, state, bot)

@router.callback_query(F.data == "select_tasks", ShiftNavigationState.VIEWING_SHIFT)
async def select_tasks_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    await select_tasks(callback, state, bot)

@router.callback_query(F.data.startswith("report:"), ShiftNavigationState.VIEWING_SHIFT)
async def view_report_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.manager.reports import view_report
    await view_report(callback, state, bot)

@router.callback_query(F.data.startswith("rotation:"), ShiftNavigationState.VIEWING_SHIFT)
async def view_rotation_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.manager.rotations import view_rotation_detail
    await view_rotation_detail(callback, state, bot)

@router.callback_query(F.data.startswith("rotation_scroll:"), ShiftNavigationState.VIEWING_SHIFT)
async def view_rotation_scroll_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.manager.rotations import scroll_rotation
    await scroll_rotation(callback, state, bot)

@router.callback_query(F.data == "report:vip_generic", ShiftNavigationState.VIEWING_SHIFT)
async def view_report_vip_generic_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.manager.reports import view_report
    await view_report(callback, state, bot)

@router.callback_query(F.data == "report:legendz", ShiftNavigationState.VIEWING_SHIFT)
async def view_report_legendz_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.manager.reports import view_report
    await view_report(callback, state, bot)

@router.callback_query(F.data == "report:gsbj", ShiftNavigationState.VIEWING_SHIFT)
async def view_report_gsbj_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.manager.reports import view_report
    await view_report(callback, state, bot)

@router.callback_query(F.data.startswith("task:list:"), ShiftNavigationState.SELECT_TASKS)
async def list_tasks_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.architect.tasks import list_tasks as real_list_tasks
    await real_list_tasks(callback, state, bot)

@router.callback_query(F.data.startswith("task:details:"), ShiftNavigationState.SELECT_TASKS)
async def task_details_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.architect.tasks import task_details as real_task_details
    await real_task_details(callback)

@router.callback_query(F.data.startswith("task:toggle:"), ShiftNavigationState.SELECT_TASKS)
async def toggle_task_status_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.architect.tasks import toggle_task_status as real_toggle_task_status
    await real_toggle_task_status(callback)

@router.callback_query(F.data.startswith("task:run:"), ShiftNavigationState.SELECT_TASKS)
async def task_run_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.architect.tasks import task_run as real_task_run
    await real_task_run(callback)

@router.callback_query(F.data == "task:server", ShiftNavigationState.SELECT_TASKS)
async def view_server_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.architect.tasks import view_server as real_view_server
    await real_view_server(callback, state, bot)

@router.callback_query(F.data == "return_shift", ShiftNavigationState.SELECT_TASKS)
async def return_shift_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.architect.tasks import proxy_return_shift as real_proxy_return_shift
    await real_proxy_return_shift(callback, state, bot)

async def render_architect_dashboard(msg_or_cb: Message | CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    user_id = data.get("user_id") or msg_or_cb.from_user.id
    chat_id = msg_or_cb.chat.id if isinstance(msg_or_cb, Message) else msg_or_cb.message.chat.id
    selected_date = data.get("selected_date")
    selected_shift_type = data.get("selected_shift_type")
    if selected_date is None or selected_shift_type is None:
        current_time = now()
        current_shift_type, current_date = get_current_shift_and_date(current_time)
        selected_date = selected_date or current_date
        selected_shift_type = selected_shift_type or current_shift_type
        await state.update_data(
            selected_date=selected_date,
            selected_shift_type=selected_shift_type
        )
    role = await get_user_role(user_id)
    if role.lower() == "stranger":
        text = "⛔️ Access not granted.\nPlease contact your manager to get access."
        keyboard = None
    else:
        # user_id — это int, а get_architect_main_view ожидает dict с dealer_name
        from tg_bot.services.db import get_or_create_user
        user = await get_or_create_user(user_id)
        text = get_architect_main_view(user, selected_date, selected_shift_type)
        keyboard = get_main_menu_keyboard_by_role(role)
    if isinstance(msg_or_cb, CallbackQuery):
        await msg_or_cb.answer()
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_or_cb.message.message_id,
            text=text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.update_data(message_id=msg_or_cb.message.message_id, chat_id=chat_id)
    else:
        sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode="HTML")
        await state.update_data(message_id=sent.message_id, chat_id=sent.chat.id)
