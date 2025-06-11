# tg_bot/handlers/architect/tasks.py

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from sqlalchemy import select, and_
from datetime import datetime
from babel.dates import format_date
from database.db_models import SheetsInfo, RotationsInfo
from database.session import get_session
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.utils.utils import day_or_night
import httpx
import json
from tg_bot.handlers.common_callbacks import check_stranger_callback

router = Router()
PAGE_SIZE = 5

def get_shift_label(now: datetime) -> str:
    shift = day_or_night(now)
    return "🌞 Day shift" if shift == "day" else "🌙 Night shift"

@router.callback_query(F.data == "select_tasks")
async def select_tasks(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    from tg_bot.handlers.common_callbacks import push_state
    await push_state(state, ShiftNavigationState.SELECT_TASKS)
    await state.set_state(ShiftNavigationState.SELECT_TASKS)

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="📁 SheetsInfo", callback_data="task:list:sheets:0"),
        InlineKeyboardButton(text="🔄 RotationsInfo", callback_data="task:list:rotations:0")
    )
    kb.row(InlineKeyboardButton(text="🖥 Server", callback_data="task:server"))
    kb.row(InlineKeyboardButton(text="↩️ Back", callback_data="return_shift"))

    await callback.message.edit_text(
        text="📌 <b>Select task category:</b>",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("task:list:"))
async def list_tasks(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    try:
        _, _, table_name, page_str = callback.data.split(":")
        page = int(page_str)
        now = datetime.now()
        current_month = now.replace(day=1).date()
        shift_label = get_shift_label(now)
        formatted_date = format_date(now, format="dd MMM yyyy", locale="en")

        model = SheetsInfo if table_name == "sheets" else RotationsInfo
        with get_session() as session:
            stmt = select(model).where(
                and_(
                    model.is_active == 1,
                    model.related_month == current_month
                )
            )
            # Получаем все задачи для подсчёта и текущую страницу для вывода
            all_tasks = session.execute(stmt).scalars().all()
            total_active = len(all_tasks)
            tasks = all_tasks[page * PAGE_SIZE: (page + 1) * PAGE_SIZE]

        if not tasks:
            await callback.answer("No active tasks found", show_alert=True)
            return

        text = (
            f"<b>📋 Select task to view report:</b>\n"
            f"<i>{formatted_date} — {shift_label}</i>\n"
            f"<i>Showing {len(tasks)} of {total_active} active tasks</i>\n\n"
            f"<b>Status | Name | Page | Month | Last Scan | Scans / Fails</b>\n"
        )

        for task in tasks:
            last_scan = task.last_scan.strftime('%Y-%m-%d %H:%M') if isinstance(task.last_scan, datetime) else "–"
            text += (
                f"{'✅' if task.is_active else '❌'} <b>{task.name_of_process}</b> | "
                f"{task.source_page_name} | "
                f"{task.related_month.strftime('%Y-%m')} | "
                f"{last_scan} | "
                f"{task.scan_quantity}📈 / {task.scan_failures}❌\n"
            )

        kb = InlineKeyboardBuilder()
        for task in tasks:
            kb.row(
                InlineKeyboardButton(text="✅On" if task.is_active else "❌Off",
                                     callback_data=f"task:toggle:{table_name}:{task.id}:{page}"),
                InlineKeyboardButton(text=task.name_of_process[:32],
                                     callback_data=f"task:details:{table_name}:{task.id}:{page}"),
                InlineKeyboardButton(text="🔄Run",
                                     callback_data=f"task:run:{table_name}:{task.id}")
            )

        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"task:list:{table_name}:{page - 1}"))
        if len(tasks) == PAGE_SIZE:
            nav_row.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"task:list:{table_name}:{page + 1}"))
        if nav_row:
            kb.row(*nav_row)

        kb.row(InlineKeyboardButton(text="↩️ Back to Menu", callback_data="select_tasks"))

        await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    except Exception as e:
        await callback.answer(f"Error: {e}", show_alert=True)

@router.callback_query(F.data.startswith("task:details:"))
async def task_details(callback: CallbackQuery):
    if await check_stranger_callback(callback): return
    _, _, table_name, task_id, page = callback.data.split(":")
    model = SheetsInfo if table_name == "sheets" else RotationsInfo

    with get_session() as session:
        task = session.get(model, int(task_id))
        if not task:
            await callback.answer("Task not found", show_alert=True)
            return
        task_data = {k: getattr(task, k, None) for k in [
            "name_of_process", "source_table_type", "source_page_name", "source_page_area",
            "scan_group", "last_scan", "scan_interval", "scan_quantity", "scan_failures",
            "hash", "process_data_method", "values_json",
            "target_table_type", "target_page_name", "target_page_area",
            "update_group", "last_update", "update_quantity", "update_failures"
        ]}

    ordered_fields = [
        "name_of_process", "source_table_type", "source_page_name", "source_page_area",
        "scan_group", "last_scan", "scan_interval", "scan_quantity", "scan_failures",
        "hash", "process_data_method", "values_json",
        "target_table_type", "target_page_name", "target_page_area",
        "update_group", "last_update", "update_quantity", "update_failures"
    ]

    emojis = {
        "name_of_process": "🧠", "source_table_type": "📄", "source_page_name": "📄", "source_page_area": "📄",
        "scan_group": "🧪", "last_scan": "🕐", "scan_interval": "🕓", "scan_quantity": "📊",
        "scan_failures": "❌", "hash": "🔑", "process_data_method": "⚙️", "values_json": "📦",
        "target_table_type": "🎯", "target_page_name": "🎯", "target_page_area": "🎯",
        "update_group": "🧪", "last_update": "🕒", "update_quantity": "🔁", "update_failures": "🛑"
    }

    info = "<b>🧾 Task details:</b>\n\n"
    for key in ordered_fields:
        if key not in task_data:
            continue
        val = task_data[key]
        emoji = emojis.get(key, "")
        if isinstance(val, datetime):
            val = val.strftime('%Y-%m-%d %H:%M')
        elif key == "values_json":
            try:
                parsed = json.loads(val) if val else None
                rows = parsed[:10] if isinstance(parsed, list) else ([parsed] if parsed else [])
                val = "\n".join(" • " + json.dumps(r, ensure_ascii=False).replace('"', '')[:120] for r in rows)
            except:
                val = "⚠️ Invalid JSON"
        info += f"{emoji} <b>{key}</b>: {val}\n"

    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="↩️ Back", callback_data=f"task:list:{table_name}:{page}"))
    await callback.message.edit_text(info, parse_mode="HTML", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("task:toggle:"))
async def toggle_task_status(callback: CallbackQuery):
    if await check_stranger_callback(callback): return
    _, _, table_name, task_id, page = callback.data.split(":")
    model = SheetsInfo if table_name == "sheets" else RotationsInfo

    with get_session() as session:
        task = session.get(model, int(task_id))
        task.is_active = 0 if task.is_active else 1
        session.commit()

    await callback.answer("Status updated ✅")
    await list_tasks(callback=callback, state=None, bot=None)

@router.callback_query(F.data.startswith("task:run:"))
async def task_run(callback: CallbackQuery):
    if await check_stranger_callback(callback): return
    await callback.answer("🚧 Manual run not implemented yet", show_alert=True)

@router.callback_query(F.data == "return_shift")
async def proxy_return_shift(callback: CallbackQuery, state: FSMContext, bot):
    if await check_stranger_callback(callback): return
    from tg_bot.handlers.common_callbacks import return_to_dashboard
    await return_to_dashboard(callback, state, bot)

@router.callback_query(F.data == "task:server")
async def view_server(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    try:
        from tg_bot.handlers.common_callbacks import push_state
        await push_state(state, ShiftNavigationState.VIEW_TASKS)
        await state.set_state(ShiftNavigationState.VIEW_TASKS)

        async with httpx.AsyncClient(timeout=3) as client:
            r = await client.get("http://localhost:8888/status")
            data = r.json()

        cpu = data.get("cpu_percent", [])
        mem = data.get("memory", {})
        net = data.get("network", {})

        msg = (
            "<b>🖥 Server Info</b>\n\n"
            f"<b>CPU:</b> {', '.join([f'{x}%' for x in cpu])}\n"
            f"<b>Memory:</b> {mem.get('used') // (1024**2)}MB / {mem.get('total') // (1024**2)}MB ({mem.get('percent')}%)\n"
            f"<b>Network:</b>\n"
            f"• Sent: {net.get('bytes_sent') // (1024**2)}MB\n"
            f"• Received: {net.get('bytes_recv') // (1024**2)}MB"
        )

        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="↩️ Back", callback_data="select_tasks"))
        await callback.message.edit_text(text=msg, reply_markup=kb.as_markup(), parse_mode="HTML")
    except Exception as e:
        await callback.answer(f"Server error: {e}", show_alert=True)
