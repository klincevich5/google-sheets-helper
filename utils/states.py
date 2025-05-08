from aiogram.fsm.state import StatesGroup, State

class MenuStates(StatesGroup):
    MAIN_MENU = State()
    ROTATIONS_MENU = State()
    SHIFT_SELECTED = State()
    TASK_SELECTED = State()
    SHEETS_MENU = State()
    SHEET_TASK_SELECTED = State()
    TRACKED_MENU = State()  # ← Вот это нужно было добавить!
