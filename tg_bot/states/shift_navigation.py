from aiogram.fsm.state import State, StatesGroup

class ShiftNavigationState(StatesGroup):
    # Стартовое состояние после верификации
    MAIN_MENU = State()                # Домашняя страница

    # Навигация по сменам
    CALENDAR = State()                # Календарь: выбор даты
    SHIFT_TYPE = State()              # Выбор типа смены (день/ночь)
    VIEWING_SHIFT = State()           # Просмотр конкретной смены

    # Рапорты и информация
    SELECT_REPORT = State()           # Меню выбора типа рапорта
    VIEW_REPORT = State()             # Просмотр конкретного рапорта

    # Фидбэки
    VIEW_MY_FEEDBACK = State()        # Просмотр своих фидбэков
    VIEW_TEAM_FEEDBACK = State()      # Просмотр фидбэков команды (доступен с роли менеджера)
    GIVE_FEEDBACK = State()           # Выдача фидбэка сотруднику

    # Ошибки
    VIEW_MY_MISTAKES = State()           # Просмотр ошибок на смене
    VIEW_TEAM_MISTAKES = State()           # Просмотр ошибок на смене
    ADD_MISTAKE = State()             # Добавление ошибки по сотруднику

    # Ротация
    SELECT_ROTATION = State()         # Выбор ротации по этажам
    VIEW_ROTATION = State()           # Просмотр ротации по этажам

    # Работа с командой
    VIEW_EMPLOYEE = State()           # Просмотр информации по конкретному сотруднику
    EMPLOYEE_ACTIONS = State()        # Действия с сотрудником (ошибка, фидбэк, замена)

    # Управление сменой
    SHIFT_MENU = State()              # Управление текущей сменой
    UPDATE_SHIFT_DATA = State()       # Обновление информации по смене

    # Дополнительно
    VIEW_TASKS = State()              # Задачи, дела
    VIEW_DEALERS_LIST = State()       # Список дилеров на смене
    VIEW_DEALERS_STATS = State()      # Моя статистика как менеджера
    VIEW_MANAGER_STATS = State()      # Моя статистика как менеджера
    SETTINGS_MENU = State()           # Настройки (если применимо)
