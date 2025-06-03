from aiogram.fsm.state import State, StatesGroup

class ShiftNavigationState(StatesGroup):
    # Стартовое состояние после верификации
    VIEWING_SHIFT = State()                # Домашняя страница (Просмотр конкретной смены)

    # Навигация по сменам
    CALENDAR = State()                     # Календарь: выбор даты
    SHIFT_TYPE = State()                   # Выбор типа смены (день/ночь)

    ##########################################################
    # For dealers
    ##########################################################
    VIEW_MY_FEEDBACK = State()             # Просмотр своих фидбэков
    VIEW_MY_MISTAKES = State()             # Просмотр ошибок на смене
    CONTACT_INFO = State()                 # Контактная информация (для дилеров — только руководство)

    ##########################################################
    # For service managers and architects
    ##########################################################
    SELECT_REPORT = State()                # Меню выбора типа рапорта
    VIEW_REPORT = State()                  # Просмотр конкретного рапорта

    VIEW_SHIFT_FEEDBACKS = State()         # Просмотр фидбэков команды
    VIEW_SHIFT_MISTAKES = State()          # Просмотр ошибок на смене

    SELECT_ROTATION = State()              # Выбор ротации по этажам
    VIEW_ROTATION = State()                # Просмотр ротации по этажам

    VIEW_DEALERS_LIST = State()            # Список дилеров на смене
    VIEW_DEALER = State()                  # Просмотр информации по конкретному сотруднику
    VIEW_DEALER_FEEDBACKS = State()        # Просмотр фидбэков по конкретному сотруднику
    VIEW_DEALER_MISTAKES = State()         # Просмотр ошибок по конкретному сотруднику

    ##########################################################
    # For architects
    ##########################################################
    SELECT_TASKS = State()                 # Выбор задач для просмотра
    VIEW_TASKS = State()                   # Просмотр задач
