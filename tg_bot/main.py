# tg_bot.main.py

import asyncio
from tg_bot.bot import bot, dp
from tg_bot.handlers import (
    common, calendar_navigation,
    dealer_main, manager_main, architect_main,
    common_callbacks
)
from tg_bot.handlers.common_callbacks import setup_callback_security

async def main():
    # Подключаем роутеры
    dp.include_routers(
        common,
        calendar_navigation,
        dealer_main.router,
        manager_main.router,
        architect_main.router,
        common_callbacks,
    )

    # Настройка безопасности callback для всех роутеров
    for r in [
        calendar_navigation,
        dealer_main.router,
        manager_main.router,
        architect_main.router,
        common_callbacks,
    ]:
        setup_callback_security(r)

    await dp.start_polling(bot, handle_signals=False)

if __name__ == "__main__":
    asyncio.run(main())
