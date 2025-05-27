# tg_bot/router.py

from aiogram import Dispatcher
from tg_bot.handlers import (
      common, dealer, admin, shuffler, manager, qa_manager, hr_manager,
      chief_sm_manager, trainer_manager, floor_manager, super_admin,
      calendar, viewing_shift, actions_manager
)

def register_routers(dp: Dispatcher):
    dp.include_routers(
        common.router,
        shuffler.router,
        dealer.router,
        manager.router,
        qa_manager.router,
        hr_manager.router,
        chief_sm_manager.router,
        trainer_manager.router,
        floor_manager.router,
        admin.router,
        super_admin.router,
        viewing_shift.router,
        calendar.router,
        actions_manager.router,

    )
