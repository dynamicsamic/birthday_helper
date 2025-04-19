from aiogram import Router

from src.telegram.middlewares import ChatWhitelistMiddleware, DbSessionMiddleware

from .birthday import router as birthday_router
from .common import router as common_router

router = Router()
router.include_routers(birthday_router, common_router)
router.message.middleware(DbSessionMiddleware())
router.message.outer_middleware(ChatWhitelistMiddleware())
