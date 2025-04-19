import logging

# from logging.config import fileConfig
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.dispatcher.flags import extract_flags, get_flag
from aiogram.types import CallbackQuery, Message

from src.db.connection import get_db_session
from src.db.repository import BaseRepository, get_repository
from src.settings import logger_config

# fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

allowed_chats = [359722292]


class ChatWhitelistMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        if event.chat.id not in allowed_chats:
            return
        return await handler(event, data)


class ChatWhitelistMiddleware1(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        event.from_user.id
        if event.chat.id not in allowed_chats:
            return
        return await handler(event, data)


class DbSessionMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        async with get_db_session() as db_session:
            logger.info("Started new db session")
            repositories: list[BaseRepository] = get_flag(data, "repositories")
            data["repositories"] = {
                repository.get_name(): repository(db_session)
                for repository in repositories
            }

            # repository = get_repository(get_flag(data, "repository"), db_session)
            # data["repository"] = repository
            # data["db_session"] = session
            # print(data)
            # repo = get_flag(data, "repository")
            # print(f"{repo=}")
            # if (repo := data.get("flags").get("repository")) is not None:
            #     print(f"{repo=}")
            return await handler(event, data)
        #     result = await handler(event, data)
        # return result
