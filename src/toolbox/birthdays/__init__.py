import logging
from logging.config import fileConfig

from aiogram import types

from src import settings
from src.toolbox.birthdays.excelparser import append_excel
from src.toolbox.yandex_disk import YandexDisk
from src.utils import get_bot

from .messageloader import BirthdayMessageLoader

fileConfig(fname="log_config.conf", disable_existing_loggers=False)

logger = logging.getLogger(__name__)
Bot = get_bot()
Messages = BirthdayMessageLoader.create()


async def dispatch_birthday_messages_to_chat(chat_id: int) -> None:
    """Sends preloaded birthday messages to a telegram chat.

    :param chat_id: A telegram chat id that requested message dispatch.

    :returns: None."""
    if Messages.is_empty() or not Messages.is_fresh(hours=6):
        await Messages.load()
        logger.info("load birthday messages via scheduler")
    for message in Messages:
        await Bot.send_message(chat_id, message)


async def add_birthday(message: types.Message, birthday_data: list[str | int]) -> bool:
    """Add new row to the end of the excel file with birthdays.

    :param message: instance of `aiogram.types.Message`
    :param birthday_data: list of values to be appended to
        excel file columns.

    :returns: boolean result of operation.
    """
    user_id = message.from_id
    local_filepath = settings.BASE_DIR / settings.OUTPUT_FILE_NAME
    async with YandexDisk(token=settings.YADISK_TOKEN) as disk:
        # Download latest remote file.
        await disk.download_file(settings.YADISK_FILEPATH, local_filepath.as_posix())

        if append_excel(local_filepath.as_posix(), birthday_data):
            if not await disk.upload_file(
                local_filepath.as_posix(),
                settings.YADISK_FILEPATH,
                overwrite=True,
            ):
                await Bot.send_message(
                    chat_id=settings.BOT_MANAGER_TELEGRAM_ID,
                    text=(
                        "#ошибка: при попытке добавить новое ДР "
                        f"пользователем {user_id}:\n"
                        "не удалось обновить файл на Яндекс Диске."
                    ),
                )
                return False
        else:
            await Bot.send_message(
                chat_id=settings.BOT_MANAGER_TELEGRAM_ID,
                text=(
                    "#ошибка: при попытке добавить новое ДР "
                    f"пользователем {user_id}:\n"
                    "не удалось обработать локальный excel файл."
                ),
            )
            return False
        return True
