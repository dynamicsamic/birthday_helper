import logging
from logging.config import fileConfig
from typing import Any, Iterator, Self

from aiogram import Bot
from sqlalchemy import Engine

from src import settings
from src.db.models import Birthday
from src.db.shared import db_engine as prod_db_engine
from src.db.shared import get_session
from src.toolbox.yandex_disk import YandexDisk
from src.utils import (
    BirthdayStorage,
    DownloadKwargs,
    YadiskKwargs,
    get_bot,
    get_current_date,
    set_inline_button,
)

from .excelparser import ExcelParser, df_row_to_birthday_mapping
from .messageformat import get_formatted_messages

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class BirthdayMessageLoader:
    """
    Load, convert and persist birthday data
    for further delivery to telegram chats.

    :param yadisk_kwargs: Dictionary of settings for creating
        an insatnce of `YandexDisk` from `app.services.yandex_disk`, e.g.:
        {'token': <token>} or
        {'token': <token>, 'secret': <secret>, 'id: <id>}
    :param download_kwargs: Dictionary of settings for downloading
        a file from Yandex.Disk. Must look like:
        {
            'remote_filepath': <remote_filepath>,
            'local_filepath': <local_filepath>
        }
    :param bot: An instance of `Bot` from `aiogram`.
        Used for sending notifications to `BOT_MANAGER` telegram chat.
    :param db_engine: SQLAlchemy db connector.
    """

    def __init__(
        self,
        yadisk_kwargs: YadiskKwargs,
        download_kwargs: DownloadKwargs,
        bot: Bot,
        db_engine: Engine = None,
    ) -> None:
        self.yadisk_kwargs = yadisk_kwargs
        self.download_kwargs = download_kwargs
        self.bot = bot
        self.message_store = BirthdayStorage()

        if db_engine is None:
            db_engine = prod_db_engine

        self.db_engine = db_engine

    def __iter__(self) -> Iterator[str]:
        """Concisely iterate over loaded messages.

            Desired flow:\n
            `messages` = BirthdayMessageLoader(*args, **kwargs)\n
            `await messages.load()`\n
            for `message` in `messages`:
                do stuff

        :returns: Iterator that yields strings
            each of which represents birthday message.
        """
        return iter(self.message_store.messages)

    def is_empty(self) -> bool:
        """Show if `self.message_store` is empty."""
        return self.message_store.is_empty()

    def is_fresh(self, **fresh_period: Any) -> bool:
        """Compares `self.message_store` timestamp against provided time delta.

        :param fresh_period: A keyword argument (only one should be provided)
            valid for creating a `datetime.timedelta` instance.
            e.g.: `hours=1`; 'minutes=25`; `seconds=180`.

        :returns: Result of comparing to datetimes.
        """
        return self.message_store.is_fresh(**fresh_period)

    async def load(self) -> None:
        """Load birthday messages into `self.message_store`.
        Loaded messages are then dispatched to telegram chats.
        """
        await self._generate_mappings()
        with get_session(self.db_engine) as session:
            num_inserted = Birthday.operations.refresh_table(
                self.model_mappings, session
            )
        if num_inserted == 0:
            logger.error(f"Database update failure: ")
            self.message_store["warning"] = (
                "Не удалось обновить базу данных. "
                "Ответ может не содержать наиболее актуальных данных."
            )

        else:
            self.message_store.pop("warning", None)

        await self._load_formatted_messages()

    async def _generate_mappings(self) -> None:
        """Generate mappings (namely dicts of birthday data)
        from file downloaded form `Yandex.Disk`
        for insertion into SQL table.

        Processed data than stored in `self.model_mappings` variable.
        """
        self.model_mappings = []
        async with YandexDisk(**self.yadisk_kwargs) as disk:
            if not await disk.check_token():
                kbd = set_inline_button(
                    text="Получить код", callback_data="confirm_code"
                )
                await self.bot.send_message(
                    settings.BOT_MANAGER_TELEGRAM_ID,
                    "Токен безопасности Яндекс Диска устарел.\n"
                    "Для получения кода подвтерждения нажмите на кнопку ниже и "
                    "перейдите по ссылке.\nВ открывшейся вкладке браузера войдите в "
                    "Яндекс аккаунт, на котором хранится Excel файл с данными о днях "
                    "рождениях. После этого вы автоматически перейдете на страницу "
                    "получения кода подвтерждения. Скопируйте этот код и отправьте его "
                    "боту с командой /code.",
                    reply_markup=kbd,
                )
                logger.error(
                    "Could not download file from YaDisk - token expired!"
                )

            elif await disk.download_file(**self.download_kwargs):
                parser = ExcelParser(
                    self.download_kwargs.get("local_filepath"),
                    columns=settings.COLUMNS,
                    unique_fields=("ФИО",),
                    filter_set={
                        "Дата": ["> 0", "< 32"],
                        "месяц": [f"in {settings.MONTHS}"],
                    },
                )
                try:
                    self.model_mappings = parser.run(
                        df_row_to_birthday_mapping
                    )
                except Exception as e:
                    logger.error(
                        f"ExcelParser in <load_messages> [FAILURE!]: {e}"
                    )

    async def _load_formatted_messages(self) -> None:
        """Save formatted messages into `self.message_store`."""
        today = await get_current_date(settings.TIME_API_URL)

        with get_session(self.db_engine) as session:
            today_birthdays = Birthday.queries.today(session, today)
            future_birthdays = Birthday.queries.future(session, today)

        today_message = get_formatted_messages(today_birthdays)
        future_message = get_formatted_messages(future_birthdays, today=False)

        self.message_store["today"] = today_message
        self.message_store["future"] = future_message

        if self.message_store.is_empty():
            self.message_store["future"] = (
                "Сегодня и ближайшие пару дней"
                " #деньрождения не предвидится."
            )

    @classmethod
    def create(cls) -> Self:
        """Create template loader."""
        yadisk_kwargs = {"token": settings.YADISK_TOKEN}
        local_file = settings.BASE_DIR / settings.OUTPUT_FILE_NAME
        download_kwargs = {
            "remote_filepath": settings.YADISK_FILEPATH,
            "local_filepath": local_file.as_posix(),
        }
        bot = get_bot()
        return cls(yadisk_kwargs, download_kwargs, bot)
