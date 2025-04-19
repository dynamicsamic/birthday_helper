import datetime as dt
import logging
from logging.config import fileConfig
from typing import Any, NotRequired, TypedDict, TypeVar

from aiogram import Bot, types
from aiogram.fsm.context import FSMContext
from aiohttp import ClientSession

from src import settings

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
TIME_API_URL = "http://worldtimeapi.org/api/timezone/Europe/Moscow"


class DownloadKwargs(TypedDict):
    remote_filepath: str
    local_filepath: str


class YadiskKwargs(TypedDict):
    token: str
    id: NotRequired[str]
    secret: NotRequired[str]


def days_in_month(month: str, default: int = 30) -> int:
    return {
        "январь": 31,
        "февраль": 29,
        "март": 31,
        "апрель": 30,
        "май": 31,
        "июнь": 30,
        "июль": 31,
        "август": 31,
        "сентябрь": 30,
        "октябрь": 31,
        "ноябрь": 30,
        "декабрь": 31,
    }.get(month, default)


def set_inline_button(**options):
    """Set an inline keyboard with one button."""
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton(**options))
    return kb


def months_grid_reply_kb(row_width: int = 4, **kwargs) -> types.ReplyKeyboardMarkup:
    """Generate reply keyboard with valid months."""
    if row_width < 1 or row_width > len(settings.MONTHS):
        row_width = 4
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=row_width, **kwargs)
    for month in settings.MONTHS:
        kb.insert(month)
    kb.row("❌ отмена")
    return kb


def days_grid_reply_kb(
    month: str, row_width: int = 5, **kwargs
) -> types.ReplyKeyboardMarkup:
    """Generate reply keyboard with valid days in month."""
    days = days_in_month(month)
    if row_width < 1 or row_width > days:
        row_width = 5
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=row_width, **kwargs)
    for i in range(1, days + 1):
        kb.insert(str(i))
    kb.row("❌ отмена", "🔄 начать заново")
    return kb


def message_or_call(f):
    async def inner(message: types.Message | types.CallbackQuery, state: FSMContext):
        call = None
        if isinstance(message, types.CallbackQuery):
            call = message
            message = message.message
        result = await f(message, state)

        if call is not None:
            await call.answer()
        return result

    return inner


def update_envar(path, varname: str, value: str) -> bool:
    """Update environment variable with given value."""
    with open(path) as f:
        contents = f.readlines()

    for idx, line in enumerate(contents):
        if line.startswith(varname):
            contents.pop(idx)
            contents.append("\n" + f"{varname} = {value}")

    with open(path, "w") as f:
        written = f.write("".join(contents))
    return written > 0


def today() -> dt.date:
    return dt.date.today()


def now() -> dt.datetime:
    return dt.datetime.now(settings.TIME_ZONE)


async def get_current_date(url: str) -> dt.date:
    """Try to fetch current date from external API.
    Use system date if fails.
    """
    today = dt.date.today()
    try:
        async with ClientSession() as session:
            async with session.get(url) as response:
                resp_data = await response.json()
                logger.info(f"Получен ответ от стороннего API: {url}.")
    except Exception:
        logger.warning(
            "Не удалось получить ответ от стороннего API. "
            "Текущая дата будет задана операционной системой."
        )
        return today

    if datetime := resp_data.get("datetime"):
        # `datetime` is <str> with format `2022-12-15T00:03:42.431581+03:00`
        # we need only first 10 chars to create a date
        try:
            today = dt.date.fromisoformat(datetime[:10])
        except (TypeError, ValueError):
            logger.warning(
                "Не удалось преобразовать ответ стороннего API в дату. "
                "Формат ответа был изменен."
            )
    return today


def set_timestamp():
    today = dt.datetime.today().astimezone(settings.TIME_ZONE)
    return today.timestamp()


def is_fresh(ts: dt.datetime.timestamp, fresh_period: dict[str, int]) -> bool:
    if ts is None:
        return False
    try:
        date_recieved = dt.datetime.fromtimestamp(ts).astimezone(settings.TIME_ZONE)
        tdelta = dt.timedelta(**fresh_period)
    except Exception as e:
        logger.error(f"utils <is_data_fresh> [FAILURE!]: {e}")
        return False
    today = dt.datetime.today().astimezone(settings.TIME_ZONE)
    return today - date_recieved <= tdelta


def timestamp_to_datetime_string(ts: float) -> str:
    """Convert timestamp to a datetime string."""
    date = dt.datetime.fromtimestamp(ts, tz=settings.TIME_ZONE)
    return f"{date: %d-%m-%Y %H:%M}"


_KT = TypeVar("_KT", bound=Any)
_VT = TypeVar("_VT", bound=Any)


class BirthdayStorage(dict):
    """
    Provides convinient interface for
    storing and retieving birthday messages.
    """

    # Order of message_keys matters
    message_keys = ("warning", "today", "future")
    pure_birthday_keys = ("today", "future")

    def __setitem__(self, __key: _KT, __value: _VT) -> None:
        """Update timestamp on each message update."""
        if __key not in self.message_keys or __key == "ts":
            return
        self.update(ts=set_timestamp())
        return super().__setitem__(__key, __value)

    def is_empty(self) -> bool:
        """If there is no birthday messages, then it's no use
        sending messages with warning.
        """
        return not any(self.get(key) for key in self.pure_birthday_keys)

    def is_fresh(self, **fresh_period: Any) -> bool:
        """Compares current time + fresh_period with last updated timestamp."""
        return is_fresh(self.get("ts"), fresh_period)

    @property
    def messages(self) -> list[str]:
        """Return all messages."""
        return [message for key in self.message_keys if (message := self.get(key))]


def get_bot_path() -> str:
    *local_path, bot_name = settings.BOT_INSTANCE.split(".")
    path_to_bot = f"{settings.APP_NAME}.{'.'.join(local_path)}:{bot_name}"
    return path_to_bot


def get_bot() -> Bot:
    import importlib

    path_to_bot = get_bot_path()
    module_name, instance = path_to_bot.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        raise
    # maybe need to traverse files and find string 'Bot(...)'
    bot = getattr(module, instance)
    return bot
