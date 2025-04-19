from logging.config import fileConfig

# from pathlib import Path
# from decouple import config
from pytz import timezone

# BASE_DIR = Path(__name__).resolve().parent
# APP_NAME = "app"

# BOT_INSTANCE = "bot.bot"
# BOT_TOKEN = config("BOT_TOKEN")
# BOT_MANAGER_TELEGRAM_ID = config("BOT_MANAGER_TELEGRAM_ID")

# YADISK_TOKEN = config("YADISK_TOKEN")
# YADISK_TEST_TOKEN = config("YADISK_TEST_TOKEN")
# YANDEX_APP_ID = config("YANDEX_APP_ID")
# YANDEX_SECRET_CLIENT = config("YANDEX_SECRET_CLIENT")
# YADISK_FILEPATH = config("YADISK_FILEPATH")

SOURCE_FILE_PATH = "root/src/data/source.xlsx"
BACKUP_DIR_PATH = "root/src/data/backups"

# OUTPUT_FILE_NAME = "source.xlsx"
# TIME_API_URL = "http://worldtimeapi.org/api/timezone/Europe/Moscow"

# COLUMNS = {"Дата": int, "месяц": str, "ФИО": str}
MONTHS = (
    "январь",
    "февраль",
    "март",
    "апрель",
    "май",
    "июнь",
    "июль",
    "август",
    "сентябрь",
    "октябрь",
    "ноябрь",
    "декабрь",
)
# FUTURE_SCOPE = 3
TIME_ZONE = timezone("Europe/Moscow")

# DEBUG = False

# DB = {
#     "app": {"engine": "sqlite", "driver": "", "name": config("APP_DB_NAME")},
#     "jobstore": {
#         "engine": "sqlite",
#         "driver": "",
#         "name": config("JOBSTORE_DB_NAME"),
#     },
# }

logger_config = fileConfig(fname="log_config.conf", disable_existing_loggers=False)
