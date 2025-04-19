from concurrent.futures import ThreadPoolExecutor

from aiogram import Bot, Dispatcher
from pandas import to_numeric

from src.cloud.storage_manager import YandexDiskStorageManager
from src.cloud.sync_manager import YandexDiskSyncManager
from src.data_processing.cache import DataFrameBasedCache
from src.data_processing.file_handler import ExcelFileHandler
from src.data_processing.query_engine import QueryEngine
from src.logging import setup_logging, shutdown_logging
from src.settings import BACKUP_DIR_PATH, SOURCE_FILE_PATH
from src.state.state_manager import BirthdayDataManager
from src.task_schedule.task_scheduler import start_scheduler
from src.telegram.handlers import router

bot_token = ""


def create_birthday_data_manager(executor: ThreadPoolExecutor):
    storage_manager = YandexDiskStorageManager("//get_yandex_disk_token")
    file_sync_manager = YandexDiskSyncManager(storage_manager, "//path_to_source_file")
    data_cache = DataFrameBasedCache(
        executor,
        [
            # convert day and month to ints, drop invalid
            lambda df: df.assign(
                month=lambda x: to_numeric(x["month"], errors="coerce"),
                day=lambda x: to_numeric(x["day"], errors="coerce"),
            ).dropna()
        ],
    )
    excel_file_handler = ExcelFileHandler(SOURCE_FILE_PATH, BACKUP_DIR_PATH, executor)
    query_engine = QueryEngine(executor)

    return BirthdayDataManager(
        cloud_sync_manager=file_sync_manager,
        data_cache=data_cache,
        thread_pool_executor=executor,
        file_handler=excel_file_handler,
        query_engine=query_engine,
    )


async def init_bot():
    setup_logging()

    bot = Bot(bot_token)
    dp = Dispatcher()
    executor = ThreadPoolExecutor()

    data_manager = create_birthday_data_manager(executor)
    task_scheduler = start_scheduler(data_manager)

    dp["state_manager"] = data_manager
    dp["task_scheduler"] = task_scheduler

    dp.include_router(router)

    try:
        await dp.start_polling(bot)
    finally:
        await task_scheduler.shutdown()
        executor.shutdown()
        shutdown_logging()
