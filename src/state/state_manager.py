import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from pandas import DataFrame

from src.cloud.sync_manager import CloudSyncManager
from src.data_processing.cache import DataCacheProtocol
from src.data_processing.file_handler import FileHandlerProtocol
from src.data_processing.query_engine import QueryEngineProtocol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BirthdayDataManager:
    def __init__(
        self,
        cloud_sync_manager: CloudSyncManager,
        data_cache: DataCacheProtocol,
        thread_pool_executor: ThreadPoolExecutor,
        file_handler: FileHandlerProtocol,
        query_engine: QueryEngineProtocol,
        source_file_path: str,
        backup_dir_path: str,
    ):
        self.sync_manager = cloud_sync_manager
        self.data_cache = data_cache
        self.executor = thread_pool_executor
        self.file_handler = file_handler
        self.query_engine = query_engine
        self.source_file_path = Path(source_file_path)
        self.backup_dir_path = Path(backup_dir_path)
        self._sync_complete: bool = False

    async def get_birthday(self, partner_name: str) -> DataFrame:
        return await self.query_engine.filter(
            self.data_cache.get_data(), lambda row: partner_name in row["name"]
        )

    async def get_upcoming_birthdays(self, delta: int = 3) -> DataFrame:
        today = date.today()
        recent_future = today + timedelta(days=delta)

        return await self.query_engine.filter(
            self.data_cache.get_data(),
            lambda row: (row["month"] == today.month and row["day"] == today.day)
            or (
                row["month"] == recent_future.month
                and row["day"] > today.day
                and row["day"] <= recent_future.day
            ),
        )

    async def add_birthday(self, birthday_data: dict[str, Any]):
        await self.file_handler.append_row(birthday_data)
        await self.sync_manager.sync_to_cloud(self.source_file_path)

    async def delete_birthday(self, name: str):
        await self.file_handler.delete_rows(lambda row: name in row["name"])
        await self.sync_manager.sync_to_cloud(self.source_file_path)

    async def update_birthday(self, name: str, update_data: dict[str, Any]):
        await self.file_handler.update_rows(
            lambda row: name in row["name"], update_data
        )
        await self.sync_manager.sync_to_cloud(self.source_file_path)

    async def get_data(self): ...

    async def sync_files(self):
        local_mtime = self.file_handler.last_modified
        cloud_mtime = (await self.sync_manager.get_last_modified()).timestamp()

        if local_mtime > cloud_mtime:
            logger.info("Local file is newer. Uploading update to cloud.")
            await self.sync_manager.sync_to_cloud(self.source_file_path)

        elif local_mtime < cloud_mtime:
            logger.info("Cloud file is newer. Downloading update to local file.")
            async with self.file_handler.atomic_write:
                await self.sync_manager.sync_from_cloud(self.source_file_path)

            # update the cache
            data = await self.file_handler.read_file()
            self.data_cache.update_data(data)
        else:
            logger.info("Files are already synchronized.")
