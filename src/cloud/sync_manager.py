import asyncio
from datetime import datetime

from src.cloud.storage_manager import (
    CloudStorageMangerInterface,
    YandexDiskStorageManager,
)
from src.data_processing.atomic_operation import AtomicOperation


class CloudSyncManager:
    def __init__(
        self,
        cloud_storage_manager: CloudStorageMangerInterface,
        remote_path: str,
    ):
        self.storage_manager = cloud_storage_manager
        self.remote_path = remote_path
        self.sync_complete: bool = False

    async def sync_to_cloud(self, local_file_path: str):
        pass

    async def sync_from_cloud(self, local_file_path: str):
        pass

    async def download_file(): ...

    async def sync(): ...

    async def upload_file(): ...

    async def get_last_modified(self) -> datetime: ...


class YandexDiskSyncManager(CloudSyncManager):
    def __init__(
        self,
        cloud_storage_manager: YandexDiskStorageManager,
        remote_path: str,
        backup_path: str,
    ):
        self.storage_manager = cloud_storage_manager
        self.remote_path = remote_path
        self.backup_path = backup_path
        self.sync_complete: bool = False
        self._lock = asyncio.Lock()
        self.atomic_write = AtomicOperation(self)

    def get_file_lock(self):
        return self._lock

    async def create_backup(self):
        await self.storage_manager.copy_file(self.remote_path, self.backup_path)

    async def restore_backup(self, backup_path: str):
        await self.storage_manager.copy_file(self.backup_path, self.remote_path)

    async def sync_from_cloud(self, local_file_path: str) -> bool:
        return await self.storage_manager.download_file(
            self.remote_path, local_file_path
        )

    async def sync_to_cloud(self, local_file_path: str) -> bool:
        async with self.atomic_write:
            backup_success = await self.storage_manager.copy_file(
                self.remote_path, self.backup_path
            )
            if not backup_success:
                raise ValueError("Cloud source file backup failed")
            return await self.storage_manager.upload_file(
                local_file_path, self.remote_path
            )

    async def get_last_modified(self) -> datetime:
        file_info = await self.storage_manager.get_file_info(self.remote_path)
        return file_info["modified"]
