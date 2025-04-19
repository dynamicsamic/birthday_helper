import logging
from logging.config import fileConfig
from typing import Protocol

from yadisk_async import YaDisk

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class CloudStorageMangerInterface(Protocol):
    async def download_file(self):
        pass

    async def upload_file(self):
        pass

    async def get_file_info(self):
        pass


class YandexDiskStorageManager:
    def __init__(self, token: str) -> None:
        self.token = token

    async def download_file(
        self, source_path: str, local_file_path: str, **kwargs
    ) -> bool:
        downloaded = False
        try:
            async with YaDisk(token=self.token) as disk:
                try:
                    await disk.download(source_path, local_file_path, **kwargs)
                    downloaded = True
                    logger.info("<YandexDisk.download_file> [SUCCESS!]")
                except Exception as e:
                    logger.error(f"<YandexDisk.download_file> [FAILURE!]: {e}")
        except Exception as e:
            logger.error(f"<YandexDisk cloud access error> [FAILURE!]: {e}")

        return downloaded  # maybe better to raise exception

    async def upload_file(
        self, local_file_path: str, source_path: str, **kwargs
    ) -> bool:
        uploaded = False
        try:
            async with YaDisk(token=self.token) as disk:
                try:
                    await disk.upload(
                        local_file_path,
                        source_path,
                        n_retries=3,
                        overwrite=True,
                        **kwargs,
                    )
                    uploaded = True
                    logger.info("<YandexDisk.upload_file> [SUCCESS!]")
                except Exception as e:
                    logger.error(f"<YandexDisk.upload_file> [FAILURE!]: {e}")
        except Exception as e:
            logger.error(f"<YandexDisk cloud access error> [FAILURE!]: {e}")

        return uploaded  # maybe better to raise exception

    async def copy_file(self, source_path: str, copy_path: str, **kwargs) -> bool:
        uploaded = False
        try:
            async with YaDisk(token=self.token) as disk:
                try:
                    await disk.copy(
                        source_path,
                        copy_path,
                        force_async=True,
                        overwrite=True,
                        n_retries=3,
                        **kwargs,
                    )
                    uploaded = True
                    logger.info("<YandexDisk.copy_file> [SUCCESS!]")
                except Exception as e:
                    logger.error(f"<YandexDisk.copy_file> [FAILURE!]: {e}")
        except Exception as e:
            logger.error(f"<YandexDisk cloud access error> [FAILURE!]: {e}")

        return uploaded  # maybe better to raise exception

    async def get_file_info(self, source_path: str, **kwargs):
        file_info = None
        try:
            async with YaDisk(token=self.token) as disk:
                try:
                    file_info = await disk.get_meta(
                        source_path,
                        n_retries=3,
                        **kwargs,
                    )
                    logger.info("<YandexDisk.get_meta> [SUCCESS!]")
                except Exception as e:
                    logger.error(f"<YandexDisk.get_meta> [FAILURE!]: {e}")
        except Exception as e:
            logger.error(f"<YandexDisk cloud access error> [FAILURE!]: {e}")

        return file_info  # maybe better to raise exception


class CloudStorageManager:
    async def connect(self): ...

    async def close(self): ...
