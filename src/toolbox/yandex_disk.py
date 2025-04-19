import logging
from logging.config import fileConfig

from yadisk_async import YaDisk

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class CloudStorageManager:
    async def connect(self): ...

    async def close(self): ...


class YandexDisk(YaDisk):
    """
    A wrapper class with `YaDisk.download` and `YaDisk.upload`
    methods that handle errors and close sessions.
    """

    __doc__ = YaDisk.__doc__

    async def download_file(
        self, remote_filepath: str, local_filepath: str, **kwargs
    ) -> bool:
        """Asynchronously download file from Yandex.Disk.

        :param remote_filepath: Path to remote file on `Yandex Disk`
        :param local_filepath: Path to local file that contents
            of a remote file would be downloaded into
        :param kwargs: Valid `YaDisk.dowload` method keyword arguments.
        :returns: Boolean result of download: `True` if no exception raised,
            `False` otherwise.
        """
        try:
            await self.download(remote_filepath, local_filepath, **kwargs)
            downloaded = True
            logger.info("<YandexDisk.download_file> [SUCCESS!]")
        except Exception as e:
            downloaded = False
            logger.error(f"<YandexDisk.download_file> [FAILURE!]: {e}")
        finally:
            await self.close()
            return downloaded

    async def upload_file(
        self, local_filepath: str, remote_filepath: str, **kwargs
    ) -> bool:
        """Asynchronously upload file to Yandex.Disk.

        :param local_filepath: Path to local file that would be uploaded
        :param remote_filepath: Path to remote file on `Yandex Disk`
        :param kwargs: Valid `YaDisk.upload` method keyword arguments.
        :returns: Boolean result of upload: `True` if no exception raised,
            `False` otherwise.
        """
        try:
            await self.upload(
                local_filepath,
                remote_filepath,
                **kwargs,
            )
            uploaded = True
            logger.info("<YandexDisk.upload_file> [SUCCESS!]")
        except Exception as e:
            uploaded = False
            logger.info(f"<YandexDisk.upload_file> [FAILURE!]: {e}")
        finally:
            await self.close()
            return uploaded
