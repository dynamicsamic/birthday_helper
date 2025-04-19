import pytest

from src import settings
from src.toolbox.yandex_disk import YandexDisk

from .common import constants
from .fixtures.files import temp_file


@pytest.mark.asyncio
async def test_download_from_yadisk_returns_false_if_invalid_token():
    disk = YandexDisk(token="invalid")
    local_filepath = settings.BASE_DIR / "test.xlsx"
    downloaded = await disk.download_file(
        settings.YADISK_FILEPATH, local_filepath.as_posix()
    )
    assert downloaded == False


@pytest.mark.asyncio
async def test_download_from_yadisk_returns_false_if_invalid_remote_file_path():
    disk = YandexDisk(token=settings.YADISK_TOKEN)
    local_filepath = settings.BASE_DIR / "test.xlsx"
    downloaded = await disk.download_file(
        "invalid_remote_file_path", local_filepath.as_posix()
    )
    assert downloaded == False


@pytest.mark.asyncio
async def test_download_from_yadisk_returns_false_if_invalid_local_file_path():
    disk = YandexDisk(token=settings.YADISK_TOKEN)
    downloaded = await disk.download_file(
        settings.YADISK_FILEPATH, "/invalid/local/file:path"
    )
    assert downloaded == False


@pytest.mark.asyncio
async def test_download_from_yadisk_returns_true_with_valid_args(temp_file):
    disk = YandexDisk(token=settings.YADISK_TOKEN)
    local_filepath = constants["TEMP_FILE"]
    downloaded = await disk.download_file(
        settings.YADISK_FILEPATH, local_filepath.as_posix()
    )
    assert downloaded == True
