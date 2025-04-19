"""
This file is meant for unit testing the ExcelFileHandler class.

Need to implement tests that cover all public methods.

"""

import asyncio
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
import pytest_asyncio

from src.data_processing.atomic_operation import AtomicOperation
from src.data_processing.file_handler import ExcelFileHandler
from src.exceptions import (
    FileHandlerOperationError,
    FileHandlerReadFileError,
    FileHandlerWriteFileError,
)


def create_test_file(file_path: str) -> Path:
    file = Path(file_path)
    df = pd.DataFrame({"Day": [1, 2, 3], "Month": [1, 2, 3], "Name": ["a", "b", "c"]})
    df.to_excel(file, index=False)
    return file


@pytest_asyncio.fixture
async def thread_pool():
    pool = None
    try:
        pool = ThreadPoolExecutor()
        yield pool
    finally:
        if pool:
            pool.shutdown()


@pytest_asyncio.fixture
async def test_file(thread_pool: ThreadPoolExecutor):
    temp_file = None
    loop = asyncio.get_running_loop()
    try:
        temp_file = await loop.run_in_executor(
            thread_pool, create_test_file, "tests/test_file.xlsx"
        )
        yield temp_file
    finally:
        if temp_file and temp_file.exists():
            temp_file.unlink()


@pytest_asyncio.fixture
async def handler(thread_pool: ThreadPoolExecutor, test_file: Path):
    excel_handler = None
    try:
        excel_handler = ExcelFileHandler(
            test_file, "tests/excel_file_handler/backups/", thread_pool
        )
        yield excel_handler
    finally:
        if excel_handler:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(thread_pool, shutil.rmtree(excel_handler.backup_dir))
            del excel_handler


@pytest.mark.asyncio
async def test_read_file(handler: ExcelFileHandler):
    async def mock_handler_read_excel_file():
        loop = asyncio.get_running_loop()
        df = await loop.run_in_executor(
            handler.executor, partial(pd.read_excel, handler.file_path, dtype=str)
        )
        return df

    data = await handler.read_file()
    df = await mock_handler_read_excel_file()
    assert data.equals(df)


@pytest.mark.skip
@pytest.mark.asyncio
async def test_read_file_performance(handler: ExcelFileHandler):
    async with asyncio.TaskGroup() as tg:
        start = time.monotonic()
        for i in range(1000):
            tg.create_task(handler.read_file())

    assert (time.monotonic() - start) < 20


@pytest.mark.asyncio
async def test_append_row(handler: ExcelFileHandler):
    original_data = await handler.read_file()
    row = {"Day": 24, "Month": 11, "Name": "test"}
    await handler.append_row(row)
    updated_data = await handler.read_file()

    # convert to str as all data types returned by read_file method are strings.
    row["Day"] = str(row["Day"])
    row["Month"] = str(row["Month"])
    assert row in updated_data.to_dict("records")
    assert updated_data.shape[0] - original_data.shape[0] == 1


@pytest.mark.asyncio
@patch.object(ExcelFileHandler, "read_file", side_effect=FileHandlerReadFileError)
async def test_append_row_read_error(mock: AsyncMock, handler: ExcelFileHandler):
    with pytest.raises(FileHandlerReadFileError) as e:
        await handler.append_row({"Day": 24, "Month": 11, "Name": "test"})
    assert e.type == FileHandlerReadFileError
    mock.assert_awaited_once()


@pytest.mark.asyncio
@patch.object(ExcelFileHandler, "write_file", side_effect=FileHandlerWriteFileError)
async def test_append_row_write_error(mock: AsyncMock, handler: ExcelFileHandler):
    with pytest.raises(FileHandlerWriteFileError) as e:
        await handler.append_row({"Day": 24, "Month": 11, "Name": "test"})
    assert e.type == FileHandlerWriteFileError
    mock.assert_awaited_once()


@pytest.mark.asyncio
@patch.object(AtomicOperation, "__aexit__", side_effect=FileHandlerOperationError)
async def test_append_row_unexpected_error(mock: AsyncMock, handler: ExcelFileHandler):
    with pytest.raises(FileHandlerOperationError) as e:
        await handler.append_row({"Day": 24, "Month": 11, "Name": "test"})
    assert e.type == FileHandlerOperationError
    mock.assert_awaited_once()


@pytest.mark.skip
@pytest.mark.asyncio
async def test_append_row_performance(handler: ExcelFileHandler):
    async with asyncio.TaskGroup() as tg:
        start = time.monotonic()
        for i in range(100):
            tg.create_task(handler.append_row({"Day": 24, "Month": 11, "Name": "test"}))

    assert (time.monotonic() - start) < 10


@pytest.mark.asyncio
async def test_update_rows(handler: ExcelFileHandler):
    await handler.read_file()
    data = {"Month": 12}
    num_updated = await handler.update_rows(
        lambda row: row["Month"] == "1" and row["Day"] == "1", data
    )
    updated_df = await handler.read_file()
    data["Month"] = str(data["Month"])
    assert not updated_df.query(f"Month == '{data['Month']}'").empty
    assert num_updated > 0


@pytest.mark.asyncio
async def test_delete_rows():
    pass
