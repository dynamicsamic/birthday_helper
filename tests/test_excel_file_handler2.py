import asyncio
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.data_processing.atomic_operation import AtomicOperation
from src.data_processing.file_handler import ExcelFileHandler
from src.exceptions import (
    FileHandlerOperationError,
    FileHandlerReadFileError,
    FileHandlerWriteFileError,
)

# Test data
TEST_DATA = pd.DataFrame(
    {
        "id": ["1", "2", "3"],
        "name": ["Alice", "Bob", "Charlie"],
        "age": ["25", "30", "35"],
    }
)


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
    df = pd.DataFrame(TEST_DATA)
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
async def excel_handler(thread_pool: ThreadPoolExecutor, test_file: Path):
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
async def test_read_file(excel_handler: ExcelFileHandler):
    # Test reading the file
    result = await excel_handler.read_file()
    assert_frame_equal(result, TEST_DATA)


@pytest.mark.asyncio
async def test_read_file_error(excel_handler, monkeypatch):
    # Test read error handling
    def mock_read(*args, **kwargs):
        raise Exception("Test error")

    monkeypatch.setattr(pd, "read_excel", mock_read)

    with pytest.raises(FileHandlerReadFileError):
        await excel_handler.read_file()


@pytest.mark.asyncio
async def test_write_file(excel_handler: ExcelFileHandler):
    # Test writing the file
    new_data = pd.DataFrame({"id": ["4"], "name": ["Dave"], "age": ["40"]})
    await excel_handler.write_file(new_data)

    # Verify by reading back
    result = await excel_handler.read_file()
    assert_frame_equal(result, new_data)


@pytest.mark.asyncio
async def test_write_file_error(excel_handler, monkeypatch):
    # Test write error handling
    def mock_write(*args, **kwargs):
        raise Exception("Test error")

    monkeypatch.setattr(pd.DataFrame, "to_excel", mock_write)

    with pytest.raises(FileHandlerWriteFileError):
        await excel_handler.write_file(TEST_DATA)


@pytest.mark.asyncio
async def test_append_row(excel_handler: ExcelFileHandler):
    # Test appending a row
    new_row = {"id": "4", "name": "Dave", "age": "40"}
    await excel_handler.append_row(new_row)

    # Verify by reading back
    result = await excel_handler.read_file()
    expected = pd.concat([TEST_DATA, pd.DataFrame([new_row])], ignore_index=True)
    assert_frame_equal(result, expected)


@pytest.mark.asyncio
async def test_update_rows(excel_handler: ExcelFileHandler):
    # Test updating rows
    def query(row):
        return row["id"] == "2"

    update = {"name": "Robert", "age": "31"}
    num_updated = await excel_handler.update_rows(query, update)

    assert num_updated == 1

    # Verify by reading back
    result = await excel_handler.read_file()
    expected = TEST_DATA.copy()
    expected.loc[expected["id"] == "2", ["name", "age"]] = ["Robert", "31"]
    assert_frame_equal(result, expected)


@pytest.mark.asyncio
async def test_delete_rows(excel_handler: ExcelFileHandler):
    # Test deleting rows
    def query(row):
        return row["id"] == "2"

    num_deleted = await excel_handler.delete_rows(query)

    assert num_deleted == 1

    # Verify by reading back
    result = await excel_handler.read_file()
    expected = TEST_DATA[TEST_DATA["id"] != "2"].reset_index(drop=True)
    assert_frame_equal(result, expected)


@pytest.mark.asyncio
async def test_last_modified(excel_handler: ExcelFileHandler):
    # Test last_modified property
    original_time = excel_handler.last_modified

    # Modify the file
    new_data = pd.DataFrame({"id": ["4"], "name": ["Dave"], "age": ["40"]})
    await excel_handler.write_file(new_data)

    new_time = excel_handler.last_modified
    assert new_time > original_time


@pytest.mark.asyncio
async def test_create_backup(excel_handler: ExcelFileHandler):
    # Test backup creation
    backup_path = await excel_handler.create_backup()

    assert Path(backup_path).exists()

    # Verify backup content
    backup_data = pd.read_excel(backup_path, dtype=str)
    assert_frame_equal(backup_data, TEST_DATA)


@pytest.mark.asyncio
async def test_restore_backup(excel_handler: ExcelFileHandler):
    # Create a backup first
    backup_path = await excel_handler.create_backup()

    # Modify the original file
    new_data = pd.DataFrame({"id": ["4"], "name": ["Dave"], "age": ["40"]})
    await excel_handler.write_file(new_data)

    # Restore from backup
    await excel_handler.restore_backup(Path(backup_path))

    # Verify content is restored
    result = await excel_handler.read_file()
    assert_frame_equal(result, TEST_DATA)


@pytest.mark.asyncio
async def test_concurrent_writes(excel_handler: ExcelFileHandler):
    # Test that concurrent writes are properly synchronized
    async def append_multiple_rows():
        for i in range(5, 9):
            await excel_handler.append_row(
                {"id": str(i), "name": f"User{i}", "age": str(20 + i)}
            )

    # Run multiple concurrent append operations
    tasks = [asyncio.create_task(append_multiple_rows()) for _ in range(3)]
    await asyncio.gather(*tasks)

    # Verify all rows were appended exactly once (no duplicates)
    result = await excel_handler.read_file()
    assert len(result) == len(TEST_DATA) + 4  # 4 new unique rows


@pytest.mark.asyncio
async def test_atomic_operation_rollback(excel_handler, monkeypatch):
    # Test that atomic operation rolls back on failure
    original_data = await excel_handler.read_file()

    # Mock the write operation to fail after successful read
    original_write = excel_handler.write_file

    async def mock_write(*args, **kwargs):
        raise Exception("Simulated write error")

    try:
        # Patch the write method to fail
        monkeypatch.setattr(excel_handler, "write_file", mock_write)

        # Attempt an update that should fail
        with pytest.raises(FileHandlerOperationError):
            await excel_handler.update_rows(lambda row: True, {"name": "ShouldFail"})
    finally:
        # Restore original method
        monkeypatch.setattr(excel_handler, "write_file", original_write)

    # Verify file content wasn't changed
    current_data = await excel_handler.read_file()
    assert_frame_equal(current_data, original_data)


def test_write_lock_management():
    # Test that write locks are properly managed per file path
    path1 = Path("/test/file1.xlsx")
    path2 = Path("/test/file2.xlsx")

    # Get locks for different paths
    lock1 = ExcelFileHandler._get_write_lock(path1)
    lock2 = ExcelFileHandler._get_write_lock(path2)
    lock1_again = ExcelFileHandler._get_write_lock(path1)

    assert lock1 is not lock2
    assert lock1 is lock1_again


@pytest.mark.asyncio
async def test_in_memory_operations():
    # Test the in-memory operations directly
    handler = ExcelFileHandler(
        file_path="dummy.xlsx",
        backup_dir="dummy_backups",
        thread_pool_executor=ThreadPoolExecutor(),
    )

    # Test append
    appended = handler._in_memory_append_row(
        TEST_DATA, {"id": "4", "name": "Dave", "age": "40"}
    )
    assert len(appended) == len(TEST_DATA) + 1

    # Test update
    def query(row):
        return row["id"] == "2"

    updated, count = handler._in_memory_update_rows(
        TEST_DATA, query, {"name": "Robert"}
    )
    assert count == 1
    assert updated.loc[updated["id"] == "2", "name"].iloc[0] == "Robert"

    # Test delete
    deleted, count = handler._in_memory_delete_rows(TEST_DATA, query)
    assert count == 1
    assert len(deleted) == len(TEST_DATA) - 1
    assert "2" not in deleted["id"].values
