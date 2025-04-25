import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import pandas as pd
import pytest
import pytest_asyncio
from faker import Faker
from pandas.testing import assert_frame_equal

from src.data_processing.file_handler import ExcelFileHandler

pytestmark = pytest.mark.asyncio

# Performance test constants
SMALL_FILE_ROWS = 100
MEDIUM_FILE_ROWS = 1_000
LARGE_FILE_ROWS = 10_000
XLARGE_FILE_ROWS = 50_000  # For stress testing


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest_asyncio.fixture(scope="session")
async def thread_pool():
    pool = None
    try:
        pool = ThreadPoolExecutor()
        yield pool
    finally:
        if pool:
            pool.shutdown()


def _fake():
    return Faker()


@pytest_asyncio.fixture(scope="session")
async def fake(thread_pool):
    loop = asyncio.get_running_loop()
    faked = await loop.run_in_executor(thread_pool, _fake)
    yield faked


def _performance_temp_files(tmp_path: Path, fake: Faker):
    """Create test files of different sizes for performance testing"""
    tmp_path.mkdir(parents=True, exist_ok=True)
    files = {}

    for size, rows in [
        ("small", SMALL_FILE_ROWS),
        ("medium", MEDIUM_FILE_ROWS),
        ("large", LARGE_FILE_ROWS),
        ("xlarge", XLARGE_FILE_ROWS),
    ]:
        path = tmp_path / f"test_{size}.xlsx"
        backup_dir = tmp_path / f"backups_{size}"

        # Generate test data
        data = {
            "id": [str(i) for i in range(rows)],
            "name": [fake.name() for _ in range(rows)],
            "email": [fake.email() for _ in range(rows)],
            "age": [str(fake.random_int(18, 80)) for _ in range(rows)],
            "salary": [str(fake.random_int(30_000, 150_000)) for _ in range(rows)],
        }
        df = pd.DataFrame(data)
        df.to_excel(path, index=False, engine="openpyxl")

        files[size] = (path, backup_dir)

    return files


@pytest_asyncio.fixture(scope="session")
async def performance_temp_files(thread_pool: ThreadPoolExecutor, fake: Faker):
    temp_files = None
    test_file_path = Path("/home/sammi/dev/birthday_helper/tests/excel_file_handler")
    # fake = fake
    loop = asyncio.get_running_loop()
    try:
        temp_files = await loop.run_in_executor(
            thread_pool,
            _performance_temp_files,
            test_file_path,
            fake,
        )
        yield temp_files
    finally:
        if temp_files:
            await loop.run_in_executor(thread_pool, shutil.rmtree, test_file_path)


@pytest.mark.performance
async def test_read_performance(
    thread_pool: ThreadPoolExecutor,
    performance_temp_files: dict[str, tuple[Path, Path]],
):
    """Test read performance with different file sizes"""
    results = {}

    for size in ["small", "medium", "large", "xlarge"]:
        path, backup_dir = performance_temp_files[size]
        handler = ExcelFileHandler(
            file_path=path,
            backup_dir=backup_dir,
            thread_pool_executor=thread_pool,
        )

        # Warm up
        await handler.read_file()

        # Time the operation
        start = time.perf_counter()
        await handler.read_file()
        elapsed = time.perf_counter() - start

        results[size] = elapsed
        print(f"Read {size} file ({path.stat().st_size / 1024:.1f} KB): {elapsed:.3f}s")

    # Assert that larger files take proportionally longer
    assert results["medium"] > results["small"] * 0.8  # At least 80% of small time
    assert results["large"] > results["medium"] * 0.8
    assert results["xlarge"] > results["large"] * 0.8


@pytest.mark.performance
async def test_write_performance(
    thread_pool: ThreadPoolExecutor,
    performance_temp_files: dict[str, tuple[Path, Path]],
    fake: Faker,
):
    """Test write performance with different file sizes"""
    results = {}

    for size in ["small", "medium", "large", "xlarge"]:
        path, backup_dir = performance_temp_files[size]
        handler = ExcelFileHandler(
            file_path=str(path),
            backup_dir=str(backup_dir),
            thread_pool_executor=thread_pool,
        )

        # Read the data first
        df = await handler.read_file()

        # Modify some data
        df.loc[0, "name"] = fake.name()

        # Time the write operation
        start = time.perf_counter()
        await handler.write_file(df)
        elapsed = time.perf_counter() - start

        results[size] = elapsed
        print(
            f"Write {size} file ({path.stat().st_size / 1024:.1f} KB): {elapsed:.3f}s"
        )

    # Basic sanity checks on timing
    assert results["medium"] > results["small"] * 0.8
    assert results["large"] > results["medium"] * 0.8
    assert results["xlarge"] > results["large"] * 0.8


@pytest.mark.performance
async def test_concurrent_operations_performance(
    thread_pool: ThreadPoolExecutor,
    performance_temp_files: dict[str, tuple[Path, Path]],
    fake: Faker,
):
    """Test performance with concurrent operations"""
    path, backup_dir = performance_temp_files["medium"]
    handler = ExcelFileHandler(
        file_path=str(path),
        backup_dir=str(backup_dir),
        thread_pool_executor=thread_pool,
    )

    # Number of concurrent operations
    num_operations = 50

    async def perform_operation(idx):
        # Alternate between read, append, update
        if idx % 3 == 0:
            await handler.read_file()
        elif idx % 3 == 1:
            await handler.append_row(
                [
                    str(SMALL_FILE_ROWS + idx),
                    fake.name(),
                    fake.email(),
                    str(fake.random_int(18, 80)),
                    fake.random_int(30_000, 150_000),
                ]
            )
        else:

            def query(row):
                return row["id"] == str(idx % SMALL_FILE_ROWS)

            await handler.update_rows(query, {"name": fake.name()})

    # Time concurrent operations
    start = time.perf_counter()
    await asyncio.gather(*[perform_operation(i) for i in range(num_operations)])
    elapsed = time.perf_counter() - start

    print(f"Completed {num_operations} concurrent operations in {elapsed:.3f}s")
    print(f"Average operation time: {elapsed / num_operations:.3f}s")

    # Verify all operations completed
    final_data = await handler.read_file()
    assert len(final_data) >= SMALL_FILE_ROWS  # At least original + some appends


@pytest.mark.performance
async def test_lock_contention_performance(
    thread_pool: ThreadPoolExecutor,
    performance_temp_files: dict[str, tuple[Path, Path]],
):
    """Test performance impact of lock contention"""
    path, backup_dir = performance_temp_files["medium"]
    handler = ExcelFileHandler(
        file_path=str(path),
        backup_dir=str(backup_dir),
        thread_pool_executor=thread_pool,
    )

    # Number of concurrent writers
    num_writers = 10

    async def writer_task(idx):
        # async with handler.get_lock():
        # Simulate work while holding the lock
        await asyncio.sleep(0.01)
        df = await handler.read_file()
        df.loc[0, "name"] = f"Writer{idx}"
        await handler.write_file(df)

    # Time concurrent writers
    start = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        for i in range(num_writers):
            tg.create_task(writer_task(i))
    elapsed = time.perf_counter() - start

    # # Time concurrent writers
    # start = time.perf_counter()
    # for i in range(num_writers):
    #     await writer_task(i)
    # elapsed = time.perf_counter() - start

    # # Time concurrent writers
    # start = time.perf_counter()
    # await asyncio.gather(*[writer_task(i) for i in range(num_writers)])
    # elapsed = time.perf_counter() - start

    print(f"{num_writers} concurrent writers completed in {elapsed:.3f}s")
    print(f"Average writer time: {elapsed / num_writers:.3f}s")

    # Verify final state
    final_data = await handler.read_file()
    assert final_data.loc[0, "name"].startswith("Writer")


@pytest.mark.performance
async def test_backup_restore_performance(
    thread_pool: ThreadPoolExecutor,
    performance_temp_files: dict[str, tuple[Path, Path]],
):
    """Test performance of backup and restore operations"""
    path, backup_dir = performance_temp_files["large"]
    handler = ExcelFileHandler(
        file_path=str(path),
        backup_dir=str(backup_dir),
        thread_pool_executor=thread_pool,
    )

    # Time backup
    start = time.perf_counter()
    backup_path = await handler.create_backup()
    backup_elapsed = time.perf_counter() - start
    print(f"Backup created in {backup_elapsed:.3f}s")

    # Time restore
    start = time.perf_counter()
    await handler.restore_backup(Path(backup_path))
    restore_elapsed = time.perf_counter() - start
    print(f"Restore completed in {restore_elapsed:.3f}s")

    # Verify
    assert Path(backup_path).exists()
    restored_data = await handler.read_file()
    original_data = pd.read_excel(backup_path, dtype=str)
    assert_frame_equal(restored_data, original_data)


@pytest.mark.performance
async def test_bulk_operations_performance(
    thread_pool: ThreadPoolExecutor,
    performance_temp_files: dict[str, tuple[Path, Path]],
    fake: Faker,
):
    """Test performance of bulk operations"""
    path, backup_dir = performance_temp_files["medium"]
    handler = ExcelFileHandler(
        file_path=str(path),
        backup_dir=str(backup_dir),
        thread_pool_executor=thread_pool,
    )

    # Test bulk append
    new_rows = [
        [
            str(LARGE_FILE_ROWS + i),
            fake.name(),
            fake.email(),
            str(fake.random_int(18, 80)),
            fake.random_int(30_000, 150_000),
        ]
        for i in range(10)
    ]

    start = time.perf_counter()
    for row in new_rows:
        await handler.append_row(row)
    elapsed = time.perf_counter() - start
    print(f"Appended {len(new_rows)} rows in {elapsed:.3f}s")

    # Test bulk update
    def query(row):
        return int(row["id"]) % 10 == 0  # Update every 10th row

    update = {"name": "UPDATED", "salary": "999999"}

    start = time.perf_counter()
    updated_count = await handler.update_rows(query, update)
    elapsed = time.perf_counter() - start
    print(f"Updated {updated_count} rows in {elapsed:.3f}s")

    # Verify operations
    final_data = await handler.read_file()
    assert len(final_data) == MEDIUM_FILE_ROWS + len(new_rows)
    assert final_data[final_data["name"] == "UPDATED"].shape[0] == updated_count
