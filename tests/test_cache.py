import asyncio
import random
import string
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
import pytest_asyncio

from src.data_processing.cache import DataFrameBasedCache
from src.exceptions import DataCacheEmptyCacheError


@pytest_asyncio.fixture
async def df():
    def build_dataframe():
        names = [
            "".join(random.sample(string.ascii_letters, random.randint(5, 20)))
            for _ in range(1000)
        ]
        ages = [random.randint(10, 50) for _ in range(1000)]
        return pd.DataFrame(
            {
                "name": names,
                "age": ages,
            }
        )

    return await asyncio.to_thread(build_dataframe)


@pytest_asyncio.fixture
async def cache():
    executor = ThreadPoolExecutor()
    return DataFrameBasedCache(executor)


@pytest.mark.asyncio
async def test_update_data(df: pd.DataFrame, cache: DataFrameBasedCache):
    await cache.update_data(df)
    assert (await cache.get_data()).equals(df)


@pytest.mark.asyncio
async def test_update_data_with_preprocessors(
    df: pd.DataFrame, cache: DataFrameBasedCache
):
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        return data * 2

    cache.update_preprocessors([transform, transform])
    await cache.update_data(df)
    assert (await cache.get_data()).equals(df * 4)


@pytest.mark.asyncio
async def test_original_data_not_modified_after_update(
    df: pd.DataFrame, cache: DataFrameBasedCache
):
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        return data * 3

    cache.update_preprocessors([transform])
    await cache.update_data(df)
    assert (await cache.get_data()).equals(df * 3)
    assert df.equals(df)


@pytest.mark.asyncio
async def test_get_data_not_loaded(cache: DataFrameBasedCache):
    with pytest.raises(DataCacheEmptyCacheError):
        await cache.get_data()


@pytest.mark.asyncio
async def test_last_refreshed(df: pd.DataFrame, cache: DataFrameBasedCache):
    assert cache.last_refreshed is None
    await cache.update_data(df)
    assert cache.last_refreshed is not None

    await asyncio.sleep(1)
    await cache.update_data(df)
    assert cache.last_refreshed is not None


@pytest.mark.asyncio
async def test_cache_performance(df: pd.DataFrame, cache: DataFrameBasedCache):
    import time

    async with asyncio.TaskGroup() as tg:
        start = time.monotonic()
        for i in range(1000):
            tg.create_task(cache.update_data(df))

    print(f"execution took {time.monotonic() - start} sec")
