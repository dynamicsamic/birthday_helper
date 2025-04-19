import asyncio
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
import pytest_asyncio

from src.data_processing.query_engine import QueryEngine


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
async def query_engine():
    executor = ThreadPoolExecutor()
    return QueryEngine(executor)


@pytest.mark.asyncio
async def test_query_engine(df: pd.DataFrame, query_engine: QueryEngine):
    filtered_df = await query_engine.filter(df, lambda df: df["age"] > 30)
    assert len(filtered_df) == len(df[df["age"] > 30])


@pytest.mark.asyncio
async def test_query_engine_performance(df: pd.DataFrame, query_engine: QueryEngine):
    async with asyncio.TaskGroup() as tg:
        start = time.monotonic()
        for i in range(1000):
            tg.create_task(query_engine.filter(df, lambda df: df["age"] > 30))

    print(f"execution took {time.monotonic() - start} sec")
