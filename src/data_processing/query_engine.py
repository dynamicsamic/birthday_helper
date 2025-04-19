import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Protocol

from pandas import DataFrame, Series

from src.logging import get_logger

QueryFunc = Callable[[Series], Series]
logger = get_logger(__name__)


class QueryEngineProtocol(Protocol):
    async def filter(self, df: DataFrame, query: QueryFunc) -> DataFrame: ...


class QueryEngine:
    def __init__(self, thread_pool_executor: ThreadPoolExecutor):
        self._executor = thread_pool_executor

    async def filter(self, df: DataFrame, query: QueryFunc) -> DataFrame:
        logger.debug("Filtering data...")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self._run_query, df, query)

    def _run_query(self, df: DataFrame, query: QueryFunc) -> DataFrame:
        return df.loc[df.apply(query, axis=1)]
