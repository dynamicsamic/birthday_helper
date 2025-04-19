import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable, Iterable, Protocol

from pandas import DataFrame

from src.exceptions import DataCacheEmptyCacheError, DataCacheUpdateError
from src.logging import get_logger
from src.utils import now

DataPreprocessor = Callable[[DataFrame], DataFrame]
logger = get_logger(__name__)


class DataCacheProtocol(Protocol):
    last_refreshed: datetime | None

    async def get_data(self) -> Iterable: ...
    async def update_data(self, data: Iterable) -> None: ...


class DataFrameBasedCache:
    # Maybe add data validation logic?
    # This would be smth similar to DataPreprocessor
    def __init__(
        self,
        thread_pool_executor: ThreadPoolExecutor,
        preprocessors: list[DataPreprocessor] | None = None,
    ):
        """
        Initialize DataFrameBasedCache instance.

        :param thread_pool_executor: a ThreadPoolExecutor for running data
            preprocessors in separate threads
        :param preprocessors: an optional list of functions to run on the data
            after it is loaded into the cache
        """
        self._executor = thread_pool_executor
        self._preprocessors = preprocessors or []
        self._data: DataFrame = None
        self._last_refreshed: datetime = None
        self._lock = asyncio.Lock()

    @property
    def last_refreshed(self) -> datetime | None:
        return self._last_refreshed

    async def get_data(self) -> DataFrame:
        """
        Get a copy of the data from the cache.

        :raises EmptyDataCacheException: if called before data is loaded
        :return: a copy of the data in the cache
        """
        async with self._lock:
            if self._data is None:
                logger.error("Attempt to get data before data loaded into cache.")
                raise DataCacheEmptyCacheError("Data not loaded into cache.")

            logger.info("Returning data from cache.")
            # Return a copy so that the caller doesn't modify the internal cache.
            return self._data.copy()

    async def update_data(self, data: DataFrame):
        """
        Update the data in the cache.

        :param data: the new data to put in the cache
        :raises UpdateDataCacheException: if an error occurs during data
            transformation
        :return: None
        """
        async with self._lock:
            loop = asyncio.get_running_loop()
            try:
                transformed = await loop.run_in_executor(
                    self._executor,
                    self._apply_preprocessors,
                    data,
                )
            except Exception as e:
                logger.error(f"Error during data transformation: {e}")
                raise DataCacheUpdateError from e

            self._data = transformed
            self._last_refreshed = now()
            logger.info("Updated data in cache.")

    def update_preprocessors(
        self,
        preprocessors: list[DataPreprocessor],
        replace: bool = True,
    ):
        """
        Update the preprocessors to be run on the data.

        :param preprocessors: the new preprocessors to run. If `replace` is
            `True`, these will replace the existing preprocessors. If `replace`
            is `False`, these preprocessors will be added to the existing list.
        :param replace: If `True`, replace the existing preprocessors with the
            given preprocessors. If `False`, add the given preprocessors to the
            existing list.
        :return: None
        """
        self._preprocessors = (
            preprocessors if replace else self._preprocessors + preprocessors
        )

    def _apply_preprocessors(self, data: DataFrame) -> DataFrame:
        """
        Applies the preprocessors to the given DataFrame.

        This method applies each preprocessor in the order they were added to the
        cache. If a preprocessor raises an exception, it is logged and the
        exception is propagated.

        :param data: the DataFrame to apply the preprocessors to
        :return: a new DataFrame with the preprocessors applied
        """
        result = data.copy()
        for func in self._preprocessors:
            logger.info(f"Applying preprocessor: {func.__name__}")
            result = func(result)
        return result
