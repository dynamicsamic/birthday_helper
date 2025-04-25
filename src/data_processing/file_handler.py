import asyncio
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any, Callable, Iterable, Literal

import pandas as pd
from openpyxl import load_workbook

from src.data_processing.atomic_operation import (
    AtomicOperation,
    WriteSafeFileHandlerProtocol,
)
from src.exceptions import (
    FileHandlerOperationError,
    FileHandlerReadFileError,
    FileHandlerWriteFileError,
)
from src.logging import get_logger

SupportedExcelEngines = Literal["openpyxl", "calamine", "odf", "pyxlsb", "xlrd"]
QueryFunction = Callable[[pd.Series], bool]

logger = get_logger(__name__)


class FileHandlerProtocol(WriteSafeFileHandlerProtocol):
    atomic_write: AtomicOperation

    async def read_file(self) -> Iterable:
        ...

    async def append_row(self, row: dict):
        ...

    async def update_rows(self, query: Callable, rows: dict) -> int:
        ...

    async def delete_rows(self, query: Callable) -> int:
        ...

    @property
    def last_modified(self) -> float:
        ...


class ExcelFileHandler:
    _write_locks = {}

    def __init__(
        self,
        file_path: str | Path,
        backup_dir: str | Path,
        thread_pool_executor: ThreadPoolExecutor,
        excel_engine: SupportedExcelEngines = "openpyxl",
    ):
        self.file_path = Path(file_path)
        self.backup_dir = Path(backup_dir)
        self.executor = thread_pool_executor
        self.engine = excel_engine
        self.write_lock: asyncio.Lock = self._get_write_lock(self.file_path)
        self.atomic_write = AtomicOperation(self)
        self._ensure_directories()

    @property
    def last_modified(self) -> float:
        return self.file_path.stat().st_mtime

    async def read_file(self, **kwargs) -> pd.DataFrame:
        try:
            data = await self._run_in_thread(partial(self._sync_read, **kwargs))
            logger.info(f"Excel file read successfully: {self.file_path}")
            return data
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise FileHandlerReadFileError from e

    async def write_file(self, data: pd.DataFrame, **kwargs):
        async with self.atomic_write:
            try:
                await self._run_in_thread(partial(self._sync_write, data, **kwargs))
                logger.info(f"Excel file written successfully: {self.file_path}")
            except Exception as e:
                logger.error(f"Error writing Excel file: {e}")
                raise FileHandlerWriteFileError from e

    async def append_row(self, row: list[str | int]) -> bool:
        result = False
        async with self.atomic_write:
            try:
                result = await self._run_in_thread(partial(self._sync_append_row, row))
            except Exception as e:
                logger.error(f"Error appending row to Excel file: {e}")
                raise FileHandlerOperationError from e

        return result

    async def update_rows(self, query: QueryFunction, update: dict):
        # async with self.atomic_write:
        try:
            df = await self.read_file()
            updated_df, num_updated = await self._run_in_thread(
                partial(self._in_memory_update_rows, df, query, update)
            )
            await self.write_file(updated_df)
            logger.info(f"Rows updated in Excel file: {self.file_path}")
            return num_updated
        except FileHandlerReadFileError as e:
            logger.error(f"Error reading Excel file: {e}")
            raise
        except FileHandlerWriteFileError as e:
            logger.error(f"Error writing updated Excel file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error updating rows in Excel file: {e}")
            raise FileHandlerOperationError from e

    async def delete_rows(self, query: QueryFunction):
        # async with self.atomic_write:
        try:
            df = await self.read_file()
            filtered_df, num_deleted = await self._run_in_thread(
                partial(self._in_memory_delete_rows, df, query)
            )
            await self.write_file(filtered_df)
            logger.info(f"Rows deleted from Excel file: {self.file_path}")
            return num_deleted
        except FileHandlerReadFileError as e:
            logger.error(f"Error reading Excel file: {e}")
            raise
        except FileHandlerWriteFileError as e:
            logger.error(f"Error writing updated Excel file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error deleting rows from Excel file: {e}")
            raise FileHandlerOperationError from e

    def get_lock(self) -> asyncio.Lock:
        return self.write_lock

    async def create_backup(self) -> Path:
        backup_path = self._create_backup_path()
        logger.info(f"Creating backup of {self.file_path} at {backup_path}")
        await self._run_in_thread(partial(shutil.copy2, self.file_path, backup_path))
        return backup_path

    async def restore_backup(self, backup_path: Path):
        logger.info(f"Restoring backup from {backup_path} to {self.file_path}")
        await self._run_in_thread(partial(shutil.copy2, backup_path, self.file_path))

    def _sync_read(self, **kwargs) -> pd.DataFrame:
        logger.debug(f"Synchronously reading Excel file: {self.file_path}")
        return pd.read_excel(self.file_path, engine=self.engine, dtype=str, **kwargs)

    def _sync_write(self, df: pd.DataFrame, **kwargs):
        logger.debug(f"Synchronously writing Excel file: {self.file_path}")
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            df.to_excel(tmp.name, index=False, engine=self.engine, **kwargs)
            shutil.move(tmp.name, self.file_path)
        logger.info(f"Excel file written successfully: {self.file_path}")

    def _sync_append_row(self, row: list[str | int]) -> bool:
        try:
            wb = load_workbook(self.file_path)
        except Exception as e:
            logger.error(f"Error loading the Excel workdbook: {e}")
            return False

        ws = wb.active
        if ws:
            try:
                ws.append(row)
            except Exception as e:
                logger.error(f"Error appending row to Excel file: {e}")
                return False
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            wb.save(tmp.name)
            shutil.move(tmp.name, self.file_path)
        wb.close()
        return True

        return False

    async def _append_row(self, data: dict[str, Any]):
        """A less efficient version for row appending."""
        # async with self.atomic_write:
        try:
            df = await self.read_file()
            updated_df = await self._run_in_thread(
                partial(self._in_memory_append_row, df, data)
            )
            await self.write_file(updated_df)
            logger.info(f"Row appended to Excel file: {self.file_path}")
        except FileHandlerReadFileError as e:
            logger.error(f"Error reading Excel file: {e}")
            raise
        except FileHandlerWriteFileError as e:
            logger.error(f"Error writing updated Excel file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error appending row to Excel file: {e}")
            raise FileHandlerOperationError from e

    def _in_memory_append_row(
        self, df: pd.DataFrame, data: dict[str, Any]
    ) -> pd.DataFrame:
        new_df = pd.DataFrame([data])
        return pd.concat([df, new_df], ignore_index=True)

    def _in_memory_update_rows(
        self, df: pd.DataFrame, query: QueryFunction, update: dict
    ) -> tuple[pd.DataFrame, int]:
        mask = df.apply(query, axis=1)
        df.loc[mask] = df.loc[mask].assign(**update)
        num_updated = int(mask.sum())
        return df, num_updated

    def _in_memory_delete_rows(
        self, df: pd.DataFrame, query: QueryFunction
    ) -> tuple[pd.DataFrame, int]:
        mask = df.apply(query, axis=1)
        filtered_df = df[~mask]
        num_deleted = int(mask.sum())
        return filtered_df, num_deleted

    async def _run_in_thread(self, func):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, func)

    @classmethod
    def _get_write_lock(cls, path: Path) -> asyncio.Lock:
        if path not in cls._write_locks:
            cls._write_locks[path] = asyncio.Lock()
        return cls._write_locks[path]

    def _ensure_directories(self):
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    def _create_backup_path(self) -> Path:
        return (
            self.backup_dir
            / f"{self.file_path.stem}_bak_{pd.Timestamp.now().value}{self.file_path.suffix}"
        )
