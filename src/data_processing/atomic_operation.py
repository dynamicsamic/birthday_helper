import asyncio
from pathlib import Path
from typing import Protocol, final

from src.logging import get_logger
from src.exceptions import AtomicOperationError

logger = get_logger(__name__)


class WriteSafeFileHandlerProtocol(Protocol):
    def get_lock(self) -> asyncio.Lock: ...

    async def create_backup(self) -> Path: ...

    async def restore_backup(self, backup_path: Path): ...


@final
class AtomicOperation:
    def __init__(self, handler: WriteSafeFileHandlerProtocol):
        self.handler = handler
        self.lock = self.handler.get_lock()
        self.backup_path: Path | None = None
        self._lock_acquired: bool = False

    async def __aenter__(self):
        if not self._lock_acquired:
            logger.debug("Acquiring write lock for atomic write")
            self._lock_acquired = await self.lock.acquire()
            self.backup_path = await self.handler.create_backup()

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, _
    ):
        if self._lock_acquired:
            logger.debug("Releasing write lock for atomic write")
            self.lock.release()
            self._lock_acquired = False

        if exc_type is not None:
            logger.error(
                (
                    f"{exc_type} exception with value {exc_val} caught during "
                    f"atomic write.\nRestoring backup from {self.backup_path}"
                )
            )

            if self.backup_path:
                await self.handler.restore_backup(self.backup_path)

            raise AtomicOperationError(exc_val)
