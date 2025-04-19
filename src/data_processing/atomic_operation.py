import asyncio
from typing import Protocol

from src.logging import get_logger

logger = get_logger(__name__)


class WriteSafeFileHandlerProtocol(Protocol):
    def get_lock(self) -> asyncio.Lock: ...
    async def create_backup(self) -> str: ...
    async def restore_backup(self, backup_path: str): ...


class AtomicOperation:
    def __init__(self, handler: WriteSafeFileHandlerProtocol):
        self.handler = handler
        self.lock = self.handler.get_lock()
        self.backup_path: str = None

    async def __aenter__(self):
        logger.debug("Acquiring write lock for atomic write")
        await self.lock.acquire()
        self.backup_path = await self.handler.create_backup()
        # return self.handler

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.lock.locked():
            logger.debug("Releasing write lock for atomic write")
            self.lock.release()

        if exc_type is not None:
            logger.error(
                f"{exc_type} exception with value {exc_val} caught during atomic write.\n"
                f"Restoring backup from {self.backup_path}"
            )

            if self.backup_path:
                await self.handler.restore_backup(self.backup_path)

            raise exc_val
