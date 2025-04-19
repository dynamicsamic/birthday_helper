from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

engine = create_async_engine("sqlite+aiosqlite://", echo=True)
sessionmaker = async_sessionmaker(expire_on_commit=False, autoflush=False)


@asynccontextmanager
async def get_db_session():
    async with sessionmaker(bind=engine) as session:
        try:
            async with session.begin():
                yield session
        except Exception as e:
            print(f"Exception occured: {e}")
            await session.rollback()
        finally:
            await session.close()
