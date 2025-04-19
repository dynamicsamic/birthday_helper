from sqlalchemy.ext.asyncio import AsyncSession


def to_snake_case(string: str) -> str:
    result = []
    for i, char in enumerate(string):
        if i == 0 and char.isupper():
            result.append(char.lower())
        elif char.isupper():
            result.append(f"_{char.lower()}")
        else:
            result.append(char)
    return "".join(result)


class BaseRepository:
    def __init__(self, db_session: AsyncSession):
        self.session = db_session

    @classmethod
    def get_name(cls) -> str:
        return to_snake_case(cls.__name__)


class BirthdayRepository(BaseRepository):
    pass


class RepositoryIndex:
    birthday = BirthdayRepository


repository_index = {"birthday": BirthdayRepository}


def get_repository(repository_name: str, db_session: AsyncSession) -> BaseRepository:
    if not (repository := repository_index.get(repository_name)):
        raise AttributeError(
            f"Repository index does not have repository: {repository_name}"
        )
    return repository(db_session)
