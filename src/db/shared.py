# from contextlib import contextmanager

# from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker

# from app import settings


class Base(DeclarativeBase):
    pass


# app_db = settings.DB["app"]
# jobstore_db = settings.DB["jobstore"]

# db_engine = create_engine(
#     f"{app_db['engine']}:////{settings.BASE_DIR}/data/{app_db['name']}",
#     echo=settings.DEBUG,
# )

# jobstore_engine = create_engine(
#     f"{jobstore_db['engine']}:////{settings.BASE_DIR}/data/{jobstore_db['name']}",
#     echo=settings.DEBUG,
# )

# Session = scoped_session(sessionmaker(bind=db_engine))


# @contextmanager
# def get_session(engine: Engine = db_engine):
#     Session = scoped_session(sessionmaker(bind=engine))
#     try:
#         yield Session
#     except Exception:
#         Session.rollback()
#     finally:
#         Session.close()
