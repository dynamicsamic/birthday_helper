import pytest
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine

from src import settings
from src.scheduler import BotScheduler

engine_ = create_engine("sqlite://", echo=True)
job_store = SQLAlchemyJobStore(engine=engine_)


@pytest.fixture
def scheduler():
    Scheduler = BotScheduler(
        jobstores={"default": job_store},
        timezone=settings.TIME_ZONE,
        executors={"default": AsyncIOExecutor()},
        job_defaults={"misfire_grace_time": 30, "coalesce": True},
    )
    return Scheduler


def test_scheduler_add_chat_to_birthday_mailing_saves_to_jobstore(scheduler):
    scheduler.start()

    # Create jobtstore SQLAlchemy table
    job_store.jobs_t.create(engine_, True)

    scheduler.add_chat_to_birthday_mailing(22)
    jobs = job_store.get_all_jobs()

    assert len(jobs) == 1
    assert jobs[0].name == "dispatch_birthday_messages_to_chat"


def test_scheduler_add_chat_to_birthday_mailing_replaces_existing_job(
    scheduler,
):
    scheduler.start()

    # Create jobtstore SQLAlchemy table
    job_store.jobs_t.create(engine_, True)

    scheduler.add_chat_to_birthday_mailing(22)
    scheduler.add_chat_to_birthday_mailing(22)

    jobs = job_store.get_all_jobs()

    assert len(jobs) == 1
