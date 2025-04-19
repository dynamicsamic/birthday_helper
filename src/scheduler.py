from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.job import Job
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src import settings
from src.db.shared import jobstore_engine
from src.toolbox.birthdays import dispatch_birthday_messages_to_chat


class BotScheduler(AsyncIOScheduler):
    """Subclass of `AsyncIOScheduler` from `appscheduler` package
    extended with some convinient custom methods."""

    __doc__ += AsyncIOScheduler.__doc__

    def add_chat_to_birthday_mailing(self, chat_id: int) -> Job:
        """Schedule daily birthday messages delivery to provided chat."""
        return self.add_job(
            dispatch_birthday_messages_to_chat,
            trigger=CronTrigger(day_of_week="mon-sun", hour=9),
            id=str(chat_id),
            replace_existing=True,
            kwargs={"chat_id": chat_id},
        )


Scheduler = BotScheduler(
    jobstores={"default": SQLAlchemyJobStore(engine=jobstore_engine)},
    timezone=settings.TIME_ZONE,
    executors={"default": AsyncIOExecutor()},
    job_defaults={"misfire_grace_time": 30, "coalesce": True},
)
