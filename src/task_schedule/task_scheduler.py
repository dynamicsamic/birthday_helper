# from apscheduler.jobstores
import asyncio
import logging
from datetime import datetime

from aiogram import Bot
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.state.state_manager import BirthdayDataManager

logger = logging.getLogger(__name__)


async def perform_sync(data_manager: BirthdayDataManager):
    try:
        await data_manager.sync_files()
    except Exception as e:
        logger.error("Error in scheduled synchronization", exc_info=e)
        raise


def start_scheduler(data_manager: BirthdayDataManager):
    jobstores = {"default": MemoryJobStore()}
    executors = {"default": ThreadPoolExecutor()}
    job_defaults = {"coalesce": True}
    scheduler = AsyncIOScheduler(
        jobstores=jobstores, executors=executors, job_defaults=job_defaults
    )
    scheduler.add_job(
        perform_sync, "interval", hours=2, replace_existing=True, args=[data_manager]
    )
    scheduler.start()
    return scheduler


async def send_upcoming_birthday_messages(
    bot: Bot, chat_id: int, data_manager: BirthdayDataManager
):
    birthday_data = data_manager.get_upcoming_birthdays()
    # birthday_data = format_upcoming_birthdays
    await bot.send_message(chat_id, text=birthday_data)


def add_chat_to_birthday_mailing_list(
    scheduler: AsyncIOScheduler,
    bot: Bot,
    chat_id: int,
    data_manager: BirthdayDataManager,
):
    scheduler.add_job(
        send_upcoming_birthday_messages,
        trigger=CronTrigger(day_of_week="mon-sun", hour=9),
        id=str(chat_id),
        replace_existing=True,
        args=[bot, chat_id, data_manager],
    )


def remove_chat_from_birthday_mailing_list(scheduler: AsyncIOScheduler, chat_id: int):
    scheduler.remove_job(str(chat_id))


def tick():
    print(f"Tick is {datetime.now()}")


async def main():
    jobstores = {"default": MemoryJobStore()}
    executors = {"default": ThreadPoolExecutor()}
    job_defaults = {"coalesce": True, "replace_existing": True}
    scheduler = AsyncIOScheduler(
        jobstores=jobstores, executors=executors, job_defaults=job_defaults
    )
    scheduler.add_job(tick, IntervalTrigger(hours=2), id="123")
    scheduler.start()
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
