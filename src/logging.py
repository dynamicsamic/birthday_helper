import atexit
import logging
import logging.handlers
import queue

log_queue = queue.Queue(-1)
queue_listener = None


def setup_logging():
    global queue_listener

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler("app.log")
    file_handler.setFormatter(formatter)

    queue_listener = logging.handlers.QueueListener(
        log_queue,
        stream_handler,
        file_handler,
        respect_handler_level=True,
    )

    queue_listener.start()

    atexit.register(shutdown_logging)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.addHandler(logging.handlers.QueueHandler(log_queue))
    logger.setLevel(logging.INFO)
    return logger


def shutdown_logging():
    if queue_listener:
        queue_listener.stop()
