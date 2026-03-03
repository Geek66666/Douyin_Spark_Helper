import logging
import os
from collections import deque
from logging.handlers import RotatingFileHandler


if not os.path.exists("logs"):
    os.makedirs("logs")


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
LOG_FILE = "logs/app.log"
MAX_IN_MEMORY_LOGS = 2000
_recent_logs = deque(maxlen=MAX_IN_MEMORY_LOGS)


class InMemoryLogHandler(logging.Handler):
    """Keep recent log lines in memory for dashboard display."""

    def emit(self, record):
        try:
            _recent_logs.append(self.format(record))
        except Exception:
            # Never let logging break business logic.
            pass


def get_recent_logs(limit=400):
    if limit <= 0:
        return ""
    return "\n".join(list(_recent_logs)[-limit:])


def setup_logger(name="app", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    memory_handler = InMemoryLogHandler()
    memory_handler.setLevel(level)
    memory_handler.setFormatter(formatter)
    logger.addHandler(memory_handler)

    try:
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as exc:
        logger.warning("Failed to initialize file logger: %s", exc)

    return logger

