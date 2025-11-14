# logger.py
import logging
import os

LOGS_DIR = "./server/logs"
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOGS_DIR, "server.log")

_logger = None  # cache the instance

def get_logger():
    global _logger
    if _logger is not None:
        return _logger

    logger = logging.getLogger("backend")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # prevent passing logs to root logger

    file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    _logger = logger
    return _logger
