import logging
import os

LOGS_DIR = "./logs"
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOGS_DIR, "frontend.log")

def get_logger():
    logger = logging.getLogger("frontend")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:  # Prevent duplicate handlers
        file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
