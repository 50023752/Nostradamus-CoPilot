import logging
import sys

def setup_logger():
    """
    Sets up a logger that prints to stdout with a specific format.
    """
    logger = logging.getLogger("Nostradamus-CoPilot")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent logs from being passed to the root logger

    # Avoid adding multiple handlers if the logger is already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

logger = setup_logger()