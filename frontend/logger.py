import logging
import sys
import os

class StreamToLogger:
    """Redirects print() and stderr output to a logger."""
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ""

    def write(self, buf):
        if buf.rstrip():  # Avoid empty lines
            for line in buf.rstrip().splitlines():
                self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass


def setup_logger():
    """
    Sets up a persistent logger that:
    ✅ Logs everything (DEBUG and above)
    ✅ Writes to both console and file (append mode)
    ✅ Redirects print() and stderr to log file
    ✅ Avoids duplicate handlers across Streamlit reruns
    """
    logger = logging.getLogger("Nostradamus-CoPilot")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # Ensure logs directory exists
    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, "nostradamus_copilot.log")

    # Only add handlers once (Streamlit re-runs the script)
    if not logger.handlers:
        # Suppress noisy logs from dependencies before setting up our handlers
        logging.getLogger("tqdm").setLevel(logging.WARNING)
        logging.getLogger("streamlit.runtime.caching.storage.WatchedPath").setLevel(logging.WARNING)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

        # --- File handler (append mode) ---
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # --- Console handler ---
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # Log setup confirmation
        logger.info(f"✅ Logger initialized. Writing logs to: {log_file}")

        # Redirect stdout/stderr only if NOT running in Chainlit context
        # Chainlit/Uvicorn has its own logging config that conflicts with this.
        # We can check if 'chainlit' is in the running script's path.
        if "chainlit" not in sys.argv[0]:
            sys.stdout = StreamToLogger(logger, logging.INFO)
            sys.stderr = StreamToLogger(logger, logging.ERROR)

    return logger


# Initialize the global logger
logger = setup_logger()
