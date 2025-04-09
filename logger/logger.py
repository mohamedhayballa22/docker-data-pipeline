import os
import logging
import sys


def get_logger(name):
    env = os.environ.get("ENVIRONMENT", "dev")
    log_dir = "/app/logs"

    # Create logger
    logger = logging.getLogger(name)

    # Clear any existing handlers to avoid duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    # Configure based on environment
    if env.lower() == "dev":
        level = logging.INFO
        # In dev: Log to console
        handler = logging.StreamHandler(sys.stdout)
    else:  # prod
        level = logging.WARNING
        # In prod: Log to file
        os.makedirs(log_dir, exist_ok=True)
        handler = logging.FileHandler(f"{log_dir}/{name}.log")

    # Set formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    # Configure logger
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger
