import logging
from pathlib import Path
import sys


def get_logger(
    logger_name: str,
    log_path: str = "/app/logs",
    log_level: str = "INFO"
) -> logging.Logger:
    """
    Create and return a configured logger.

    Args:
        logger_name (str): Logger name
        log_path (str): Path to store logs
        log_level (str): Logging level

    Returns:
        logging.Logger: Configured logger instance
    """

    logger = logging.getLogger(logger_name)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(log_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Create logs directory
    Path(log_path).mkdir(parents=True, exist_ok=True)

    # File Handler
    file_handler = logging.FileHandler(
        f"{log_path}/{logger_name}.log"
    )

    file_handler.setFormatter(formatter)

    # Add Handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger