# logger_module.py

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Union
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

class CustomLogger:
    """
    Custom logger class that provides both console and file logging capabilities
    with rotation options.
    """

    _instance = None
    _initialized = False

    def __new__(cls):
        """Implement singleton pattern"""
        if cls._instance is None:
            cls._instance = super(CustomLogger, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize logger only once"""
        if CustomLogger._initialized:
            return

        self.logger = logging.getLogger('CustomLogger')
        self.logger.setLevel(logging.DEBUG)
        self._initialized = True

    def setup(self,
              log_file: Optional[Union[str, Path]] = None,
              console_level: int = logging.INFO,
              file_level: int = logging.DEBUG,
              log_format: str = None,
              rotation_type: str = 'size',
              max_bytes: int = 5_242_880,  # 5MB
              backup_count: int = 5,
              when: str = 'midnight') -> logging.Logger:
        """
        Setup logger with console and optional file handlers.

        Args:
            log_file: Path to log file. If None, only console logging is enabled
            console_level: Logging level for console output
            file_level: Logging level for file output
            log_format: Custom log format string
            rotation_type: 'size' or 'time' for log rotation
            max_bytes: Maximum size in bytes for size-based rotation
            backup_count: Number of backup files to keep
            when: Time-based rotation interval ('S', 'M', 'H', 'D', 'midnight')

        Returns:
            logging.Logger: Configured logger instance
        """
        # Clear existing handlers
        self.logger.handlers.clear()

        # Default format if none provided
        if log_format is None:
            log_format = (
                '%(asctime)s | %(levelname)-8s | '
                '%(filename)s:%(lineno)d | %(funcName)s | %(message)s'
            )

        formatter = logging.Formatter(log_format)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(console_level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # File handler (if log_file provided)
        if log_file:
            log_file = Path(log_file)
            log_file.parent.mkdir(parents=True, exist_ok=True)

            if rotation_type.lower() == 'size':
                file_handler = RotatingFileHandler(
                    log_file,
                    maxBytes=max_bytes,
                    backupCount=backup_count
                )
            else:  # time-based rotation
                file_handler = TimedRotatingFileHandler(
                    log_file,
                    when=when,
                    backupCount=backup_count
                )

            file_handler.setLevel(file_level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        return self.logger

    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance."""
        return self.logger

# Create default logger instance
logger = CustomLogger().get_logger()

# Convenience functions
def debug(msg: str, *args, **kwargs):
    logger.debug(msg, *args, **kwargs)

def info(msg: str, *args, **kwargs):
    logger.info(msg, *args, **kwargs)

def warning(msg: str, *args, **kwargs):
    logger.warning(msg, *args, **kwargs)

def error(msg: str, *args, **kwargs):
    logger.error(msg, *args, **kwargs)

def critical(msg: str, *args, **kwargs):
    logger.critical(msg, *args, **kwargs)

def exception(msg: str, *args, **kwargs):
    logger.exception(msg, *args, **kwargs)
