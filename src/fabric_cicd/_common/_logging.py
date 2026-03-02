# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Logging utilities for the fabric_cicd package."""

import inspect
import logging
import re
import sys
import traceback
from logging import LogRecord
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import ClassVar, Optional

from fabric_cicd import constants
from fabric_cicd._common import _exceptions
from fabric_cicd._common._color import Fore, Style


class CustomFormatter(logging.Formatter):
    LEVEL_COLORS: ClassVar[dict[str, str]] = {
        "DEBUG": Fore.BLACK,
        "INFO": Fore.WHITE + Style.BRIGHT,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Style.BRIGHT + Fore.RED,
    }

    def format(self, record: LogRecord) -> str:
        level_color = self.LEVEL_COLORS.get(record.levelname, "")
        level_name = {
            "WARNING": "warn",
            "DEBUG": "debug",
            "INFO": "info",
            "ERROR": "error",
            "CRITICAL": "crit",
        }.get(record.levelname, "unknown")

        level_name = f"{level_color}[{level_name}]"
        timestamp = f"{self.formatTime(record, self.datefmt)}"
        message = f"{record.getMessage()}{Style.RESET_ALL}"

        # indent if the message contains "->"
        if constants.INDENT in message:
            message = message.replace(constants.INDENT, "")
            full_message = f"{' ' * 8} {timestamp} - {message}"
        else:
            # Calculate visual length by removing ANSI escape codes

            ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

            # Get visual length of level_name without ANSI codes
            visual_level_length = len(ansi_escape.sub("", level_name))
            # Pad to 16 visual characters
            padding = " " * max(0, 8 - visual_level_length)

            full_message = f"{level_name}{padding} {timestamp} - {message}"
        return full_message


_FABRIC_CICD_HANDLER_ATTR = "_fabric_cicd_managed"


def _cleanup_managed_handlers(*loggers: logging.Logger) -> None:
    """Close and remove only handlers previously added by fabric_cicd."""
    for logger_instance in loggers:
        for handler in list(logger_instance.handlers):
            if getattr(handler, _FABRIC_CICD_HANDLER_ATTR, False):
                handler.close()
                logger_instance.removeHandler(handler)


def _mark_handler(handler: logging.Handler) -> logging.Handler:
    """Mark a handler as managed by fabric_cicd."""
    setattr(handler, _FABRIC_CICD_HANDLER_ATTR, True)
    return handler


def _create_console_handler(level: int) -> logging.StreamHandler:
    """Create a console handler with the standard fabric_cicd formatter."""
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(
        CustomFormatter(
            "[%(levelname)s] %(asctime)s - %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    return handler


def configure_logger(
    level: int = logging.INFO,
    file_path: Optional[str] = None,
    rotate_on: bool = False,
    suppress_debug_console: bool = False,
    debug_only_file: bool = False,
    disable_log_file: bool = False,
) -> None:
    """
    Configure the logger.

    Args:
        level: The log level to set. Must be one of the standard logging levels.
        file_path: Path to log file (optional).
        rotate_on: Use RotatingFileHandler with size-based rotation.
        suppress_debug_console: Suppress DEBUG output to console (only applies when level is DEBUG).
        debug_only_file: Only write DEBUG messages to file (only applies when level is DEBUG).
        disable_log_file: Disable file logging entirely.
    """
    # For non-fabric_cicd packages: INFO if DEBUG, else ERROR
    root_level = logging.INFO if level == logging.DEBUG else logging.ERROR

    root_logger = logging.getLogger()
    root_logger.setLevel(root_level)

    package_logger = logging.getLogger("fabric_cicd")
    console_only_logger = logging.getLogger("console_only")

    # Close and remove only handlers previously added by fabric_cicd
    _cleanup_managed_handlers(root_logger, package_logger, console_only_logger)

    # Configure file handler unless disabled
    if not disable_log_file:
        if rotate_on and file_path:
            # Configure rotating file handler for append mode with size-based rotation
            file_handler = RotatingFileHandler(
                file_path,
                maxBytes=5 * 1024 * 1024,  # 5 MB
                backupCount=7,  # Retain 7 rotated files (35 MB total)
            )
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        else:
            # Configure default logging (explicit file handler with delay and package filter)
            file_handler = logging.FileHandler(
                "fabric_cicd.error.log",
                mode="w",
                delay=True,  # Delay file creation until first log
            )
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))

        # Capture only DEBUG messages in log file when DEBUG level is set
        if debug_only_file and level == logging.DEBUG:
            file_handler.setLevel(logging.DEBUG)
            file_handler.addFilter(lambda record: record.levelno == logging.DEBUG)

        # Filter to only accept fabric_cicd logs
        file_handler.addFilter(lambda record: record.name.startswith("fabric_cicd"))
        root_logger.addHandler(_mark_handler(file_handler))

    # Determine console level
    console_level = level
    if suppress_debug_console and level == logging.DEBUG:
        console_level = logging.INFO

    # Create a logger that writes to the console and log file
    package_logger.setLevel(level)
    package_logger.addHandler(_mark_handler(_create_console_handler(console_level)))

    # Create a logger that only writes to the console
    console_only_logger.setLevel(console_level)
    console_only_logger.addHandler(_mark_handler(_create_console_handler(console_level)))
    console_only_logger.propagate = False  # Prevent logs from being propagated to other loggers


def exception_handler(exception_type: type[BaseException], exception: BaseException, traceback: traceback) -> None:
    """
    Handle exceptions that are instances of any class from the _common._exceptions module.

    Args:
        exception_type: The type of the exception.
        exception: The exception instance.
        traceback: The traceback object.
    """
    # Get all exception classes from the _common._exceptions module
    exception_classes = [cls for _, cls in inspect.getmembers(_exceptions, inspect.isclass)]

    # Check if the exception is an instance of any class from _common._exceptions
    if any(isinstance(exception, cls) for cls in exception_classes):
        # Log the exception using the logger associated with the exception
        original_logger = exception.logger

        # Check if file logging is enabled by looking for managed file handlers
        root_logger = logging.getLogger()
        file_handler = next(
            (
                h
                for h in root_logger.handlers
                if isinstance(h, (logging.FileHandler, RotatingFileHandler))
                and getattr(h, _FABRIC_CICD_HANDLER_ATTR, False)
            ),
            None,
        )

        # Write only the exception message to the console
        # Skip file reference for RotatingFileHandler since it only contains DEBUG logs
        if file_handler is not None and not isinstance(file_handler, RotatingFileHandler):
            log_file_path = Path(file_handler.baseFilename).resolve()
            logging.getLogger("console_only").error(f"{exception!s}\n\nSee {log_file_path} for full details.")
        else:
            logging.getLogger("console_only").error(f"{exception!s}")

        # Write exception and full stack trace to logs but not terminal
        package_logger = logging.getLogger("fabric_cicd")

        additional_info = getattr(exception, "additional_info", None)
        additional_info = "\n\nAdditional Info: \n" + additional_info if additional_info is not None else ""

        # Remove only managed console handlers so the stack trace goes to file only (not terminal)
        _cleanup_managed_handlers(package_logger)
        original_logger.exception(f"%s{additional_info}", exception, exc_info=(exception_type, exception, traceback))
    else:
        # If the exception is not from _common._exceptions, use the default exception handler
        sys.__excepthook__(exception_type, exception, traceback)


def log_header(logger: logging.Logger, message: str) -> None:
    """
    Logs a header message with a decorative line above and below it.

    Args:
        logger: The logger to use for logging the header message.
        message: The header message to log.
    """
    line_separator = "#" * 100
    formatted_message = f"########## {message}"
    formatted_message = f"{formatted_message} {line_separator[len(formatted_message) + 1 :]}"

    logger.info("")  # Log a blank line before the header
    logger.info(f"{Fore.GREEN}{Style.BRIGHT}{line_separator}{Style.RESET_ALL}")
    logger.info(f"{Fore.GREEN}{Style.BRIGHT}{formatted_message}{Style.RESET_ALL}")
    logger.info(f"{Fore.GREEN}{Style.BRIGHT}{line_separator}{Style.RESET_ALL}")
    logger.info("")
