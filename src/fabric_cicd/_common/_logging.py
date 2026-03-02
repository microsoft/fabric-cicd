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


"""Helper functions to configure logging and handle exceptions across the fabric_cicd package."""

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


def _configure_file_handler(
    level: int,
    file_path: str | None,
    use_file_rotation: bool,
    debug_only_file: bool,
) -> logging.Handler:
    """Configure a file handler (default or rotating)."""
    if use_file_rotation and file_path:
        handler = RotatingFileHandler(
            file_path,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=7,  # Retain 7 rotated files (35 MB total)
        )
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    else:
        handler = logging.FileHandler(
            "fabric_cicd.error.log",
            mode="w",
            delay=True,  # Delay file creation until first log
        )
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))

    # Capture only DEBUG messages in log file when DEBUG level is set and debug_only_file is True
    if debug_only_file and level == logging.DEBUG:
        handler.setLevel(logging.DEBUG)
        handler.addFilter(lambda record: record.levelno == logging.DEBUG)

    # The file handler sits on the root logger, which receives propagated records from ALL loggers
    # (e.g. azure.identity, urllib3, requests). This filter ensures only fabric_cicd package logs
    # are written to the file, preventing third-party noise in the log output.
    handler.addFilter(lambda record: record.name.startswith("fabric_cicd"))

    return _mark_handler(handler)


def _configure_console_handler(level: int) -> logging.StreamHandler:
    """Configure a console handler with the standard fabric_cicd formatter."""
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(
        CustomFormatter(
            "[%(levelname)s] %(asctime)s - %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    return _mark_handler(handler)


def _get_file_handler() -> logging.FileHandler | None:
    """Get the file handler on the root logger, if one exists."""
    root_logger = logging.getLogger()
    return next(
        (
            h
            for h in root_logger.handlers
            if isinstance(h, (logging.FileHandler, RotatingFileHandler))
            and getattr(h, _FABRIC_CICD_HANDLER_ATTR, False)
        ),
        None,
    )


def _build_console_message(exception: BaseException, file_handler: logging.FileHandler | None) -> str:
    """Build the user-facing console error message, optionally referencing the log file."""
    # Skip file reference for RotatingFileHandler since it only contains DEBUG logs
    if file_handler is not None and not isinstance(file_handler, RotatingFileHandler):
        log_file_path = Path(file_handler.baseFilename).resolve()
        return f"{exception!s}\n\nSee {log_file_path} for full details."
    return f"{exception!s}"


def _build_file_message(exception: BaseException) -> str:
    """Build the log file message, including additional info if available."""
    additional_info = getattr(exception, "additional_info", None)
    if additional_info is not None:
        return f"%s\n\nAdditional Info: \n{additional_info}"
    return "%s"


"""Main logging configuration function and exception handler for fabric_cicd."""


def configure_logger(
    level: int = logging.INFO,
    file_path: Optional[str] = None,
    use_file_rotation: bool = False,
    suppress_debug_console: bool = False,
    debug_only_file: bool = False,
    disable_log_file: bool = False,
) -> None:
    """
    Configure the logger.

    Args:
        level: The log level to set. Must be one of the standard logging levels.
        file_path: Path to custom log file (optional).
        use_file_rotation: Use RotatingFileHandler with size-based rotation.
        suppress_debug_console: Suppress DEBUG output to console (only applies when level is DEBUG).
        debug_only_file: Only write DEBUG messages to file (only applies when level is DEBUG).
        disable_log_file: Disable file logging entirely.
    """
    # Determine console level - suppress DEBUG to console if specified, otherwise same as level
    console_level = logging.INFO if suppress_debug_console and level == logging.DEBUG else level

    # Get all loggers
    root_logger = logging.getLogger()
    package_logger = logging.getLogger("fabric_cicd")
    console_only_logger = logging.getLogger("console_only")

    # Close and remove old handlers before adding new ones
    _cleanup_managed_handlers(root_logger, package_logger, console_only_logger)

    # Root logger - receives propagated records from fabric_cicd loggers
    # Holds the file handler so all fabric_cicd.* child loggers write to file via propagation
    # Set root logger level - for non-fabric_cicd packages: INFO if DEBUG, else ERROR
    root_logger.setLevel(level=logging.INFO if level == logging.DEBUG else logging.ERROR)

    # Configure file handler unless disabled
    if not disable_log_file:
        root_logger.addHandler(_configure_file_handler(level, file_path, use_file_rotation, debug_only_file))

    # Package logger - primary logger for all fabric_cicd library logging
    # Writes to console via its own handler and to file via propagation to root
    package_logger.setLevel(level)
    package_logger.addHandler(_configure_console_handler(console_level))

    # Console-only logger - used exclusively by exception_handler() to display
    # user-facing error messages on the terminal without writing them to the log file
    console_only_logger.setLevel(console_level)
    console_only_logger.addHandler(_configure_console_handler(console_level))
    console_only_logger.propagate = False


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

    # If the exception is not from _common._exceptions, use the default exception handler
    if not any(isinstance(exception, cls) for cls in exception_classes):
        sys.__excepthook__(exception_type, exception, traceback)
        return

    # Step 1: Write user-facing error message to console only (no file)
    file_handler = _get_file_handler()
    console_message = _build_console_message(exception, file_handler)
    logging.getLogger("console_only").error(console_message)

    # Step 2: Write full stack trace to file only (not terminal)
    # Remove console handler first from the package logger so the stack trace doesn't also print to terminal
    package_logger = logging.getLogger("fabric_cicd")
    _cleanup_managed_handlers(package_logger)
    file_message = _build_file_message(exception)
    exception.logger.exception(file_message, exception, exc_info=(exception_type, exception, traceback))


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
