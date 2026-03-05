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
from typing import ClassVar, Optional, Union

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


class PackageFilter(logging.Filter):
    """
    Filter that only allows records from the fabric_cicd package logs.

    Args:
        debug_only: If True, only allows DEBUG level records. If False, allows all levels.
    """

    def __init__(self, debug_only: bool = False) -> None:
        super().__init__()
        self.debug_only = debug_only

    def filter(self, record: LogRecord) -> bool:
        is_fabric_cicd = record.name.startswith("fabric_cicd")
        if self.debug_only:
            return is_fabric_cicd and record.levelno == logging.DEBUG
        return is_fabric_cicd


"""Helper functions to configure logging and handle exceptions across the fabric_cicd package."""

_DEFAULT_LOG_FILENAME = "fabric_cicd.error.log"
_DEFAULT_LOG_FILE_FORMATTER = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
_FABRIC_CICD_HANDLER_ATTR = "_fabric_cicd_managed"
_FABRIC_CICD_EXTERNAL_HANDLER_ATTR = "_fabric_cicd_external"


def _cleanup_external_handler_filters(root_logger: logging.Logger) -> None:
    """Remove PackageFilter from any external handler previously configured by fabric_cicd."""
    for handler in list(root_logger.handlers):
        if getattr(handler, _FABRIC_CICD_EXTERNAL_HANDLER_ATTR, False):
            # Remove all PackageFilters that were added
            for f in list(handler.filters):
                if isinstance(f, PackageFilter):
                    handler.removeFilter(f)
            # Remove the marker attribute
            delattr(handler, _FABRIC_CICD_EXTERNAL_HANDLER_ATTR)
            # Remove handler from root logger (don't close it - caller owns it)
            root_logger.removeHandler(handler)


def _cleanup_managed_handlers(*loggers: logging.Logger) -> None:
    """Close and remove only handlers previously added by fabric_cicd."""
    for logger_instance in loggers:
        # First, clean up any external handlers configured (filters only, don't close)
        _cleanup_external_handler_filters(logger_instance)

        # Then clean up fabric_cicd-managed handlers (close and remove)
        for handler in list(logger_instance.handlers):
            if getattr(handler, _FABRIC_CICD_HANDLER_ATTR, False):
                handler.close()
                logger_instance.removeHandler(handler)


def _mark_handler(handler: logging.Handler) -> logging.Handler:
    """Mark a handler as managed by fabric_cicd."""
    setattr(handler, _FABRIC_CICD_HANDLER_ATTR, True)
    return handler


def _mark_external_handler(handler: logging.Handler) -> logging.Handler:
    """Mark an external handler as configured by fabric_cicd (for filter cleanup only)."""
    setattr(handler, _FABRIC_CICD_EXTERNAL_HANDLER_ATTR, True)
    return handler


def _configure_default_file_handler() -> logging.Handler:
    """Configure the default file handler for standalone fabric_cicd usage."""
    handler = logging.FileHandler(
        filename=_DEFAULT_LOG_FILENAME,
        mode="w",
        delay=True,
    )
    handler.setFormatter(logging.Formatter(_DEFAULT_LOG_FILE_FORMATTER))
    handler.addFilter(PackageFilter())  # All levels from fabric_cicd package logs

    return _mark_handler(handler)


def _configure_external_file_handler(
    external_handler: Union[logging.FileHandler, RotatingFileHandler],
    level: int,
    debug_only_file: bool,
) -> logging.Handler:
    """
    Configure an external file handler for fabric_cicd package logs.

    Reuses the external handler directly to preserve rotation behavior (if any).
    The external handler is not marked as fabric_cicd-managed (won't be closed),
    but is marked as external so filters can be cleaned up on reconfiguration.

    Note: Any existing PackageFilter is already removed by _cleanup_managed_handlers()
    before this function is called.
    """
    # Add the appropriate filter
    if level == logging.DEBUG and debug_only_file:
        external_handler.addFilter(PackageFilter(debug_only=True))
    else:
        external_handler.addFilter(PackageFilter())

    # Mark as external in order to clean up filters later (but don't close it)
    return _mark_external_handler(external_handler)


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


def _build_console_message(exception: BaseException, file_handler: Optional[logging.FileHandler] = None) -> str:
    """Build the user-facing console error message, optionally referencing the log file."""
    # Write exception to console when file logging is disabled or when using an external file handler
    if file_handler is None or Path(file_handler.baseFilename).name != _DEFAULT_LOG_FILENAME:
        return f"{exception!s}"

    # Only reference the default log file which contains full error details
    log_file_path = Path(file_handler.baseFilename).resolve()
    return f"{exception!s}\n\nSee {log_file_path} for full details."


def _build_file_message(exception: BaseException) -> str:
    """Build the log file message, including additional info if available."""
    additional_info = getattr(exception, "additional_info", None)
    if additional_info is not None:
        return f"%s\n\nAdditional Info: \n{additional_info}"
    return "%s"


"""Main logging configuration and exception handling functions for fabric_cicd."""


def get_file_handler(
    logger: Optional[logging.Logger] = None,
) -> Optional[Union[logging.FileHandler, RotatingFileHandler]]:
    """
    Get a file handler from a logger.

    Args:
        logger: The logger to search for a file handler. If None, searches the root logger
            for fabric_cicd-managed handlers only.

    Returns:
        The first FileHandler or RotatingFileHandler found, or None if not found.
    """
    target_logger = logger if logger is not None else logging.getLogger()

    # When searching root logger, only return fabric_cicd-managed handlers
    check_managed = logger is None

    return next(
        (
            h
            for h in target_logger.handlers
            if isinstance(h, (logging.FileHandler, RotatingFileHandler))
            and (getattr(h, _FABRIC_CICD_HANDLER_ATTR, False) if check_managed else True)
        ),
        None,
    )


def configure_logger(
    level: int = logging.INFO,
    suppress_debug_console: bool = False,
    debug_only_file: bool = False,
    disable_log_file: bool = False,
    external_file_handler: Optional[Union[logging.FileHandler, RotatingFileHandler]] = None,
) -> None:
    """
    Configure the logger.

    Args:
        level: The log level to set. Must be one of the standard logging levels.
        suppress_debug_console: Suppress DEBUG output to console (only applies when level is DEBUG).
        debug_only_file: Only write DEBUG messages to file (only applies when level is DEBUG).
        disable_log_file: Disable file logging entirely.
        external_file_handler: External file handler to append logs to instead of creating the default one.
    """
    # Determine console level - suppress DEBUG to console if specified, otherwise same as level
    console_level = logging.INFO if suppress_debug_console and level == logging.DEBUG else level

    # Get all loggers
    root_logger = logging.getLogger()
    package_logger = logging.getLogger("fabric_cicd")
    console_only_logger = logging.getLogger("console_only")

    # Close and remove old handlers before adding new ones
    # This also cleans up any PackageFilter added to external handlers
    _cleanup_managed_handlers(root_logger, package_logger, console_only_logger)

    # Root logger - receives propagated records from fabric_cicd loggers
    # Holds the file handler so all fabric_cicd.* child loggers write to file via propagation
    # Set root logger level - for non-fabric_cicd packages: INFO if DEBUG, else ERROR
    root_logger.setLevel(level=logging.INFO if level == logging.DEBUG else logging.ERROR)

    # Configure file handler based on parameters
    if external_file_handler is not None:
        # Use provided external file handler for fabric_cicd logs
        root_logger.addHandler(_configure_external_file_handler(external_file_handler, level, debug_only_file))

    elif not disable_log_file:
        # Use the default file handler for fabric_cicd logs
        root_logger.addHandler(_configure_default_file_handler())

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
    file_handler = get_file_handler()  # searches root logger for managed handlers
    console_message = _build_console_message(exception, file_handler)
    logging.getLogger("console_only").error(console_message)

    # Step 2: Write full stack trace to file only (not terminal)
    # Only write to file if using fabric_cicd default file handler (includes ERROR level logs)
    is_default_file_handler = file_handler is not None and Path(file_handler.baseFilename).name == _DEFAULT_LOG_FILENAME
    if is_default_file_handler:
        # Remove console handler from package logger so stack trace doesn't print to terminal
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
