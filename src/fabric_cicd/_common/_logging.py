# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Logging utilities for the fabric_cicd package."""

import inspect
import logging
import re
import sys
import traceback
from logging import LogRecord
from pathlib import Path
from typing import Any, ClassVar, Optional

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
        location = f"[{record.funcName}:{record.lineno}]"
        item_context = ""
        item_type = getattr(record, "item_type", None)
        item_name = getattr(record, "item_name", None)
        if item_type or item_name:
            type_str = item_type if item_type else "?"
            name_str = item_name if item_name else "?"
            item_context = f"[{type_str}:{name_str}] "

        message = f"{record.getMessage()}{Style.RESET_ALL}"

        # indent if the message contains "->"
        if constants.INDENT in message:
            message = message.replace(constants.INDENT, "")
            full_message = f"{' ' * 8} {timestamp} - {location} - {item_context}{message}"
        else:
            # Calculate visual length by removing ANSI escape codes

            ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

            # Get visual length of level_name without ANSI codes
            visual_level_length = len(ansi_escape.sub("", level_name))
            # Pad to 16 visual characters
            padding = " " * max(0, 8 - visual_level_length)

            full_message = f"{level_name}{padding} {timestamp} - {location} - {item_context}{message}"
        return full_message


class ItemLoggerAdapter(logging.LoggerAdapter):
    """
    A LoggerAdapter that automatically includes item context (type, name) in log messages.

    This adapter is designed for use during item publishing operations where logs need
    to be traceable to specific items, especially in parallel execution scenarios.

    Example:
        >>> logger = logging.getLogger(__name__)
        >>> item_logger = ItemLoggerAdapter(logger, item_type="Notebook", item_name="MyNotebook")
        >>> item_logger.info("Publishing")
        # Output: [info] 14:32:01 - [_publish_item:650] - [Notebook>MyNotebook] Publishing
    """

    def __init__(
        self,
        logger: logging.Logger,
        item_type: Optional[str] = None,
        item_name: Optional[str] = None,
    ) -> None:
        """
        Initialize the ItemLoggerAdapter with item context.

        Args:
            logger: The underlying logger to wrap.
            item_type: The type of the item (e.g., "Notebook", "Environment").
            item_name: The display name of the item.
        """
        extra = {
            "item_type": item_type,
            "item_name": item_name,
        }
        super().__init__(logger, extra)

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """
        Process the logging call to inject item context into the extra dict.

        Args:
            msg: The log message.
            kwargs: Keyword arguments passed to the logging call.

        Returns:
            Tuple of (message, kwargs) with item context injected.
        """
        extra = kwargs.get("extra", {})
        extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs

    def with_item(
        self,
        item_type: Optional[str] = None,
        item_name: Optional[str] = None,
    ) -> "ItemLoggerAdapter":
        """
        Create a new ItemLoggerAdapter with updated item context.

        This allows creating derived loggers with different item context
        while reusing the same underlying logger.

        Args:
            item_type: The type of the item (overrides current if provided).
            item_name: The display name of the item (overrides current if provided).

        Returns:
            A new ItemLoggerAdapter with the updated context.
        """
        return ItemLoggerAdapter(
            self.logger,
            item_type=item_type if item_type is not None else self.extra.get("item_type"),
            item_name=item_name if item_name is not None else self.extra.get("item_name"),
        )


def get_item_logger(
    name: str,
    item_type: Optional[str] = None,
    item_name: Optional[str] = None,
) -> ItemLoggerAdapter:
    """
    Factory function to create an ItemLoggerAdapter for item-scoped logging.

    Args:
        name: The logger name (typically __name__).
        item_type: The type of the item (e.g., "Notebook", "Environment").
        item_name: The display name of the item.

    Returns:
        An ItemLoggerAdapter configured with the item context.

    Example:
        >>> item_logger = get_item_logger(__name__, item_type="Notebook", item_name="MyNotebook")
        >>> item_logger.info("Starting publish")
    """
    logger = logging.getLogger(name)
    return ItemLoggerAdapter(logger, item_type=item_type, item_name=item_name)


def configure_logger(level: int = logging.INFO) -> None:
    """
    Configure the logger.

    Args:
        level: The log level to set. Must be one of the standard logging levels.
    """
    # Configure default logging
    logging.basicConfig(
        level=(
            # For non-fabric_cicd packages: INFO if DEBUG, else ERROR
            logging.INFO if level == logging.DEBUG else logging.ERROR
        ),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        filename="fabric_cicd.error.log",
        filemode="w",
    )

    # Configure Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(
        CustomFormatter(
            "[%(levelname)s] %(asctime)s - %(message)s",
            datefmt="%H:%M:%S",
        )
    )

    # Create a logger that writes to the console and log file
    package_logger = logging.getLogger("fabric_cicd")
    package_logger.setLevel(level)
    package_logger.handlers = []
    package_logger.addHandler(console_handler)

    # Create a logger that only writes to the console
    console_only_logger = logging.getLogger("console_only")
    console_only_logger.setLevel(level)
    console_only_logger.handlers = []
    console_only_logger.addHandler(console_handler)
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

        # Write only the exception message to the console
        logging.getLogger("console_only").error(
            f"{exception!s}\n\nSee {Path('fabric_cicd.error.log').resolve()} for full details."
        )

        # Write exception and full stack trace to logs but not terminal
        package_logger = logging.getLogger("fabric_cicd")

        # Clear any existing handlers to prevent writing to console
        additional_info = getattr(exception, "additional_info", None)
        additional_info = "\n\nAdditional Info: \n" + additional_info if additional_info is not None else ""

        package_logger.handlers = []
        original_logger.exception(f"%s{additional_info}", exception, exc_info=(exception_type, exception, traceback))
    else:
        # If the exception is not from _common._exceptions, use the default exception handler
        sys.__excepthook__(exception_type, exception, traceback)


def print_header(message: str) -> None:
    """
    Prints a header message with a decorative line above and below it.

    Args:
        message: The header message to print.
    """
    line_separator = "#" * 100
    formatted_message = f"########## {message}"
    formatted_message = f"{formatted_message} {line_separator[len(formatted_message) + 1 :]}"

    print()  # Print a blank line before the header
    print(f"{Fore.GREEN}{Style.BRIGHT}{line_separator}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}{Style.BRIGHT}{formatted_message}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}{Style.BRIGHT}{line_separator}{Style.RESET_ALL}")
    print()
