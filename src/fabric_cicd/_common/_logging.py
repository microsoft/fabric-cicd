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
from typing import ClassVar

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
