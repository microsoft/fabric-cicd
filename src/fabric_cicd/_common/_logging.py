# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect
import logging
import sys
from pathlib import Path

import colorlog

from fabric_cicd._common import _exceptions


def configure_logger(level: int = logging.INFO) -> None:
    """
    Configure the logger.

    :param level: The log level to set. Must be one of the standard logging levels.
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
        colorlog.ColoredFormatter(
            "%(log_color)s[%(levelname)s] %(asctime)s - %(message)s",
            datefmt="%H:%M:%S",
            log_colors={"DEBUG": "cyan", "INFO": "green", "WARNING": "yellow", "ERROR": "red", "CRITICAL": "bold_red"},
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


def exception_handler(exception_type, exception, traceback):
    """
    Handle exceptions that are instances of any class from the _common._exceptions module.

    :param exception_type: The type of the exception.
    :param exception: The exception instance.
    :param traceback: The traceback object.
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
