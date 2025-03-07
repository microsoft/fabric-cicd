# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Provides tools for validating a Parameter.yml file."""

import logging
import sys

from fabric_cicd._common._logging import configure_logger, exception_handler
from fabric_cicd._parameterization._parameter_validation import ParameterValidation

logger = logging.getLogger(__name__)


def change_log_level(level: str = "DEBUG") -> None:
    """
    Sets the log level for all loggers within the fabric_cicd package. Currently only supports DEBUG.

    Args:
        level: The logging level to set (e.g., DEBUG).

    Examples:
        Basic usage
        >>> from fabric_cicd import change_log_level
        >>> change_log_level("DEBUG")
    """
    if level.upper() == "DEBUG":
        configure_logger(logging.DEBUG)
        logger.info("Changed log level to DEBUG")
    else:
        logger.warning(f"Log level '{level}' not supported.  Only DEBUG is supported at this time. No changes made.")


configure_logger()
sys.excepthook = exception_handler


__all__ = [
    "ParameterValidation",
    "change_log_level",
]
