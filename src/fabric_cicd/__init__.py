# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Provides tools for managing and publishing items in a Fabric workspace."""

import logging
import sys

import fabric_cicd.constants as constants
from fabric_cicd._common._check_utils import check_version
from fabric_cicd._common._deployment_result import DeploymentResult, DeploymentStatus
from fabric_cicd._common._logging import configure_logger, exception_handler, get_file_handler
from fabric_cicd.constants import FeatureFlag, ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import deploy_with_config, publish_all_items, unpublish_all_orphan_items

logger = logging.getLogger(__name__)


def append_feature_flag(feature: str) -> None:
    """
    Append a feature flag to the global feature_flag set.

    Args:
        feature: The feature flag to be included.

    Examples:
        Basic usage
        >>> from fabric_cicd import append_feature_flag
        >>> append_feature_flag("enable_lakehouse_unpublish")
    """
    constants.FEATURE_FLAG.add(feature)


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


def configure_external_file_logging(external_logger: logging.Logger) -> None:
    """
    Configure fabric_cicd package logging to integrate with an external logger's
    file handler. This is an advanced alternative to the default file logging
    configuration when level is set to DEBUG via `change_log_level()`.

    Extracts the file handler from the provided logger and configures fabric_cicd
    to append only DEBUG logs (e.g., API request/response details) to the same file.
    The external logger retains full ownership of the handler, including file
    rotation (if applicable) and lifecycle management.

    Note:
        - This function resets logging configuration. Use as an alternative to
        ``change_log_level()`` or ``disable_file_logging()``, not in combination

        - Only DEBUG logs from the fabric_cicd package are written to the log file.
        Exception messages are displayed on the console, but full stack traces
        are not written to the external log file

        - Console output remains at INFO level (default fabric_cicd console behavior)

    Args:
        external_logger: The external logger instance that has
            a `FileHandler` or `RotatingFileHandler` attached.

    Raises:
        ValueError: If no file handler is found on the provided logger.

    Examples:
        General usage:
        >>> import logging
        >>> from logging.handlers import RotatingFileHandler
        >>> from fabric_cicd import configure_external_file_logging
        ...
        >>> # Set up your own logger with a file handler
        >>> my_logger = logging.getLogger("MyApp")
        >>> handler = RotatingFileHandler("app.log", maxBytes=5*1024*1024, backupCount=7)
        >>> handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        >>> my_logger.addHandler(handler)
        ...
        >>> # Configure fabric_cicd to use the same file
        >>> configure_external_file_logging(my_logger)
    """
    # Extract file handler from external logger
    file_handler = get_file_handler(external_logger)
    if file_handler is None:
        msg = "No FileHandler or RotatingFileHandler found on the provided logger."
        raise ValueError(msg)

    configure_logger(
        level=logging.DEBUG,
        suppress_debug_console=True,
        debug_only_file=True,
        external_file_handler=file_handler,
    )


def disable_file_logging() -> None:
    """
    Disable file logging for the fabric_cicd package.

    When called, no log file will be created and only console logging will occur
    at the default INFO level.

    Note:
        - This function is intended to be used as an alternative to
        `change_log_level()` or `configure_external_file_logging()`, not in
        combination with them as this will reset logging configurations
        to INFO-level console output only.
        - Exception messages will still be displayed on the console, but full
        stack traces will not be written to any log file or console.

    Examples:
        Basic usage
        >>> from fabric_cicd import disable_file_logging
        >>> disable_file_logging()
    """
    configure_logger(disable_log_file=True)


configure_logger()
sys.excepthook = exception_handler

if not constants.VERSION_CHECK_DISABLED:
    check_version()

__all__ = [
    "DeploymentResult",
    "DeploymentStatus",
    "FabricWorkspace",
    "FeatureFlag",
    "ItemType",
    "append_feature_flag",
    "change_log_level",
    "configure_external_file_logging",
    "deploy_with_config",
    "disable_file_logging",
    "publish_all_items",
    "unpublish_all_orphan_items",
]
