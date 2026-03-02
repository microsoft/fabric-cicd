# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Provides tools for managing and publishing items in a Fabric workspace."""

import logging
import sys

import fabric_cicd.constants as constants
from fabric_cicd._common._check_utils import check_version
from fabric_cicd._common._logging import configure_logger, exception_handler
from fabric_cicd._common._validate_input import validate_log_file_path
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


def configure_logger_with_rotation(file_path: str) -> None:
    """
    Configure fabric_cicd logging with file rotation (size-based).

    Writes only DEBUG logs to a rotating log file while keeping console output
    at INFO level. The log file rotates at 5 MB, retaining up to 7 backup
    files (35 MB total).

    Note:
        This resets logging configuration. Use as an alternative to
        ``change_log_level`` or ``disable_file_logging``, not in combination.

        The rotating log file only captures DEBUG-level messages. Exceptions
        are displayed on the console but full stack traces are not persisted
        to any log file in this mode.

        Rotation settings can be overridden before calling this function::

            import fabric_cicd.constants as constants
            constants.ROTATION_LOG_FILE_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
            constants.ROTATION_LOG_FILE_BACKUP_COUNT = 3              # 3 backups

        The rotating log file uses a simplified format compared to the default::

            Default:  2026-03-02 10:30:00,123 - ERROR - fabric_cicd.publish - message
            Rotating: 2026-03-02 10:30:00,123 - DEBUG - message

    Args:
        file_path: The path to the log file in which rotation will be applied.

    Examples:
        >>> from fabric_cicd import configure_logger_with_rotation
        >>> configure_logger_with_rotation(file_path="C:/my_app/logs/fabric.log")
    """
    configure_logger(
        level=logging.DEBUG,
        file_path=validate_log_file_path(file_path),
        use_file_rotation=True,
        suppress_debug_console=True,
        debug_only_file=True,
    )


def disable_file_logging() -> None:
    """
    Disable file logging for the fabric_cicd package.

    When called, no log file will be created and only console logging will occur
    at the default INFO level.

    Note:
        This function is intended to be used as an alternative to
        `change_log_level` or `configure_logger_with_rotation`, not in
        combination with them as this will reset logging configurations
        to INFO-level console output only.

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
    "FabricWorkspace",
    "FeatureFlag",
    "ItemType",
    "append_feature_flag",
    "change_log_level",
    "configure_logger_with_rotation",
    "deploy_with_config",
    "disable_file_logging",
    "publish_all_items",
    "unpublish_all_orphan_items",
]
