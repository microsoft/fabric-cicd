# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Provides tools for managing and publishing items in a Fabric workspace."""

import logging
import sys

import fabric_cicd.constants as constants
from fabric_cicd._common._check_utils import check_version
from fabric_cicd._common._logging import configure_logger, exception_handler
from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import publish_all_items, unpublish_all_orphan_items

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


configure_logger()
sys.excepthook = exception_handler

check_version()

__all__ = [
    "FabricWorkspace",
    "append_feature_flag",
    "change_log_level",
    "publish_all_items",
    "unpublish_all_orphan_items",
]
