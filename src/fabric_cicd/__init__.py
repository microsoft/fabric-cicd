# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Provides tools for managing and publishing items in a Fabric workspace."""

import logging
import sys

import fabric_cicd.constants as constants
from fabric_cicd._common._deployment_result import DeploymentResult, DeploymentStatus
from fabric_cicd._common._git_diff_utils import get_changed_items
from fabric_cicd._common._logging import configure_logger, exception_handler, get_file_handler
from fabric_cicd._common._validate_env_vars import _get_fabric_fqdn_url, is_env_flag_enabled, validate_api_url
from fabric_cicd._common._validate_input import validate_workspace_id
from fabric_cicd.constants import EnvVar, FeatureFlag, ItemType
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

    This controls the verbosity of both console output and file logging (if enabled).
    File logging is controlled independently via the ``FABRIC_CICD_FILE_LOGGING_ENABLED``
    environment variable — this function never creates or removes the log file.

    Args:
        level: The logging level to set (e.g., DEBUG).

    Warning:
        When DEBUG is enabled and file logging is active, the log file may contain
        sensitive information such as API endpoints, workspace identifiers, and
        request/response payloads. You are responsible for managing and deleting
        these files in accordance with your organization's security policies.

    Examples:
        Basic usage
        >>> from fabric_cicd import change_log_level
        >>> change_log_level("DEBUG")
    """
    if level.upper() == "DEBUG":
        file_logging_enabled = is_env_flag_enabled(EnvVar.FILE_LOGGING_ENABLED.value)
        configure_logger(logging.DEBUG, disable_log_file=not file_logging_enabled)
        logger.info("Changed log level to DEBUG")
        if file_logging_enabled:
            logger.warning(
                "The log file may contain sensitive information (API endpoints, workspace IDs, request/response payloads). "
                "You are responsible for managing and deleting this file per your organization's security policies."
            )
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


def configure_fabric_fqdn(workspace_id: str) -> None:
    """
    Configure Fabric API URLs for private-link-enabled workspaces.

    Updates the global Fabric API URL constants to use the FQDN format required
    for private-link-enabled workspaces. Call this function before initializing
    a FabricWorkspace if you are using a private-link-enabled workspace.

    Args:
        workspace_id: The workspace ID string in standard GUID format with dashes
            (e.g., "f953f3da-c5f0-4e36-a644-c85933e35e2f").

    Side Effects:
        Updates the module-level constants in fabric_cicd.constants:
        - FABRIC_API_ROOT_URL: Set to the FQDN URL derived from workspace_id
        - DEFAULT_API_ROOT_URL: Set to the same FQDN URL

    Examples:
        Basic usage with FabricWorkspace:
        >>> from fabric_cicd import configure_fabric_fqdn, FabricWorkspace
        >>> from azure.identity import AzureCliCredential
        >>>
        >>> workspace_id = "f953f3da-c5f0-4e36-a644-c85933e35e2f"
        >>> configure_fabric_fqdn(workspace_id)
        >>>
        >>> token_credential = AzureCliCredential()
        >>> workspace = FabricWorkspace(
        ...     workspace_id=workspace_id,
        ...     repository_directory="/path/to/workspace",
        ...     token_credential=token_credential
        ... )
    """
    workspace_id = validate_workspace_id(workspace_id)
    fqdn_url = _get_fabric_fqdn_url(workspace_id)
    fqdn_url = validate_api_url(fqdn_url, "configure_fabric_fqdn")

    if constants.FABRIC_API_ROOT_URL != "https://api.fabric.microsoft.com":
        logger.warning(
            f"configure_fabric_fqdn: overwriting previously set FABRIC_API_ROOT_URL '{constants.FABRIC_API_ROOT_URL}'"
        )

    constants.FABRIC_API_ROOT_URL = fqdn_url
    constants.DEFAULT_API_ROOT_URL = fqdn_url


if is_env_flag_enabled(EnvVar.FILE_LOGGING_ENABLED.value):
    configure_logger()
else:
    configure_logger(disable_log_file=True)
sys.excepthook = exception_handler

__all__ = [
    "DeploymentResult",
    "DeploymentStatus",
    "FabricWorkspace",
    "FeatureFlag",
    "ItemType",
    "append_feature_flag",
    "change_log_level",
    "configure_external_file_logging",
    "configure_fabric_fqdn",
    "deploy_with_config",
    "disable_file_logging",
    "get_changed_items",
    "publish_all_items",
    "unpublish_all_orphan_items",
]
