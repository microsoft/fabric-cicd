import logging
import sys

from fabric_cicd._common._logging import configure_logger, exception_handler
from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import publish_all_items, unpublish_all_orphan_items

logger = logging.getLogger(__name__)

def change_log_level(level: str = "DEBUG") -> None:
    """
    Sets the log level for all loggers within the fabric_cicd package.  Currently only supports DEBUG.

    This function configures the logging level for all loggers in the fabric_cicd package to DEBUG,
    which is useful for development and debugging purposes.

    :param level: The logging level to set (e.g., DEBUG).
    :type level: int

    Examples:
        Basic usage:
            >>> from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, change_log_level
            >>> change_log_level("DEBUG")
            >>> workspace = FabricWorkspace(
            ...     workspace_id="your-workspace-id",
            ...     repository_directory="/path/to/repo",
            ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
            ... )
            >>> publish_all_items(workspace)
            >>> unpublish_orphaned_items(workspace)
    """
    if level.upper() == "DEBUG":
        configure_logger(logging.DEBUG)
        logger.info(f"Changed log level to DEBUG")
    else:
        logger.warning(f"Log level '{level}' not supported.  Only DEBUG is supported at this time. No changes made.")


configure_logger()
sys.excepthook = exception_handler

__all__ = [
    "FabricWorkspace",
    "publish_all_items",
    "unpublish_all_orphan_items",
    "change_log_level",
]
