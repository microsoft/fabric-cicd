import logging
import sys

from fabric_cicd._common._logging import configure_logger, exception_handler
from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import publish_all_items, unpublish_all_orphan_items


def enable_debug_log() -> None:
    """
    Sets the log level for all loggers within the fabric_cicd package to DEBUG.

    This function configures the logging level for all loggers in the fabric_cicd package to DEBUG,
    which is useful for development and debugging purposes.

    Examples:
        Basic usage:
            >>> from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, set_log_level_to_debug
            >>> set_log_level_to_debug()
            >>> workspace = FabricWorkspace(
            ...     workspace_id="your-workspace-id",
            ...     repository_directory="/path/to/repo",
            ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
            ... )
            >>> publish_all_items(workspace)
            >>> unpublish_orphaned_items(workspace)
    """
    configure_logger(logging.DEBUG)


configure_logger()
sys.excepthook = exception_handler

__all__ = [
    "FabricWorkspace",
    "publish_all_items",
    "unpublish_all_orphan_items",
    "enable_debug_log",
]
