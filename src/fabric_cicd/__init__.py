import logging
import sys

from fabric_cicd._common._logging import configure_logger, exception_handler
from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import publish_all_items, unpublish_all_orphan_items


def enable_debug_log() -> None:
    """
    Set the log level for all loggers within the fabric_cicd package to DEBUG.
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
