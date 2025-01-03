"""
Initializes the fabric_cicd package and sets up logging.
"""

from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import publish_all_items, unpublish_all_orphan_items
import logging
import colorlog


def _configure_logger(level: int = logging.INFO) -> None:
    """
    Configure the logger.

    :param level: The log level to set. Must be one of the standard logging levels.
    """
    # Configure logging
    log_formatter = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"

    logging.basicConfig(
        level=(
            # For non-fabric_cicd packages: INFO if DEBUG, else ERROR
            logging.INFO
            if level == logging.DEBUG
            else logging.ERROR
        ),
        format=log_formatter,
        filename="fabric_cicd.error.log",
        filemode="w",
    )

    package_logger = logging.getLogger("fabric_cicd")
    package_logger.setLevel(level)
    package_logger.handlers = []

    # Configure Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    console_formatter = colorlog.ColoredFormatter(
        "%(log_color)s[%(levelname)s] %(asctime)s - %(message)s",
        datefmt="%H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )
    console_handler.setFormatter(console_formatter)

    # Add the handler to the logger
    package_logger.addHandler(console_handler)


# Initial logger configuration
_configure_logger()


def enable_debug_log() -> None:
    """
    Set the log level for all loggers within the fabric_cicd package to DEBUG.
    """

    _configure_logger(logging.DEBUG)


__all__ = [
    "FabricWorkspace",
    "publish_all_items",
    "unpublish_all_orphan_items",
    "enable_debug_log",
]
