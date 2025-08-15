# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for structured publish/unpublish logging."""

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional

# This allows type hints without circular imports (fabric_workspace imports this module)
if TYPE_CHECKING:
    from fabric_cicd.fabric_workspace import FabricWorkspace


@dataclass
class DeploymentLogEntry:
    """Represents a structured log entry for publish/unpublish operations."""

    name: str
    item_type: str
    success: bool
    error: Optional[str]
    start_time: datetime
    end_time: datetime
    guid: Optional[str] = None
    operation_type: str = "publish"  # or "unpublish"

    @property
    def duration_seconds(self) -> float:
        """Calculate the duration of the operation in seconds."""
        return (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> dict[str, str | bool | float | None]:
        """Convert the log entry to a dictionary for serialization."""
        return {
            "name": self.name,
            "item_type": self.item_type,
            "success": self.success,
            "error": self.error,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "guid": self.guid,
            "operation_type": self.operation_type,
        }


@contextmanager
def log_operation(
    workspace_obj: "FabricWorkspace",
    operation_type: str,
    item_name: str,
    item_type: str,
    item_guid: Optional[str] = None,
) -> Generator[dict[str, str | bool | None], None, None]:
    """
    Context manager for structured operation logging.

    Args:
        workspace_obj: The FabricWorkspace object
        operation_type: "publish" or "unpublish"
        item_name: Name of the item
        item_type: Type of the item
        item_guid: Optional GUID of the item

    Yields:
        dict: Context dictionary with operation state
    """
    if operation_type not in ("publish", "unpublish"):
        msg = f"Unsupported operation_type: {operation_type}. Must be 'publish' or 'unpublish'"
        raise ValueError(msg)

    # Initialize operation state
    start_time = datetime.now()
    context: dict[str, str | bool | None] = {
        "success": True,
        "error_msg": None,
        "item_guid": item_guid,
    }
    try:
        yield context
    except Exception as e:
        # Mark operation as failed and capture error details
        context["success"] = False
        context["error_msg"] = str(e)
        raise  # Re-raise the exception (allows exception to propagate)
    finally:
        end_time = datetime.now()

        # Create log entry
        log_entry = DeploymentLogEntry(
            name=item_name,
            item_type=item_type,
            success=bool(context["success"]),
            error=context["error_msg"] if isinstance(context["error_msg"], str) else None,
            start_time=start_time,
            end_time=end_time,
            guid=context["item_guid"] if isinstance(context["item_guid"], str) else None,
            operation_type=operation_type,
        )

        # Append to appropriate log list
        if operation_type == "publish":
            workspace_obj.publish_log_entries.append(log_entry)
        elif operation_type == "unpublish":
            workspace_obj.unpublish_log_entries.append(log_entry)
