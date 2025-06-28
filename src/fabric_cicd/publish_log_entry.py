"""
PublishLogEntry dataclass for logging published items in a Fabric workspace.
This dataclass captures the details of each published item, including its name,
type, success status, any error messages, and timestamps for when the publish
operation started and ended.
"""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PublishLogEntry:
    """Dataclass to represent a log entry for published items."""

    name: str
    item_type: str
    success: bool
    error: Optional[str]
    start_time: datetime
    end_time: datetime

    def to_dict(self) -> dict:
        """Convert the PublishLogEntry to a dictionary."""
        d = asdict(self)
        # convert datetimes to ISO strings for JSON friendliness
        d["start_time"] = self.start_time.isoformat()
        d["end_time"] = self.end_time.isoformat()
        return d
