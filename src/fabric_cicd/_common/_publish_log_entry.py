# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for structured publish logging."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PublishLogEntry:
    """Represents a structured log entry for publish operations."""
    
    name: str
    item_type: str
    success: bool
    error: Optional[str]
    start_time: datetime
    end_time: datetime
    guid: Optional[str] = None
    
    @property
    def duration_seconds(self) -> float:
        """Calculate the duration of the operation in seconds."""
        return (self.end_time - self.start_time).total_seconds()
    
    def to_dict(self) -> dict:
        """Convert the log entry to a dictionary for serialization."""
        return {
            "name": self.name,
            "item_type": self.item_type,
            "success": self.success,
            "error": self.error,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "guid": self.guid
        }
