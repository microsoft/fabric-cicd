# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from fabric_cicd._common._file import File

"""
Functions and classes to manage Item operations.
"""


@dataclass
class Item:
    """A class to represent a single item."""

    type: str
    name: str
    description: str
    guid: str
    logical_id: str = field(default="")
    path: Path = field(default_factory=Path)
    item_files: list = field(default_factory=list)
    IMMUTABLE_FIELDS: ClassVar[set] = {"type", "name", "description"}

    def __setattr__(self, key, value):
        """Override setattr for 'immutable' fields"""
        if key in self.IMMUTABLE_FIELDS and hasattr(self, key):
            msg = f"item {key} is immutable"
            raise AttributeError(msg)
        super().__setattr__(key, value)

    def collect_item_files(self):
        """Collect all files in the item path"""
        self.item_files = []
        for root, _dirs, files in os.walk(self.path):
            for file in files:
                full_path = Path(root, file)
                self.item_files.append(File(self.path, full_path))
