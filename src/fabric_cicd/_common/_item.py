# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions and classes to manage Item operations."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from fabric_cicd._common._file import File


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
    folder_id: str = field(default="")
    IMMUTABLE_FIELDS: ClassVar[set] = {"type", "name", "description"}
    skip_publish: bool = field(default=False)

    def __setattr__(self, key: str, value: any) -> None:
        """
        Override setattr for 'immutable' fields.

        Args:
            key: The attribute name.
            value: The attribute value.
        """
        if key in self.IMMUTABLE_FIELDS and hasattr(self, key):
            msg = f"item {key} is immutable"
            raise AttributeError(msg)
        super().__setattr__(key, value)

    @property
    def relative_path(self) -> str:
        """Return the relative path of the file."""
        return str(self.file_path.relative_to(self.item_path).as_posix())

    def collect_item_files(self) -> None:
        """Collect all files in the item path."""
        self.item_files = []
        for root, _dirs, files in os.walk(self.path):
            for file in files:
                full_path = Path(root, file)
                self.item_files.append(File(self.path, full_path))
