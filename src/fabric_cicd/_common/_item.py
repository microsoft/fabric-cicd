import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from fabric_cicd._common._file import File


@dataclass
class Item:
    type: str
    name: str
    description: str
    guid: str
    logical_id: str = field(default="")
    path: Path = field(default_factory=Path)
    item_files: list = field(default_factory=list)
    IMMUTABLE_FIELDS: ClassVar[set] = {"type", "name", "description"}

    def __setattr__(self, key, value):
        if key in self.IMMUTABLE_FIELDS and hasattr(self, key):
            msg = f"item {key} is immutable"
            raise AttributeError(msg)
        super().__setattr__(key, value)

    def collect_item_files(self):
        for root, _dirs, files in os.walk(self.path):
            for file in files:
                full_path = Path(root, file)
                self.item_files.append(File(self.path, full_path))
