from dataclasses import dataclass, field
from typing import ClassVar


@dataclass
class Item:
    type: str
    name: str
    description: str
    guid: str
    logical_id: str = field(default="")
    path: str = field(default="")
    IMMUTABLE_FIELDS: ClassVar[set] = {"type", "name", "description"}

    def __setattr__(self, key, value):
        if key in self.IMMUTABLE_FIELDS and hasattr(self, key):
            msg = f"item {key} is immutable"
            raise AttributeError(msg)
        super().__setattr__(key, value)
