from dataclasses import dataclass, field


@dataclass
class Item:
    type: str
    name: str
    description: str
    guid: str
    logical_id: str = field(default="")
    path: str = field(default="")
