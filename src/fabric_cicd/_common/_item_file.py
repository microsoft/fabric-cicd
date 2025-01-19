import base64
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(kw_only=True)
class ItemFile:
    item_path: Path
    file_path: Path
    contents: str = field(default="", init=False)
    name: str = field(default="", init=False)
    _initializing: bool = field(default=True, init=False, repr=False)

    def __post_init__(self):
        self.contents = self.file_path.read_text(encoding="utf-8")
        self.name = self.file_path.name
        self._initializing = False

    @property
    def payload(self):
        byte_file = self.contents.encode("utf-8")
        return base64.b64encode(byte_file).decode("utf-8")

    @property
    def relative_path(self):
        return str(self.file_path.relative_to(self.item_path).as_posix())

    def __setattr__(self, key, value):
        if not self._initializing and key != "contents" and hasattr(self, key):
            msg = f"{key} is immutable"
            raise AttributeError(msg)
        super().__setattr__(key, value)
