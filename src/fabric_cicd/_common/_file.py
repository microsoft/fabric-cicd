import base64
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from fabric_cicd._common._check_utils import check_file_type


@dataclass()
class File:
    item_path: Path
    file_path: Path
    type: str = field(default="text", init=False)
    contents: str = field(default="", init=False)
    IMMUTABLE_FIELDS: ClassVar[set] = {"item_path", "file_path"}

    def __setattr__(self, key, value):
        ## override setattr for contents
        if key in self.IMMUTABLE_FIELDS and hasattr(self, key):
            msg = f"item {key} is immutable"
            raise AttributeError(msg)

        # Image file contents cannot be set
        if key == "contents" and self.type != "text":
            msg = f"item {key} is immutable for non text files"
            raise AttributeError(msg)
        super().__setattr__(key, value)

    def __post_init__(self):
        file_type = check_file_type(self.file_path)

        if file_type != "text":
            self.contents = self.file_path.read_bytes()
        else:
            self.contents = self.file_path.read_text()

        # set after as imagine contents are now immutable
        self.type = file_type

    @property
    def name(self):
        return self.file_path.name

    @property
    def relative_path(self):
        return str(self.file_path.relative_to(self.item_path).as_posix())

    @property
    def base64_payload(self):
        byte_file = self.contents.encode("utf-8") if self.type == "text" else self.contents

        return {
            "path": self.relative_path,
            "payload": base64.b64encode(byte_file).decode("utf-8"),
            "payloadType": "InlineBase64",
        }
