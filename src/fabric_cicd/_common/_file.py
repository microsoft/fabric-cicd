# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions and classes to manage file operations."""

import base64
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from fabric_cicd._common._check_utils import check_file_type
from fabric_cicd._common._exceptions import FileTypeError

logger = logging.getLogger(__name__)


@dataclass()
class File:
    """A class to represent a single file in an item object."""

    item_path: Path
    file_path: Path
    type: str = field(default="text", init=False)
    contents: str = field(default="", init=False)
    IMMUTABLE_FIELDS: ClassVar[set] = {"item_path", "file_path"}

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

        # Image file contents cannot be set
        if key == "contents" and self.type != "text":
            msg = f"item {key} is immutable for non text files"
            raise AttributeError(msg)
        super().__setattr__(key, value)

    def __post_init__(self) -> None:
        """After initializing the object, read the file contents and set the type."""
        file_type = check_file_type(self.file_path)

        if file_type != "text":
            try:
                self.contents = self.file_path.read_bytes()
            except Exception as e:
                msg = (
                    f"Error reading file {self.file_path} as binary.  "
                    f"Please submit this as a bug https://github.com/microsoft/fabric-cicd/issues/new?template=1-bug.yml.md. Exception: {e}"
                )
                FileTypeError(msg, logger)
        else:
            try:
                self.contents = self.file_path.read_text(encoding="utf-8")
            except Exception as e:
                msg = (
                    f"Error reading file {self.file_path} as text.  "
                    f"Please submit this as a bug https://github.com/microsoft/fabric-cicd/issues/new?template=1-bug.yml.md. Exception: {e}"
                )
                FileTypeError(msg, logger)

        # set after as image contents are now immutable
        self.type = file_type

    @property
    def name(self) -> str:
        """Return the file name."""
        return self.file_path.name

    @property
    def relative_path(self) -> str:
        """Return the relative path of the file."""
        return str(self.file_path.relative_to(self.item_path).as_posix())

    @property
    def base64_payload(self) -> dict:
        """Return the file contents as a base64 encoded payload."""
        byte_file = self.contents.encode("utf-8") if self.type == "text" else self.contents

        return {
            "path": self.relative_path,
            "payload": base64.b64encode(byte_file).decode("utf-8"),
            "payloadType": "InlineBase64",
        }
