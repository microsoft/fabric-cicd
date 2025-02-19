import base64
from dataclasses import dataclass, field
from pathlib import Path

import filetype


@dataclass(kw_only=True)
class ItemFile:
    item_path: Path
    file_path: Path
    fabric_workspace_obj: callable = None
    func_process_file: callable = None
    contents: str = field(default="", init=False)
    name: str = field(default="", init=False)
    _initializing: bool = field(default=True, init=False, repr=False)

    def __post_init__(self):
        if self.is_image:
            self.contents = self.file_path.read_bytes()
        else:
            self.contents = self.file_path.read_text(encoding="utf-8")

        self.name = self.file_path.name
        self._initializing = False

    @property
    def relative_path(self):
        return str(self.file_path.relative_to(self.item_path).as_posix())

    def __setattr__(self, key, value):
        if not self._initializing and key != "contents" and hasattr(self, key):
            msg = f"{key} is immutable"
            raise AttributeError(msg)
        super().__setattr__(key, value)

    @property
    def is_image(self):
        kind = filetype.guess(self.file_path)
        if kind is None:
            return False
        return kind.mime.startswith("image/")

    @property
    def base64_payload(self, fabric_workspace):
        if not self.is_image:
            # Replace values within file
            if self.func_process_file:
                self.contents = self.func_process_file()

            # Replace logical IDs with deployed GUIDs.
            if self.name != ".platform":
                self.contents = self._replace_logical_ids()
                self.contents = self._replace_parameters()

        byte_file = self.contents.encode("utf-8")

        return {
            "path": self.relative_path,
            "payload": base64.b64encode(byte_file).decode("utf-8"),
            "payloadType": "InlineBase64",
        }

    def _replace_logical_ids(self, item_file_obj):
        """
        Replaces logical IDs with deployed GUIDs in the raw file content.

        :param raw_file: The raw file content where logical IDs need to be replaced.
        :return: The raw file content with logical IDs replaced by GUIDs.
        """
        for items in self.repository_items.values():
            for item_dict in items.values():
                logical_id = item_dict["logical_id"]
                item_guid = item_dict["guid"]

                if logical_id in raw_file:
                    if item_guid == "":
                        msg = f"Cannot replace logical ID '{logical_id}' as referenced item is not yet deployed."
                        raise ParsingError(msg, logger)
                    raw_file = raw_file.replace(logical_id, item_guid)

        return raw_file

    def _replace_parameters(self, item_file_obj):
        """
        Replaces values found in parameter file with the chosen environment value.

        :param raw_file: The raw file content where parameter values need to be replaced.
        """
        if "find_replace" in self.environment_parameter:
            for key, parameter_dict in self.environment_parameter["find_replace"].items():
                if key in raw_file and self.environment in parameter_dict:
                    # replace any found references with specified environment value
                    raw_file = raw_file.replace(key, parameter_dict[self.environment])

        return raw_file
