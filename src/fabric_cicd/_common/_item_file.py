import base64
from pathlib import Path


class ItemFile:
    def __init__(self, item_path, file_path):
        self.name = file_path.name
        self.path = Path(file_path)
        self.item_path = Path(item_path)
        self.contents = self._read_file()

    def _read_file(self):
        with self.path.open(encoding="utf-8") as f:
            return f.read()

    def get_payload(self):
        byte_file = self.contents.encode("utf-8")
        return base64.b64encode(byte_file).decode("utf-8")

    def get_relative_path(self):
        return str(self.path.relative_to(self.item_path).as_posix())


if __name__ == "__main__":
    item_file = ItemFile(
        Path("C:\\Users\\jaknigh\\Repositories\\fabric-cicd-forked\\sample\\workspace\\ABC.SemanticModel\\"),
        Path(
            "C:\\Users\\jaknigh\\Repositories\\fabric-cicd-forked\\sample\\workspace\\ABC.SemanticModel\\definition\\tables\\Table.tmdl"
        ),
    )

    print(item_file.name)
    print(item_file.path)
    print(item_file.get_relative_path())
    print(item_file.contents)
