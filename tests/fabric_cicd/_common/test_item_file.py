import base64
from pathlib import Path

import pytest

from fabric_cicd._common._item_file import ItemFile


@pytest.fixture
def sample_item_file(tmp_path):
    item_path = tmp_path / "workspace/ABC.SemanticModel"
    file_path = item_path / "definition/tables/Table.tmdl"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure the parent directories are created
    file_path.write_text("Sample contents", encoding="utf-8")
    return ItemFile(item_path=item_path, file_path=file_path)


def test_item_file_initialization(sample_item_file):
    assert sample_item_file.name == "Table.tmdl"
    assert sample_item_file.contents == "Sample contents"
    assert sample_item_file.relative_path == "definition/tables/Table.tmdl"


def test_item_file_payload(sample_item_file):
    expected_payload = base64.b64encode(b"Sample contents").decode("utf-8")
    assert sample_item_file.payload == expected_payload


def test_item_file_immutable_fields(sample_item_file):
    with pytest.raises(AttributeError):
        sample_item_file.name = "NewName"
    with pytest.raises(AttributeError):
        sample_item_file.file_path = Path("/new/path")
    with pytest.raises(AttributeError):
        sample_item_file.item_path = Path("/new/path")


def test_item_file_set_contents(sample_item_file):
    sample_item_file.contents = "New contents"
    assert sample_item_file.contents == "New contents"
