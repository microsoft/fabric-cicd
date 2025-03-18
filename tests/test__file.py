# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
from pathlib import Path

import pytest

from fabric_cicd._common._file import File

SAMPLE_IMAGE_DATA = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
SAMPLE_TEXT_DATA = "sample text"


@pytest.fixture
def text_file(tmp_path):
    item_path = tmp_path / "workspace/ABC.SemanticModel"
    file_path = item_path / "definition/tables/Table.tmdl"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure the parent directories are created
    file_path.write_text(SAMPLE_TEXT_DATA)
    return File(item_path=item_path, file_path=file_path)


@pytest.fixture
def image_file(tmp_path):
    item_path = tmp_path / "workspace/ABC.Report"
    file_path = item_path / "StaticResources/RegisteredResources/image.png"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure the parent directories are created
    file_path.write_bytes(SAMPLE_IMAGE_DATA)
    return File(item_path=item_path, file_path=file_path)


def test_file_text_initialization(text_file):
    assert text_file.name == "Table.tmdl"
    assert text_file.contents == SAMPLE_TEXT_DATA
    assert text_file.relative_path == "definition/tables/Table.tmdl"


def test_file_text_payload(text_file):
    expected_payload = base64.b64encode(SAMPLE_TEXT_DATA.encode("utf-8")).decode("utf-8")
    assert text_file.base64_payload == {
        "path": "definition/tables/Table.tmdl",
        "payload": expected_payload,
        "payloadType": "InlineBase64",
    }


def test_file_text_set_contents(text_file):
    text_file.contents = "New contents"
    assert text_file.contents == "New contents"


def test_file_image_immutable_fields(image_file):
    with pytest.raises(AttributeError):
        image_file.item_path = Path("/new/path")
    with pytest.raises(AttributeError):
        image_file.file_path = Path("/new/path")
    with pytest.raises(AttributeError):
        image_file.contents = "new contents"


def test_file_image_payload(image_file):
    expected_payload = base64.b64encode(SAMPLE_IMAGE_DATA).decode("utf-8")
    assert image_file.base64_payload == {
        "path": "StaticResources/RegisteredResources/image.png",
        "payload": expected_payload,
        "payloadType": "InlineBase64",
    }


def test_file_text_special_characters(tmp_path):
    special_text = (
        "Greek: a, β, y, δ, ε, ζ\n"
        "Latin: æ, ø, å, ß, é, ñ, ü\n"
        "Cyrillic: Ж, Д, и, ю\n"
        "Arabic: مرحبا, سلام\n"
        "Chinese: 你好, 世界\n"
        "Emoji: 😊, 🚀, 🌟\n"
        "Symbols: ©, ®, ™, €, £, ¥, ∞, ≠, ≤, ≥"
    )
    item_path = tmp_path / "workspace/SpecialTextModel"
    file_path = item_path / "definition/special.txt"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(special_text, encoding="utf-8")
    file_obj = File(item_path=item_path, file_path=file_path)
    expected_payload = base64.b64encode(special_text.encode("utf-8")).decode("utf-8")
    assert file_obj.contents == special_text
    assert file_obj.base64_payload == {
        "path": "definition/special.txt",
        "payload": expected_payload,
        "payloadType": "InlineBase64",
    }
