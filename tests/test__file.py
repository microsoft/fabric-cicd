# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
from pathlib import Path

import pytest

from fabric_cicd._common._file import File

SAMPLE_BINARY_DATA = b"PK\x03\x04\x14\x00\x00\x00\x08\x00\x00\x00!\x00\xb7\xac\xce\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
SAMPLE_IMAGE_DATA = "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
SAMPLE_TEXT_DATA = "sample text"


@pytest.fixture
def text_file(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text(SAMPLE_TEXT_DATA)
    return file_path


@pytest.fixture
def binary_file(tmp_path):
    file_path = tmp_path / "test.bin"
    file_path.write_bytes(SAMPLE_BINARY_DATA)
    return file_path


@pytest.fixture
def image_file(tmp_path):
    file_path = tmp_path / "test.png"
    file_path.write_bytes(SAMPLE_IMAGE_DATA)
    return file_path


def test_file_text(text_file):
    item_path = text_file.parent
    file = File(item_path=item_path, file_path=text_file)
    assert file.type == "text"
    assert file.contents == SAMPLE_TEXT_DATA
    assert file.name == "test.txt"
    assert file.relative_path == "test.txt"
    expected_payload = base64.b64encode(SAMPLE_TEXT_DATA.encode("utf-8")).decode("utf-8")
    assert file.base64_payload == {
        "path": "test.txt",
        "payload": expected_payload,
        "payloadType": "InlineBase64",
    }


def test_file_binary(binary_file):
    item_path = binary_file.parent
    file = File(item_path=item_path, file_path=binary_file)
    assert file.type == "binary"
    assert file.contents == SAMPLE_BINARY_DATA
    assert file.name == "test.bin"
    assert file.relative_path == "test.bin"
    expected_payload = base64.b64encode(SAMPLE_BINARY_DATA).decode("utf-8")
    assert file.base64_payload == {
        "path": "test.bin",
        "payload": expected_payload,
        "payloadType": "InlineBase64",
    }
    with pytest.raises(AttributeError):
        file.contents = "new contents"


def test_immutable_fields(binary_file):
    item_path = binary_file.parent
    file = File(item_path=item_path, file_path=binary_file)
    with pytest.raises(AttributeError):
        file.item_path = Path("/new/path")
    with pytest.raises(AttributeError):
        file.file_path = Path("/new/path")
    with pytest.raises(AttributeError):
        file.contents = "new contents"
