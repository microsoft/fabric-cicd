# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pytest

from fabric_cicd._common._check_utils import check_file_type


@pytest.fixture
def text_file(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("sample text")
    return file_path


@pytest.fixture
def binary_file(tmp_path):
    file_path = tmp_path / "test.bin"
    file_path.write_bytes(
        b"PK\x03\x04\x14\x00\x00\x00\x08\x00\x00\x00!\x00\xb7\xac\xce\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    )
    return file_path


@pytest.fixture
def image_file(tmp_path):
    file_path = tmp_path / "test.png"
    file_path.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01")
    return file_path


def test_check_file_type_text(text_file):
    assert check_file_type(text_file) == "text"


def test_check_file_type_binary(binary_file):
    assert check_file_type(binary_file) == "binary"


def test_check_file_type_image(image_file):
    assert check_file_type(image_file) == "image"
