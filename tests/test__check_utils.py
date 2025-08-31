# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

import pytest

from fabric_cicd._common._check_utils import check_file_type, check_valid_json, check_valid_json_content


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


@pytest.fixture
def json_file(tmp_path):
    file_path = tmp_path / "test.json"
    file_path.write_text('{"key": "value", "number": 123}')
    return file_path


@pytest.fixture
def schedules_like_file(tmp_path):
    """Create a file like .schedules with JSON content but no .json extension."""
    file_path = tmp_path / ".schedules"
    schedules_content = {"schedules": [{"jobType": "Execute", "enabled": True, "cronExpression": "0 0 12 * * ?"}]}
    file_path.write_text(json.dumps(schedules_content))
    return file_path


@pytest.fixture
def invalid_json_file(tmp_path):
    file_path = tmp_path / "invalid.json"
    file_path.write_text('{"key": "value" invalid json}')
    return file_path


@pytest.fixture
def text_file_no_json(tmp_path):
    file_path = tmp_path / "test.py"
    file_path.write_text('print("Hello World")')
    return file_path


def test_check_valid_json_with_json_file(json_file):
    """Test check_valid_json with a standard .json file."""
    assert check_valid_json(json_file) is True


def test_check_valid_json_with_schedules_file(schedules_like_file):
    """Test check_valid_json with .schedules file (JSON content, no .json extension)."""
    assert check_valid_json(schedules_like_file) is True


def test_check_valid_json_with_invalid_json(invalid_json_file):
    """Test check_valid_json with invalid JSON content."""
    assert check_valid_json(invalid_json_file) is False


def test_check_valid_json_with_non_json_file(text_file_no_json):
    """Test check_valid_json with non-JSON text file."""
    assert check_valid_json(text_file_no_json) is False


def test_check_valid_json_with_nonexistent_file(tmp_path):
    """Test check_valid_json with nonexistent file."""
    nonexistent_file = tmp_path / "nonexistent.json"
    assert check_valid_json(nonexistent_file) is False


@pytest.fixture
def real_schedules_file(tmp_path):
    """Create a realistic .schedules file with exact structure like fabric-cicd uses."""
    # Create a DataPipeline directory structure
    pipeline_dir = tmp_path / "Test Pipeline.DataPipeline"
    pipeline_dir.mkdir()

    schedules_file = pipeline_dir / ".schedules"
    schedules_content = {
        "schedules": [
            {
                "jobType": "Execute",
                "enabled": True,
                "cronExpression": "0 0 12 * * ?",
                "timeZone": "UTC",
                "description": "Daily execution at noon",
            },
            {
                "jobType": "Refresh",
                "enabled": False,
                "cronExpression": "0 0 6 * * ?",
                "timeZone": "UTC",
                "description": "Morning refresh",
            },
        ]
    }
    schedules_file.write_text(json.dumps(schedules_content, indent=2))
    return schedules_file


def test_schedules_file_json_validation_and_structure(real_schedules_file):
    """Test that .schedules files are properly validated and contain expected structure."""
    # Test that check_valid_json correctly identifies .schedules as valid JSON
    assert check_valid_json(real_schedules_file) is True

    # Verify the file has the expected structure for key_value_replace
    content = real_schedules_file.read_text(encoding="utf-8")
    data = json.loads(content)

    # Verify the structure matches what the JSONPath expression expects
    assert "schedules" in data
    assert isinstance(data["schedules"], list)
    assert len(data["schedules"]) >= 1

    # Find Execute job and verify it has enabled field
    execute_jobs = [schedule for schedule in data["schedules"] if schedule.get("jobType") == "Execute"]
    assert len(execute_jobs) >= 1

    execute_job = execute_jobs[0]
    assert "enabled" in execute_job
    assert isinstance(execute_job["enabled"], bool)

    # Verify file path ends with .schedules
    assert real_schedules_file.name == ".schedules"
    assert str(real_schedules_file).endswith(".schedules")


def test_schedules_file_jsonpath_compatibility(real_schedules_file):
    """Test that .schedules files work with the specific JSONPath expression used in parameter.yml."""
    try:
        from jsonpath_ng.ext import parse
    except ImportError:
        pytest.skip("jsonpath_ng not available for testing")

    # Read and parse the .schedules file
    content = real_schedules_file.read_text(encoding="utf-8")
    data = json.loads(content)

    # Test the exact JSONPath expression from the parameter.yml
    jsonpath_expr = parse('$.schedules[?(@.jobType=="Execute")].enabled')
    matches = [match.value for match in jsonpath_expr.find(data)]

    # Should find at least one enabled field from Execute jobs
    assert len(matches) >= 1
    assert all(isinstance(match, bool) for match in matches)

    # Verify we can access the specific value that would be replaced
    first_match = matches[0]
    assert first_match is True  # Our test data has enabled=True

    print(f"✓ JSONPath found {len(matches)} enabled values: {matches}")
    print("✓ .schedules file is compatible with key_value_replace parameter")


def test_real_sample_schedules_file():
    """Test that the actual sample .schedules file works with our function."""
    from pathlib import Path

    schedules_file = Path("sample/workspace/Run Hello World.DataPipeline/.schedules")

    # Skip if sample file doesn't exist (optional test)
    if not schedules_file.exists():
        pytest.skip("Sample .schedules file not found")

    # Test that our function works with the real file
    assert check_valid_json(schedules_file) is True

    # Verify the structure contains what we expect
    content = schedules_file.read_text(encoding="utf-8")
    data = json.loads(content)

    assert "schedules" in data
    assert isinstance(data["schedules"], list)

    print(f"✓ Real sample .schedules file is valid JSON with {len(data['schedules'])} schedules")


def test_check_valid_json_content_with_valid_json():
    """Test check_valid_json_content with valid JSON string."""
    valid_json = '{"key": "value", "number": 123, "boolean": true}'
    assert check_valid_json_content(valid_json) is True


def test_check_valid_json_content_with_invalid_json():
    """Test check_valid_json_content with invalid JSON string."""
    invalid_json = '{"key": "value" invalid json}'
    assert check_valid_json_content(invalid_json) is False


def test_check_valid_json_content_with_empty_string():
    """Test check_valid_json_content with empty string."""
    assert check_valid_json_content("") is False


def test_check_valid_json_content_with_schedules_structure():
    """Test check_valid_json_content with realistic schedules JSON structure."""
    schedules_json = json.dumps({
        "schedules": [{"jobType": "Execute", "enabled": True, "cronExpression": "0 0 12 * * ?"}]
    })
    assert check_valid_json_content(schedules_json) is True
