# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pytest

from fabric_cicd._parameter._parameter import Parameter

SAMPLE_PARAMETER_FILE = """ 
find_replace:
    # Required Fields 
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World"] 
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: 
"""

SAMPLE_PLATFORM_FILE = """
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {
    "type": "Notebook",
    "displayName": "Hello World",
    "description": "Sample notebook"
  },
  "config": {
    "version": "2.0",
    "logicalId": "99b570c5-0c79-9dc4-4c9b-fa16c621384c"
  }
}
"""

SAMPLE_NOTEBOOK_FILE = "print('Hello World and replace connection string: db52be81-c2b2-4261-84fa-840c67f4bbd0')"


@pytest.fixture
def item_type_in_scope():
    return ["Notebook", "DataPipeline", "Environment"]


@pytest.fixture
def target_environment():
    return "PPE"


@pytest.fixture
def mock_constants():
    class MockConstants:
        # Parameter file configs
        PARAMETER_FILE_NAME = "parameter.yml"

        # Parameter file validation messages
        from typing import ClassVar

        INVALID_YAML: ClassVar[dict] = {"char": "Invalid characters found", "quote": "Unclosed quote: {}"}

        INVALID_REPLACE_VALUE_SPARK_POOL: ClassVar[dict] = {
            "missing key": "The '{}' environment dict in spark_pool must contain a 'type' and a 'name' key",
            "missing value": "The '{}' environment in spark_pool is missing a value for '{}' key",
            "invalid value": "The '{}' environment in spark_pool must contain 'Capacity' or 'Workspace' as a value for 'type'",
        }

        PARAMETER_MSGS: ClassVar[dict] = {
            "validating": "Validating {}",
            "passed": "Validation passed: {}",
            "failed": "Validation failed with error: {}",
            "terminate": "Validation terminated: {}",
            "found": f"Found {PARAMETER_FILE_NAME} file",
            "not found": "Parameter file not found with path: {}",
            "invalid content": INVALID_YAML,
            "valid load": f"Successfully loaded {PARAMETER_FILE_NAME}",
            "invalid load": f"Error loading {PARAMETER_FILE_NAME} " + "{}",
            "old structure": "The parameter file structure used will no longer be supported after April 24, 2025. Please migrate to the new structure",
            "raise issue": "Raise a GitHub issue here: https://github.com/microsoft/fabric-cicd/issues for migration timeline issues",
            "invalid structure": "Invalid parameter file structure",
            "valid structure": "Parameter file structure is valid",
            "invalid name": "Invalid parameter name '{}' found in the parameter file",
            "valid name": "Parameter names are valid",
            "parameter not found": "{} parameter is not present",
            "invalid data type": "The provided '{}' is not of type {} in {}",
            "missing key": "{} is missing keys",
            "invalid key": "{} contains invalid keys",
            "valid keys": "{} contains valid keys",
            "missing required value": "Missing value for '{}' key in {}",
            "valid required values": "Required values in {} are valid",
            "missing replace value": "{} is missing a replace value for '{}' environment'",
            "valid replace value": "Values in 'replace_value' dict in {} are valid",
            "invalid replace value": INVALID_REPLACE_VALUE_SPARK_POOL,
            "no optional": "No optional values provided in {}",
            "invalid item type": "Item type '{}' not in scope",
            "invalid item name": "Item name '{}' not found in the repository directory",
            "invalid file path": "Path '{}' not found in the repository directory",
            "valid optional": "Optional values in {} are valid",
            "valid parameter": "{} parameter is valid",
            "skip": "The find value '{}' replacement will be skipped due to {} in parameter {}",
            "no target env": "target environment '{}' not found",
            "no filter match": "unmatched optional filters",
        }

    return MockConstants()


@pytest.fixture
def repository_directory(tmp_path, mock_constants):
    # Create the sample workspace structure
    workspace_dir = tmp_path / "sample" / "workspace"
    workspace_dir.mkdir(parents=True, exist_ok=True)

    # Create the sample parameter file
    parameter_file_path = workspace_dir / mock_constants.PARAMETER_FILE_NAME
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE)

    # Create the sample notebook files
    notebook_dir = workspace_dir / "Hello World.Notebook"
    notebook_dir.mkdir(parents=True, exist_ok=True)

    notebook_platform_file_path = notebook_dir / ".platform"
    notebook_platform_file_path.write_text(SAMPLE_PLATFORM_FILE)

    notebook_file_path = notebook_dir / "notebook-content.py"
    notebook_file_path.write_text(SAMPLE_NOTEBOOK_FILE)

    return workspace_dir


def test_parameter_class(repository_directory, item_type_in_scope, target_environment, mock_constants):
    """Test the Parameter class initialization."""
    parameter_file_name = mock_constants.PARAMETER_FILE_NAME
    # Initialize the Parameter class
    parameter_obj = Parameter(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        environment=target_environment,
        parameter_file_name=parameter_file_name,
    )

    # Check if the object is initialized correctly
    assert parameter_obj.repository_directory == repository_directory
    assert parameter_obj.item_type_in_scope == item_type_in_scope
    assert parameter_obj.environment == target_environment
    assert parameter_obj.parameter_file_name == parameter_file_name
    assert parameter_obj.parameter_file_path == repository_directory / parameter_file_name


@pytest.fixture
def parameter_object(repository_directory, item_type_in_scope, target_environment, mock_constants):
    """Fixture to create a Parameter object."""
    return Parameter(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        environment=target_environment,
        parameter_file_name=mock_constants.PARAMETER_FILE_NAME,
    )


def test_parameter_file_validation(parameter_object, mock_constants):
    """Test the validation methods for the parameter file"""
    # Test parameter file exists
    assert parameter_object._validate_parameter_file_exists() == (True, mock_constants.PARAMETER_MSGS["found"])

    # Test yaml content and parameter file load
    # assert parameter_object._validate_yaml_content() == []
    assert parameter_object._validate_load_parameters_to_dict() == (True, mock_constants.PARAMETER_MSGS["valid load"])

    # Test parameter file names
    assert parameter_object._validate_parameter_names() == (True, mock_constants.PARAMETER_MSGS["valid name"])

    # Test parameter file structure
    # Test for old structure

    assert parameter_object._validate_parameter_structure() == (
        True,
        mock_constants.PARAMETER_MSGS["valid structure"],
    )


@pytest.mark.parametrize(
    ("parameter_name", "expected"),
    [
        ("find_replace", True),
        ("spark_pool", True),
    ],
)
def test_parameter_validation(parameter_object, mock_constants, parameter_name, expected):
    """Test the validation methods for the specific parameters"""
    assert parameter_object._validate_parameter(parameter_name) == (
        expected,
        mock_constants.PARAMETER_MSGS["valid parameter"].format(parameter_name),
    )


@pytest.mark.parametrize(
    ("param_name", "param_value", "expected"),
    [
        ("find_replace", ["find_value", "replace_value"], True),
        ("find_replace", ["find_value", "replace_value", "item_type", "item_name", "file_path"], True),
        ("find_replace", ["find_value", "replace_value", "item_type"], True),
    ],
)
def test_parameter_keys_validation(parameter_object, mock_constants, param_name, param_value, expected):
    """Test the validation methods for the find_replace parameter"""

    assert parameter_object._validate_parameter_keys(param_name, param_value) == (
        expected,
        mock_constants.PARAMETER_MSGS["valid keys"].format(param_name),
    )


def test_parameter_required_values_validation(parameter_object, mock_constants):
    """Test the validation methods for the required values"""
    parameter_dictionary = parameter_object.environment_parameter.get("find_replace")
    for param_dict in parameter_dictionary:
        assert parameter_object._validate_required_values("find_replace", param_dict) == (
            True,
            mock_constants.PARAMETER_MSGS["valid required values"].format("find_replace"),
        )

        assert parameter_object._validate_replace_value("find_replace", param_dict["replace_value"]) == (
            True,
            mock_constants.PARAMETER_MSGS["valid replace value"].format("find_replace"),
        )
