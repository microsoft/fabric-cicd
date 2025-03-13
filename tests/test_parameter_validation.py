# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pytest

from fabric_cicd._parameterization import ParameterValidation

SAMPLE_PARAMETER_FILE = """ 
find_replace:
    # Required Fields 
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World", "Hello World Subfolder"]
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

SAMPLE_PARAMETER_FILE_INVALID = """ 
find_replace_invalid:
    # Required Fields 
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World", "Hello World Subfolder"]
      file_path: "/Hello World.Notebook/notebook-content.py"
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
def repository_directory(tmp_path):
    # Create the sample workspace structure
    workspace_dir = tmp_path / "sample" / "workspace"
    workspace_dir.mkdir(parents=True, exist_ok=True)

    # Create the sample parameter file
    parameter_file_path = workspace_dir / "parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE)

    # Create the sample parameter file
    parameter_file_path = workspace_dir / "invalid_parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE_INVALID)

    # Create the sample notebook files
    notebook_dir = workspace_dir / "Hello World.Notebook"
    notebook_dir.mkdir(parents=True, exist_ok=True)

    notebook_platform_file_path = notebook_dir / ".platform"
    notebook_platform_file_path.write_text(SAMPLE_PLATFORM_FILE)

    notebook_file_path = notebook_dir / "notebook-content.py"
    notebook_file_path.write_text(SAMPLE_NOTEBOOK_FILE)

    return workspace_dir


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

    def debug(self, message):
        self.messages.append(message)

    def error(self, message):
        self.messages.append(message)

    def warning(self, message):
        self.messages.append(message)


@pytest.fixture
def setup_mocks(monkeypatch, mocker):
    dl = DummyLogger()
    mock_logger = mocker.Mock()
    mock_logger.isEnabledFor.return_value = True
    mock_logger.info.side_effect = dl.info
    mock_logger.debug.side_effect = dl.debug
    mock_logger.error.side_effect = dl.error
    mock_logger.warning.side_effect = dl.warning
    monkeypatch.setattr("fabric_cicd._parameterization._parameter_validation.logger", mock_logger)

    return dl


@pytest.fixture
def parameter_validation_object(repository_directory):
    item_type_in_scope = ["DataPipeline", "Environment", "Notebook"]
    environment = "PPE"
    parameter_file = "parameter.yml"

    return ParameterValidation(
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
        environment=environment,
        parameter_file_name=parameter_file,
    )


@pytest.fixture
def parameter_validation_object_invalid(repository_directory):
    item_type_in_scope = ["DataPipeline", "Environment", "Notebook"]
    environment = "PPE"
    parameter_file = "invalid_parameter.yml"

    return ParameterValidation(
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
        environment=environment,
        parameter_file_name=parameter_file,
    )


def test_item_type_parameter(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert parameter_validation_object._validate_item_type("SparkNotebook") is False
    assert "Item type 'SparkNotebook' is not in scope" in dl.messages

    assert parameter_validation_object._validate_item_type(["Notebook", "Pipeline"]) is False
    assert "Item type 'Pipeline' is not in scope" in dl.messages

    assert parameter_validation_object._validate_item_type("Notebook") is True
    assert "Item type 'Notebook' is in scope" in dl.messages

    assert parameter_validation_object._validate_item_type(["Notebook", "Environment"]) is True
    assert "Item types are in scope" in dl.messages


def test_item_name_parameter(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert parameter_validation_object._validate_item_name("Hello World 2") is False
    assert "Item name 'Hello World 2' is not found in the repository directory" in dl.messages

    assert parameter_validation_object._validate_item_name(["Hello World", "Hello World Subfolder"]) is False
    assert "Item name 'Hello World Subfolder' is not found in the repository directory" in dl.messages

    assert parameter_validation_object._validate_item_name("Hello World") is True
    assert "Item name 'Hello World' is found in the repository directory" in dl.messages

    assert parameter_validation_object._validate_item_name(["Hello World"]) is True
    assert "Item names are found in the repository directory" in dl.messages


def test_file_path_parameter(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert parameter_validation_object._validate_file_path("Hello World 2.Notebook/notebook-content.py") is False
    assert "Path 'Hello World 2.Notebook/notebook-content.py' is not found in the repository directory" in dl.messages

    assert parameter_validation_object._validate_file_path("/Hello World.Notebook/notebook-content.py") is True
    assert "Path '/Hello World.Notebook/notebook-content.py' is found in the repository directory" in dl.messages

    assert (
        parameter_validation_object._validate_file_path([
            "/Hello World.Notebook/notebook-content.py",
            "\\Hello World.Notebook\\notebook-content.py",
            "Hello World.Notebook/notebook-content.py",
            "Hello World.Notebook\\notebook-content.py",
        ])
        is True
    )
    assert "All paths are found in the repository directory" in dl.messages


def test_target_environment(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    target_environment = parameter_validation_object.environment
    param_name = "find_replace"

    assert parameter_validation_object._validate_environment({"PROD": "value"}, param_name) is False
    assert f"Target environment '{target_environment}' is not a key in 'replace_value' for {param_name}" in dl.messages

    assert parameter_validation_object._validate_environment({"PPE": "value"}, param_name) is True
    assert f"Target environment: '{target_environment}' is a key in 'replace_value' for {param_name}" in dl.messages


def test_data_type_validation(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert parameter_validation_object._validate_data_type("sample_value", "dictionary") is False
    assert "'sample_value' is of type <class 'str'> and must be dictionary type" in dl.messages
    assert parameter_validation_object._validate_data_type("sample_value", "list") is False
    assert "'sample_value' is of type <class 'str'> and must be list type" in dl.messages
    assert parameter_validation_object._validate_data_type("find_replace", "string") is True
    assert parameter_validation_object._validate_data_type("find_replace", "string or list") is True
    assert (
        parameter_validation_object._validate_data_type(["sample_value_1", "sample_value_2"], "string or list") is True
    )


def test_optional_parameters(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    param_dict_find_replace = parameter_validation_object.environment_parameter["find_replace"][0]
    assert parameter_validation_object._validate_optional_values(param_dict_find_replace, "find_replace") is False

    param_dict_spark_pool = parameter_validation_object.environment_parameter["spark_pool"][0]
    assert parameter_validation_object._validate_optional_values(param_dict_spark_pool, "spark_pool") is True

    assert "Validating optional values" in dl.messages
    assert "item_name value is invalid" in dl.messages
    assert "Optional parameter values are valid for spark_pool" in dl.messages


def test_replace_value_find_replace(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    replace_value_dict_invalid = {"PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733", "PROD": None}
    replace_value_dict_valid = parameter_validation_object.environment_parameter["find_replace"][0]["replace_value"]

    assert parameter_validation_object._validate_find_replace_replace_value(replace_value_dict_invalid) is False
    assert "find_replace is missing a replace_value for PROD environment" in dl.messages

    assert (
        parameter_validation_object._validate_replace_value_dict_values(replace_value_dict_valid, "find_replace")
        is True
    )
    assert "Values in replace_value dictionary are valid for find_replace" in dl.messages


def test_replace_value_spark_pool(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    valid_dict = parameter_validation_object.environment_parameter["spark_pool"][0]["replace_value"]
    invalid_dict_1 = {"PPE": {"type": "Capacity", "name": "value"}, "PROD": None}
    invalid_dict_2 = {"PPE": {"type": "Capacity", "name": "value"}, "PROD": ["type", "name"]}
    invalid_dict_3 = {"PPE": {"type_1": "Capacity", "name": "value"}}
    invalid_dict_4 = {"PPE": {"type": "Capacity"}, "PROD": {"type": "CapacityWorkspace", "name": "value"}}
    invalid_dict_5 = {
        "PPE": {"type": "Capacity", "name": "value"},
        "PROD": {"type": ["Capacity", "Workspace"], "name": "value"},
    }
    invalid_dict_6 = {"PPE": {"type": "Capacity", "name": "value"}, "PROD": {"type": "Capacity", "name": None}}
    invalid_dict_7 = {
        "PPE": {"type": "Capacity", "name": "value"},
        "PROD": {"type": "Capacity", "name": {"key": "value"}},
    }

    assert parameter_validation_object._validate_replace_value_dict_values(valid_dict, "spark_pool") is True
    assert parameter_validation_object._validate_replace_value_dict_values(invalid_dict_1, "spark_pool") is False
    assert parameter_validation_object._validate_replace_value_dict_values(invalid_dict_2, "spark_pool") is False
    assert parameter_validation_object._validate_replace_value_dict_values(invalid_dict_3, "spark_pool") is False
    assert parameter_validation_object._validate_replace_value_dict_values(invalid_dict_4, "spark_pool") is False
    assert parameter_validation_object._validate_replace_value_dict_values(invalid_dict_5, "spark_pool") is False
    assert parameter_validation_object._validate_replace_value_dict_values(invalid_dict_6, "spark_pool") is False
    assert parameter_validation_object._validate_replace_value_dict_values(invalid_dict_7, "spark_pool") is False

    expected_messages = [
        "Values in replace_value dictionary are valid for spark_pool",
        "spark_pool is missing replace_value for PROD environment",
        "'['type', 'name']' is of type <class 'list'> and must be dictionary type",
        "'type_1' is an invalid key in PPE environment for spark_pool",
        "'CapacityWorkspace' is an invalid value for 'type' key in PROD environment for spark_pool",
        "'['Capacity', 'Workspace']' is an invalid value for 'type' key in PROD environment for spark_pool",
        "'None' is of type <class 'NoneType'> and must be string type",
        "'Invalid value found for 'name' key in PROD environment in spark_pool",
        "'{'key': 'value'}' is of type <class 'dict'> and must be string type",
        "'Invalid value found for 'name' key in PROD environment in spark_pool",
    ]

    for message in expected_messages:
        assert message in dl.messages


def test_required_values(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    param_dict = parameter_validation_object.environment_parameter["find_replace"][0]
    assert parameter_validation_object._validate_required_values(param_dict, "find_replace") is True
    assert "Validating required values" in dl.messages
    assert "Required values are present in find_replace and are of valid data types" in dl.messages

    invalid_param_dict_1 = {
        "find_value": None,
        "replace_value": {
            "PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733",
            "PROD": "5d6a1b16-447f-464a-b959-45d0fed35ca0",
        },
        "item_type": "Notebook",
        "item_name": None,
        "file_path": None,
    }

    invalid_param_dict_2 = {
        "find_value": "db52be81-c2b2-4261-84fa-840c67f4bbd0",
        "replace_value": None,
        "item_type": "Notebook",
        "item_name": None,
        "file_path": None,
    }

    assert parameter_validation_object._validate_required_values(invalid_param_dict_1, "find_replace") is False
    assert parameter_validation_object._validate_required_values(invalid_param_dict_2, "find_replace") is False
    assert "Missing value for 'find_value' key in find_replace" in dl.messages
    assert "Missing value for 'replace_value' key in find_replace" in dl.messages


def test_minimum_parameter_keys(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter_keys("find_replace", ("find_value", "replace_value")) is True
    assert parameter_validation_object._validate_parameter_keys("find_replace", ("find_value")) is False
    assert (
        parameter_validation_object._validate_parameter_keys("find_replace", ("find_value_new", "replace_value"))
        is False
    )
    assert "Validating find_replace keys" in dl.messages
    assert "find_replace contains valid keys" in dl.messages
    assert "find_replace is missing required keys" in dl.messages


def test_maximum_parameter_keys(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace", ("find_value", "replace_value", "item_type", "item_name", "file_path")
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace", ("find_value", "replace_value", "file_path")
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace", ("find_value", "replace_value", "item_type", "item_name", "file_path", "file_regex")
        )
        is False
    )
    assert "Validating find_replace keys" in dl.messages
    assert "find_replace contains invalid keys" in dl.messages
    assert "find_replace contains valid keys" in dl.messages


def test_parameter_validation(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter("find_replace") is True
    assert "Validating find_replace parameter" in dl.messages
    assert "Validating replace_value dictionary keys and values" in dl.messages
    assert "find_replace parameter validation passed" in dl.messages


def test_parameter_name_validation(setup_mocks, parameter_validation_object, parameter_validation_object_invalid):
    dl = setup_mocks
    assert parameter_validation_object_invalid._validate_parameter_names() is False
    assert parameter_validation_object._validate_parameter_names() is True
    assert "Validating parameter names" in dl.messages
    assert "Invalid parameter 'find_replace_invalid' in the parameter file" in dl.messages


def test_parameter_file_load(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter_file_load() is True
    assert "Validating parameter file load" in dl.messages
    assert "Parameter file load validation passed" in dl.messages


def test_parameter_file_validation(setup_mocks, parameter_validation_object, parameter_validation_object_invalid):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter_file() is True
    assert parameter_validation_object_invalid._validate_parameter_file() is False
    assert "Parameter file validation passed" in dl.messages
    assert "Parameter file validation failed" in dl.messages
