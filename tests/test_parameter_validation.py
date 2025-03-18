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

SAMPLE_PARAMETER_FILE_OLD = """
find_replace:
    # SQL Connection Guid
    "db52be81-c2b2-4261-84fa-840c67f4bbd0":
        PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
        PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"

spark_pool:
    # CapacityPool_Large
    "72c68dbc-0775-4d59-909d-a47896f4573b":
        type: "Capacity"
        name: "CapacityPool_Large"
    # CapacityPool_Medium
    "e7b8f1c4-4a6e-4b8b-9b2e-8f1e5d6a9c3d":
        type: "Workspace"
        name: "WorkspacePool_Medium"
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

    # Create the sample parameter files
    parameter_file_path = workspace_dir / "parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE)

    parameter_file_path = workspace_dir / "invalid_parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE_INVALID)

    parameter_file_path = workspace_dir / "old_parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE_OLD)

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


@pytest.fixture
def parameter_validation_object_old(repository_directory):
    item_type_in_scope = ["DataPipeline", "Environment", "Notebook"]
    environment = "PPE"
    parameter_file = "old_parameter.yml"

    return ParameterValidation(
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
        environment=environment,
        parameter_file_name=parameter_file,
    )


@pytest.mark.parametrize(
    ("item_type", "expected_result", "expected_msg"),
    [
        ("SparkNotebook", False, "Item type: 'SparkNotebook' not in scope"),
        (["Notebook", "Pipeline"], False, "Item type: '['Notebook', 'Pipeline']' not in scope"),
        ("Notebook", True, "Item type: 'Notebook' in scope"),
        (["Notebook", "Environment"], True, "Item type: '['Notebook', 'Environment']' in scope"),
    ],
)
def test_item_type_parameter(setup_mocks, parameter_validation_object, item_type, expected_result, expected_msg):
    dl = setup_mocks
    assert parameter_validation_object._validate_item_type(item_type) == expected_result
    assert expected_msg in dl.messages


@pytest.mark.parametrize(
    ("item_name", "expected_result", "expected_msg"),
    [
        ("Hello World 2", False, "Item name: 'Hello World 2' not found in the repository directory"),
        (
            ["Hello World", "Hello World Subfolder"],
            False,
            "Item name: '['Hello World', 'Hello World Subfolder']' not found in the repository directory",
        ),
        ("Hello World", True, "Item name: 'Hello World' found in the repository directory"),
        (["Hello World"], True, "Item name: '['Hello World']' found in the repository directory"),
    ],
)
def test_item_name_parameter(setup_mocks, parameter_validation_object, item_name, expected_result, expected_msg):
    dl = setup_mocks
    assert parameter_validation_object._validate_item_name(item_name) == expected_result
    assert expected_msg in dl.messages


@pytest.mark.parametrize(
    ("file_path", "expected_result", "expected_msg"),
    [
        (
            "Hello World 2.Notebook/notebook-content.py",
            False,
            "Path: 'Hello World 2.Notebook/notebook-content.py' not found in the repository directory",
        ),
        (
            ["/Hello World.Notebook/notebook-content.py", "\\Hello World 2.Notebook\\notebook-content.py"],
            False,
            "Path: '['/Hello World.Notebook/notebook-content.py', '\\\\Hello World 2.Notebook\\\\notebook-content.py']' not found in the repository directory",
        ),
        (
            "/Hello World.Notebook/notebook-content.py",
            True,
            "Path found in the repository directory",
        ),
        (
            [
                "/Hello World.Notebook/notebook-content.py",
                "\\Hello World.Notebook\\notebook-content.py",
                "Hello World.Notebook/notebook-content.py",
                "Hello World.Notebook\\notebook-content.py",
            ],
            True,
            "Path found in the repository directory",
        ),
    ],
)
def test_file_path_parameter(setup_mocks, parameter_validation_object, file_path, expected_result, expected_msg):
    dl = setup_mocks
    assert parameter_validation_object._validate_file_path(file_path) == expected_result
    assert expected_msg in dl.messages


@pytest.mark.parametrize(
    ("replace_val_dict", "param_name", "expected_result", "expected_msg"),
    [
        (
            {"PROD": "value"},
            "find_replace",
            False,
            "Target environment 'PPE' is not a key in the 'replace_value' dict in find_replace",
        ),
        (
            {"PPE": "value"},
            "find_replace",
            True,
            "Target environment: 'PPE' is a key in the 'replace_value' dict in find_replace",
        ),
        (
            {"DEV": "value"},
            "spark_pool",
            False,
            "Target environment 'PPE' is not a key in the 'replace_value' dict in spark_pool",
        ),
        (
            {"PPE": "value"},
            "spark_pool",
            True,
            "Target environment: 'PPE' is a key in the 'replace_value' dict in spark_pool",
        ),
    ],
)
def test_target_environment(
    setup_mocks, parameter_validation_object, replace_val_dict, param_name, expected_result, expected_msg
):
    dl = setup_mocks
    assert parameter_validation_object._validate_environment(replace_val_dict, param_name) == expected_result
    assert expected_msg in dl.messages


@pytest.mark.parametrize(
    ("input_value", "expected_type", "input_name", "expected_result", "expected_msg"),
    [
        ("string_value", "dictionary", "value", False, "The provided value is not of type dictionary"),
        (
            {"string_value": None},
            "string or list[string]",
            "value",
            False,
            "The provided value is not of type string or list[string]",
        ),
        ("string_value", "string", "value", True, None),
        ("string_value", "string or list[string]", "value", True, None),
        (["string_value_1", "string_value_2"], "string or list[string]", "value", True, None),
        (
            ["string_value_1", {"key": "value"}],
            "string or list[string]",
            "value",
            False,
            "The provided value is not of type string or list[string]",
        ),
    ],
)
def test_data_type_validation(
    setup_mocks, parameter_validation_object, input_value, expected_type, input_name, expected_result, expected_msg
):
    dl = setup_mocks
    assert parameter_validation_object._validate_data_type(input_value, expected_type, input_name) == expected_result
    if expected_msg:
        assert expected_msg in dl.messages


@pytest.mark.parametrize(
    ("param_name", "param_dict", "expected_result", "expected_msg"),
    [
        (
            "find_replace",
            {"item_type": "SparkNotebook", "item_name": None, "file_path": "/Hello World.Notebook/notebook-content.py"},
            False,
            ["Item type: 'SparkNotebook' not in scope", "item_type parameter value in find_replace is invalid"],
        ),
        (
            "find_replace",
            {
                "item_type": "Notebook",
                "item_name": ["Hello World", "Hello World Subfolder"],
                "file_path": "/Hello World.Notebook/notebook-content.py",
            },
            False,
            [
                "Item type: 'Notebook' in scope",
                "Item name: '['Hello World', 'Hello World Subfolder']' not found in the repository directory",
                "item_name parameter value in find_replace is invalid",
            ],
        ),
        (
            "find_replace",
            {
                "item_type": "Notebook",
                "item_name": ["Hello World"],
                "file_path": {"/Hello World.Notebook/notebook-content.py"},
            },
            False,
            [
                "The provided file_path is not of type string or list[string]",
            ],
        ),
        (
            "find_replace",
            {"item_type": None, "item_name": None, "file_path": None},
            True,
            ["No optional parameter values in find_replace, validation passed"],
        ),
        (
            "spark_pool",
            {"item_name": "Hello World"},
            True,
            [
                "Item name: 'Hello World' found in the repository directory",
                "Optional parameter values in spark_pool are valid",
            ],
        ),
    ],
)
def test_optional_parameters(
    setup_mocks,
    parameter_validation_object,
    param_name,
    param_dict,
    expected_result,
    expected_msg,
):
    dl = setup_mocks
    assert parameter_validation_object._validate_optional_values(param_dict, param_name) == expected_result
    assert "Validating optional values" in dl.messages
    for msg in expected_msg:
        assert msg in dl.messages


@pytest.mark.parametrize(
    ("param_name", "replace_val_dict", "expected_result", "expected_msg_1", "expected_msg_2"),
    [
        (
            "find_replace",
            {"PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733", "PROD": "5d6a1b16-447f-464a-b959-45d0fed35ca0"},
            True,
            "Values in the replace_value dict in find_replace are valid",
            None,
        ),
        (
            "find_replace",
            {"PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733", "PROD": None},
            False,
            "find_replace is missing a replace_value for PROD environment",
            None,
        ),
        (
            "spark_pool",
            {
                "PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"},
                "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
            },
            True,
            "Values in replace_value dictionary are valid for spark_pool",
            None,
        ),
        (
            "spark_pool",
            {"PPE": {"type": "Capacity", "name": "value"}, "PROD": None},
            False,
            "spark_pool is missing replace_value for PROD environment",
            None,
        ),
        (
            "spark_pool",
            {"PPE": {"type": "Capacity", "name": "value"}, "PROD": ["type", "name"]},
            False,
            "'['type', 'name']' must be dictionary type",
            None,
        ),
        (
            "spark_pool",
            {"PPE": {"type_1": "Capacity", "name": "value"}},
            False,
            "'type_1' is an invalid key in PPE environment for spark_pool",
            None,
        ),
        (
            "spark_pool",
            {"PPE": {"type": "Capacity"}, "PROD": {"type": "CapacityWorkspace", "name": "value"}},
            False,
            "'CapacityWorkspace' is an invalid value for 'type' key in PROD environment for spark_pool",
            None,
        ),
        (
            "spark_pool",
            {
                "PPE": {"type": "Capacity", "name": "value"},
                "PROD": {"type": ["Capacity", "Workspace"], "name": "value"},
            },
            False,
            "'Invalid value found for 'type' key in PROD environment for spark_pool",
            "'['Capacity', 'Workspace']' must be string type",
        ),
        (
            "spark_pool",
            {"PPE": {"type": "Capacity", "name": "value"}, "PROD": {"type": "Capacity", "name": None}},
            False,
            "Value is None and must be string type",
            "'Invalid value found for 'name' key in PROD environment for spark_pool",
        ),
        (
            "spark_pool",
            {"PPE": {"type": "Capacity", "name": "value"}, "PROD": {"type": "Capacity", "name": {"key": "value"}}},
            False,
            "'{'key': 'value'}' must be string type",
            "'Invalid value found for 'name' key in PROD environment for spark_pool",
        ),
    ],
)
def test_replace_value_dict(
    setup_mocks,
    parameter_validation_object,
    param_name,
    replace_val_dict,
    expected_result,
    expected_msg_1,
    expected_msg_2,
):
    dl = setup_mocks
    assert parameter_validation_object._validate_replace_value_dict(replace_val_dict, param_name) == expected_result
    assert expected_msg_1 in dl.messages
    if expected_msg_2:
        assert expected_msg_2 in dl.messages


"""
@pytest.mark.parametrize(
    ("param_name", "param_dict", "expected_result", "expected_msg"),
    [
        (
            "find_replace",
            {
                "find_value": "db52be81-c2b2-4261-84fa-840c67f4bbd0",
                "replace_value": {
                    "PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733",
                    "PROD": "5d6a1b16-447f-464a-b959-45d0fed35ca0",
                },
            },
            True,
            "Required values are present in find_replace and are of valid data types",
        ),
        (
            "find_replace",
            {
                "find_value": None,
                "replace_value": {
                    "PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733",
                    "PROD": "5d6a1b16-447f-464a-b959-45d0fed35ca0",
                },
            },
            False,
            "Missing value for 'find_value' key in find_replace",
        ),
        (
            "find_replace",
            {
                "find_value": "db52be81-c2b2-4261-84fa-840c67f4bbd0",
                "replace_value": None,
            },
            False,
            "Missing value for 'replace_value' key in find_replace",
        ),
        (
            "spark_pool",
            {
                "instance_pool_id": "72c68dbc-0775-4d59-909d-a47896f4573b",
                "replace_value": {
                    "PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"},
                    "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
                },
            },
            True,
            "Required values are present in spark_pool and are of valid data types",
        ),
        (
            "spark_pool",
            {
                "instance_pool_id": None,
                "replace_value": {
                    "PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"},
                    "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
                },
            },
            False,
            "Missing value for 'instance_pool_id' key in spark_pool",
        ),
        (
            "spark_pool",
            {
                "instance_pool_id": "72c68dbc-0775-4d59-909d-a47896f4573b",
                "replace_value": None,
            },
            False,
            "Missing value for 'replace_value' key in spark_pool",
        ),
    ],
)
def test_required_values(
    setup_mocks, parameter_validation_object, param_name, param_dict, expected_result, expected_msg
):
    dl = setup_mocks
    assert parameter_validation_object._validate_required_values(param_dict, param_name) == expected_result
    assert "Validating required values" in dl.messages
    assert expected_msg in dl.messages


@pytest.mark.parametrize(
    ("param_name", "param_keys", "expected_result", "expected_msg"),
    [
        ("find_replace", ("find_value", "replace_value"), True, "find_replace contains valid keys"),
        ("find_replace", ("find_value"), False, "find_replace is missing required keys"),
        ("find_replace", ("find_value_new", "replace_value"), False, "find_replace is missing required keys"),
        ("spark_pool", ("instance_pool_id", "replace_value"), True, "spark_pool contains valid keys"),
        ("spark_pool", ("replace_value"), False, "spark_pool is missing required keys"),
        ("spark_pool", ("pool_id", "replace_value_1"), False, "spark_pool is missing required keys"),
        (
            "find_replace",
            ("find_value", "replace_value", "item_type", "item_name", "file_path"),
            True,
            "find_replace contains valid keys",
        ),
        ("find_replace", ("find_value", "replace_value", "file_path"), True, "find_replace contains valid keys"),
        (
            "find_replace",
            ("find_value", "replace_value", "item_type", "item_name", "file_path", "file_regex"),
            False,
            "find_replace contains invalid keys",
        ),
        ("spark_pool", ("instance_pool_id", "replace_value", "item_name"), True, "spark_pool contains valid keys"),
        (
            "spark_pool",
            ("instance_pool_id", "replace_value", "item_name", "file_path"),
            False,
            "spark_pool contains invalid keys",
        ),
        ("spark_pool", ("instance_pool_id", "replace_value", "item_type"), False, "spark_pool contains invalid keys"),
    ],
)
def test_parameter_keys(
    setup_mocks, parameter_validation_object, param_name, param_keys, expected_result, expected_msg
):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter_keys(param_name, param_keys) == expected_result
    assert f"Validating {param_name} keys" in dl.messages
    assert expected_msg in dl.messages


@pytest.mark.parametrize(("param_name"), [("find_replace"), ("spark_pool")])
def test_parameter_validation(setup_mocks, parameter_validation_object, param_name):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter(param_name) is True
    assert f"Validating {param_name} parameter" in dl.messages
    assert "Validating replace_value dictionary keys and values" in dl.messages
    assert f"{param_name} parameter validation passed" in dl.messages


@pytest.mark.parametrize(
    ("struc_type", "expected_result", "expected_msg"),
    [
        ("new", True, "Parameter file structure is valid"),
        ("old", False, "Validation skipped for old parameter file structure"),
        ("invalid", False, "Validation failed for invalid parameter file structure"),
    ],
)
def test_parameter_structure(
    setup_mocks,
    parameter_validation_object,
    parameter_validation_object_old,
    parameter_validation_object_invalid,
    struc_type,
    expected_result,
    expected_msg,
):
    dl = setup_mocks
    if struc_type == "new":
        assert parameter_validation_object._validate_parameter_structure() == expected_result
    if struc_type == "old":
        assert parameter_validation_object_old._validate_parameter_structure() == expected_result
    if struc_type == "invalid":
        assert parameter_validation_object_invalid._validate_parameter_structure() == expected_result

    assert "Validating parameter structure" in dl.messages
    assert expected_msg in dl.messages


def test_parameter_names(setup_mocks, parameter_validation_object, parameter_validation_object_invalid):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter_names() is True
    assert "Validating parameter names" in dl.messages
    assert parameter_validation_object_invalid._validate_parameter_names() is False
    assert "Invalid parameter name: 'find_replace_invalid' found in the parameter file" in dl.messages


def test_parameter_file_load(setup_mocks, parameter_validation_object):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter_file_load() is True
    assert "Validating parameter file load" in dl.messages
    assert "Parameter file load validation passed" in dl.messages


def test_parameter_file_validation(setup_mocks, parameter_validation_object, parameter_validation_object_invalid):
    dl = setup_mocks
    assert parameter_validation_object._validate_parameter_file() is True
    assert "Validation passed for find_replace and spark_pool parameters" in dl.messages
    assert parameter_validation_object_invalid._validate_parameter_file() is False
    assert "Parameter file validation failed" in dl.messages
"""
