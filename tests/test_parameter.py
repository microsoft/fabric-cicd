# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from pathlib import Path

import pytest
import yaml

import fabric_cicd.constants as constants
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

SAMPLE_PARAMETER_FILE_MULTIPLE = """ 
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
    # Required Fields 
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World"] 
      file_path: "/Hello World.Notebook/notebook-content.py"
key_value_replace:
    - find_key: $.variables[?(@.name=="SQL_Server")].value
      replace_value:
        PPE: "contoso-ppe.database.windows.net"
        PROD: "contoso-prod.database.windows.net"
        UAT: "contoso-uat.database.windows.net"
      # Optional fields:
      item_type: "VariableLibrary"
      item_name: "Vars"
    - find_key: $.variables[?(@.name=="Environment")].value
      replace_value:
        PPE: "PPE"
        PROD: "PROD"
        UAT: "UAT"
      # Optional fields:
      item_type: "VariableLibrary"
      item_name: "Vars"
    - find_key: $.variableOverrides[?(@.name=="SQL_Server")].value
      replace_value:
        PROD: "contoso-production-override.database.windows.net"
      file_path: Vars.VariableLibrary/valueSets/PROD.json
      item_type: "VariableLibrary"
      item_name: "Vars"
    - find_key: $.variableOverrides[?(@.name=="Environment")].value
      replace_value:
        PROD: "PROD_ENV"
      file_path: Vars.VariableLibrary/valueSets/PROD.json
      item_type: "VariableLibrary"
      item_name: "Vars"
"""

SAMPLE_INVALID_PARAMETER_FILE = """
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
    # CapacityPool_Large
    "72c68dbc-0775-4d59-909d-a47896f4573b":
        type: "Capacity"
        name: "CapacityPool_Large"
    # CapacityPool_Medium
    "e7b8f1c4-4a6e-4b8b-9b2e-8f1e5d6a9c3d":
        type: "Workspace"
        name: "WorkspacePool_Medium"
"""

SAMPLE_PARAMETER_NO_TARGET_ENV = """ 
find_replace:
    # Required Fields 
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          DEV: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World"] 
      file_path: "/Hello World.Notebook/notebook-content.py"
"""

SAMPLE_PARAMETER_MISSING_FIND_VAL = """ 
find_replace:
    # Required Fields 
    - find_value: 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World"] 
      file_path: "/Hello World.Notebook/notebook-content.py"
"""

SAMPLE_PARAMETER_MISMATCH_FILTER = """ 
find_replace:
    # Required Fields 
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World", 'Hello World Subfolder'] 
      file_path: "/Hello World.Notebook/notebook-content.py"
"""

SAMPLE_PARAMETER_MISSING_REPLACE_VAL = """
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
      # Optional Fields
      item_name:
"""

SAMPLE_PARAMETER_INVALID_NAME = """
spark_pool_param:
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

SAMPLE_PARAMETER_INVALID_YAML_STRUC = """
spark_pool:
    # Required Fields
    instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
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

SAMPLE_PARAMETER_INVALID_YAML_CHAR = """
find_replace:
    # Required Fields 
    - find_value: '"db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World"] 
      file_path: "/Hello World.Notebook/notebook-content.py"
"""

SAMPLE_PARAMETER_INVALID_IS_REGEX = """
find_replace:
    # Required Fields
    - find_value: \#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      is_regex: True
      item_type: "Notebook"
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
def repository_directory(tmp_path):
    # Create the sample workspace structure
    workspace_dir = tmp_path / "sample" / "workspace"
    workspace_dir.mkdir(parents=True, exist_ok=True)

    # Create the sample parameter file
    parameter_file_path = workspace_dir / constants.PARAMETER_FILE_NAME
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE)

    # Create sample invalid parameter files
    invalid_parameter_file_path = workspace_dir / "invalid_parameter.yml"
    invalid_parameter_file_path.write_text(SAMPLE_INVALID_PARAMETER_FILE)

    invalid_parameter_file_path1 = workspace_dir / "no_target_env_parameter.yml"
    invalid_parameter_file_path1.write_text(SAMPLE_PARAMETER_NO_TARGET_ENV)

    invalid_parameter_file_path2 = workspace_dir / "missing_find_val_parameter.yml"
    invalid_parameter_file_path2.write_text(SAMPLE_PARAMETER_MISSING_FIND_VAL)

    invalid_parameter_file_path3 = workspace_dir / "mismatch_filter_parameter.yml"
    invalid_parameter_file_path3.write_text(SAMPLE_PARAMETER_MISMATCH_FILTER)

    invalid_parameter_file_path4 = workspace_dir / "missing_replace_val_parameter.yml"
    invalid_parameter_file_path4.write_text(SAMPLE_PARAMETER_MISSING_REPLACE_VAL)

    invalid_parameter_file_path5 = workspace_dir / "invalid_name_parameter.yml"
    invalid_parameter_file_path5.write_text(SAMPLE_PARAMETER_INVALID_NAME)

    invalid_parameter_file_path6 = workspace_dir / "invalid_yaml_struc_parameter.yml"
    invalid_parameter_file_path6.write_text(SAMPLE_PARAMETER_INVALID_YAML_STRUC)

    invalid_parameter_file_path7 = workspace_dir / "invalid_yaml_char_parameter.yml"
    invalid_parameter_file_path7.write_text(SAMPLE_PARAMETER_INVALID_YAML_CHAR)

    invalid_parameter_file_path8 = workspace_dir / "invalid_is_regex_parameter.yml"
    invalid_parameter_file_path8.write_text(SAMPLE_PARAMETER_INVALID_IS_REGEX)

    # Create the sample parameter file with multiple of a parameter
    multiple_parameter_file_path = workspace_dir / "multiple_parameter.yml"
    multiple_parameter_file_path.write_text(SAMPLE_PARAMETER_FILE_MULTIPLE)

    # Create the sample notebook files
    notebook_dir = workspace_dir / "Hello World.Notebook"
    notebook_dir.mkdir(parents=True, exist_ok=True)

    notebook_platform_file_path = notebook_dir / ".platform"
    notebook_platform_file_path.write_text(SAMPLE_PLATFORM_FILE)

    notebook_file_path = notebook_dir / "notebook-content.py"
    notebook_file_path.write_text(SAMPLE_NOTEBOOK_FILE)

    return workspace_dir


@pytest.fixture
def parameter_object(repository_directory, item_type_in_scope, target_environment):
    """Fixture to create a Parameter object."""
    return Parameter(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        environment=target_environment,
        parameter_file_name=constants.PARAMETER_FILE_NAME,
    )


def test_parameter_class_initialization(parameter_object, repository_directory, item_type_in_scope, target_environment):
    """Test the Parameter class initialization."""
    parameter_file_name = constants.PARAMETER_FILE_NAME

    # Check if the object is initialized correctly
    assert parameter_object.repository_directory == repository_directory
    assert parameter_object.item_type_in_scope == item_type_in_scope
    assert parameter_object.environment == target_environment
    assert parameter_object.parameter_file_name == parameter_file_name
    assert parameter_object.parameter_file_path == repository_directory / parameter_file_name


def test_parameter_file_validation(parameter_object):
    """Test the validation methods for the parameter file"""
    assert parameter_object._validate_parameter_file_exists() == True
    assert parameter_object._validate_load_parameters_to_dict() == (True, parameter_object.environment_parameter)
    assert parameter_object._validate_parameter_load() == (True, constants.PARAMETER_MSGS["valid load"])
    assert parameter_object._validate_parameter_names() == (True, constants.PARAMETER_MSGS["valid name"])
    assert parameter_object._validate_parameter_structure() == (True, constants.PARAMETER_MSGS["valid structure"])
    assert parameter_object._validate_parameter("find_replace") == (
        True,
        constants.PARAMETER_MSGS["valid parameter"].format("find_replace"),
    )
    assert parameter_object._validate_parameter("spark_pool") == (
        True,
        constants.PARAMETER_MSGS["valid parameter"].format("spark_pool"),
    )
    assert parameter_object._validate_parameter_file() == True


def test_multiple_parameter_validation(repository_directory, item_type_in_scope, target_environment):
    """Test the validation methods for multiple parameters case"""
    multi_param_obj = Parameter(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        environment=target_environment,
        parameter_file_name="multiple_parameter.yml",
    )
    assert multi_param_obj._validate_parameter("find_replace") == (
        True,
        constants.PARAMETER_MSGS["valid parameter"].format("find_replace"),
    )
    assert multi_param_obj._validate_parameter("key_value_replace") == (
        True,
        constants.PARAMETER_MSGS["valid parameter"].format("key_value_replace"),
    )
    assert multi_param_obj._validate_parameter_file() == True


@pytest.mark.parametrize(
    ("param_name", "param_value", "result", "msg"),
    [
        ("find_replace", ["find_value", "replace_value"], True, "valid keys"),
        ("find_replace", ["find_value", "item_type", "item_name", "file_path"], False, "missing key"),
        ("find_replace", ["find_value", "replace_value", "is_regex", "item_type"], True, "valid keys"),
        ("spark_pool", ["instance_pool_id", "replace_value", "item_name"], True, "valid keys"),
        ("spark_pool", ["instance_pool_id", "replace_value", "item_name", "file_path"], False, "invalid key"),
    ],
)
def test_validate_parameter_keys(parameter_object, param_name, param_value, result, msg):
    """Test the validation methods for the find_replace parameter"""

    assert parameter_object._validate_parameter_keys(param_name, param_value) == (
        result,
        constants.PARAMETER_MSGS[msg].format(param_name),
    )


@pytest.mark.parametrize(("param_name"), [("find_replace"), ("spark_pool")])
def test_validate_parameter(parameter_object, param_name):
    """Test the validation methods for a specific parameter"""
    param_dict = parameter_object.environment_parameter.get(param_name)
    for param in param_dict:
        assert parameter_object._validate_required_values(param_name, param) == (
            True,
            constants.PARAMETER_MSGS["valid required values"].format(param_name),
        )
        assert parameter_object._validate_replace_value(param_name, param["replace_value"]) == (
            True,
            constants.PARAMETER_MSGS["valid replace value"].format(param_name),
        )


@pytest.mark.parametrize(
    ("replace_value", "result", "msg"),
    [
        (
            {"PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733", "PROD": "5d6a1b16-447f-464a-b959-45d0fed35ca0"},
            True,
            "valid replace value",
        ),
        (
            {"PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733", "PROD": None},
            False,
            "missing replace value",
        ),
    ],
)
def test_validate_find_replace_replace_value(parameter_object, replace_value, result, msg):
    """Test the _validate_find_replace_replace_value method."""
    assert parameter_object._validate_find_replace_replace_value(replace_value) == (
        result,
        constants.PARAMETER_MSGS[msg].format("find_replace", "PROD")
        if msg == "missing replace value"
        else constants.PARAMETER_MSGS[msg].format("find_replace"),
    )


@pytest.mark.parametrize(
    ("replace_value", "result", "msg", "desc"),
    [
        (
            {
                "PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"},
                "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
            },
            True,
            "valid replace value",
            None,
        ),
        (
            {
                "PPE": {},
                "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
            },
            False,
            "missing replace value",
            None,
        ),
        (
            {
                "PPE": {"name": "CapacityPool_Large_PPE"},
                "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
            },
            False,
            "invalid replace value",
            "missing key",
        ),
        (
            {
                "PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"},
                "PROD": {"type": "Capacity", "name": None},
            },
            False,
            "invalid replace value",
            "missing value",
        ),
        (
            {
                "PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"},
                "PROD": {"type": "Test", "name": "CapacityPool_Large_PROD"},
            },
            False,
            "invalid replace value",
            "invalid value",
        ),
    ],
)
def test_validate_spark_pool_replace_value(parameter_object, replace_value, result, msg, desc):
    """Test the _validate_spark_pool_replace_value method."""
    if msg == "valid replace value":
        msg = constants.PARAMETER_MSGS[msg].format("spark_pool")
    if msg == "missing replace value":
        msg = constants.PARAMETER_MSGS[msg].format("spark_pool", "PPE")
    if msg == "invalid replace value" and desc == "missing key":
        msg = constants.PARAMETER_MSGS[msg][desc].format("PPE")
    if msg == "invalid replace value" and desc == "missing value":
        msg = constants.PARAMETER_MSGS[msg][desc].format("PROD", "name")
    if msg == "invalid replace value" and desc == "invalid value":
        msg = constants.PARAMETER_MSGS[msg][desc].format("PROD")

    assert parameter_object._validate_spark_pool_replace_value(replace_value) == (result, msg)
    assert parameter_object._validate_replace_value(
        "spark_pool",
        {
            "PPE": {},
            "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
        },
    ) == (False, constants.PARAMETER_MSGS["missing replace value"].format("spark_pool", "PPE"))


def test_validate_data_type(parameter_object):
    """Test data type validation"""
    # General data type validation
    assert parameter_object._validate_data_type([1, 2, 3], "string or list[string]", "key", "param_name") == (
        False,
        constants.PARAMETER_MSGS["invalid data type"].format("key", "string or list[string]", "param_name"),
    )

    required_values = {
        "find_value": ["db52be81-c2b2-4261-84fa-840c67f4bbd0"],
        "replace_value": {
            "PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733",
            "PROD": "5d6a1b16-447f-464a-b959-45d0fed35ca0",
        },
    }
    # Data type error in required values
    assert parameter_object._validate_required_values("find_replace", required_values) == (
        False,
        constants.PARAMETER_MSGS["invalid data type"].format("find_value", "string", "find_replace"),
    )

    find_replace_value = {
        "PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733",
        "PROD": 123,
    }
    # Data type error in find_replace replace value dict
    assert parameter_object._validate_find_replace_replace_value(find_replace_value) == (
        False,
        constants.PARAMETER_MSGS["invalid data type"].format("PROD replace_value", "string", "find_replace"),
    )

    spark_pool_replace_value_1 = {
        "PPE": "string",
        "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
    }
    # Data type error in spark_pool replace value dict
    assert parameter_object._validate_spark_pool_replace_value(spark_pool_replace_value_1) == (
        False,
        constants.PARAMETER_MSGS["invalid data type"].format("PPE key", "dictionary", "spark_pool"),
    )

    spark_pool_replace_value_2 = {
        "PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"},
        "PROD": {"type": ["Capacity"], "name": "CapacityPool_Large_PROD"},
    }
    # Data type error in spark_pool replace value environment dict
    assert parameter_object._validate_spark_pool_replace_value(spark_pool_replace_value_2) == (
        False,
        constants.PARAMETER_MSGS["invalid data type"].format("type", "string", "spark_pool"),
    )

    param_dict = {
        "item_type": "Notebook",
        "item_name": {"Hello World"},
        "file_path": "/Hello World.Notebook/notebook-content.py",
    }
    # Data type error in optional values
    assert parameter_object._validate_optional_values("find_replace", param_dict) == (
        False,
        constants.PARAMETER_MSGS["invalid data type"].format("item_name", "string or list[string]", "find_replace"),
    )


def test_validate_yaml_content(parameter_object):
    """Test the validation of the YAML content"""
    invalid_content = "\n\n\n\t"
    assert parameter_object._validate_yaml_content(invalid_content) == ["YAML content is empty"]

    invalid_content = "\U0001f600"
    assert parameter_object._validate_yaml_content(invalid_content) == [
        constants.PARAMETER_MSGS["invalid content"]["char"]
    ]


def test_validate_parameter_file_structure(repository_directory, item_type_in_scope, target_environment):
    """Test the validation of the parameter file structure"""
    param_obj = Parameter(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        environment=target_environment,
        parameter_file_name="invalid_parameter.yml",
    )
    assert param_obj._validate_parameter_structure() == (False, constants.PARAMETER_MSGS["invalid structure"])


def test_validate_optional_values(parameter_object):
    """Test the _validate_optional_values method."""
    param_dict_1 = {
        "item_type": "Notebook",
        "item_name": ["Hello World"],
        "file_path": "/Hello World.Notebook/notebook-content.py",
    }
    assert parameter_object._validate_optional_values("find_replace", param_dict_1) == (
        True,
        constants.PARAMETER_MSGS["valid optional"].format("find_replace"),
    )

    param_dict_2 = {
        "item_type": "SparkNotebook",
        "item_name": ["Hello World"],
        "file_path": "/Hello World.Notebook/notebook-content.py",
    }
    assert parameter_object._validate_optional_values("find_replace", param_dict_2, check_match=True) == (
        False,
        "no match",
    )

    param_dict_3 = {"item_name": "Hello World"}
    assert parameter_object._validate_optional_values("spark_pool", param_dict_3, check_match=True) == (
        True,
        constants.PARAMETER_MSGS["valid optional"].format("spark_pool"),
    )


@pytest.mark.parametrize(
    ("param_name"),
    ["find_replace", "spark_pool"],
)
def test_validate_parameter_environment_and_filters(parameter_object, param_name):
    """Test the validation methods for environment and filters"""
    for param_dict in parameter_object.environment_parameter.get(param_name):
        # Environment validation
        assert parameter_object._validate_environment(param_dict["replace_value"]) == True

    # Optional filters validation
    assert parameter_object._validate_item_type("Pipeline") == (
        False,
        constants.PARAMETER_MSGS["invalid item type"].format("Pipeline"),
    )
    assert parameter_object._validate_item_name("Hello World 2") == (
        False,
        constants.PARAMETER_MSGS["invalid item name"].format("Hello World 2"),
    )
    assert parameter_object._validate_file_path("Hello World 2.Notebook/notebook-content.py") == (
        False,
        constants.PARAMETER_MSGS["invalid file path"].format("Hello World 2.Notebook/notebook-content.py"),
    )


@pytest.mark.parametrize(
    ("param_file_name", "result", "msg"),
    [
        ("no_target_env_parameter.yml", True, "valid parameter"),
        ("missing_find_val_parameter.yml", False, "missing required value"),
        ("mismatch_filter_parameter.yml", True, "valid parameter"),
        ("missing_replace_val_parameter.yml", False, "missing required value"),
        ("invalid_name_parameter.yml", False, "invalid name"),
        ("invalid_yaml_struc_parameter.yml", False, "invalid load"),
        ("invalid_yaml_char_parameter.yml", False, "invalid load"),
        ("invalid_is_regex_parameter.yml", False, "invalid data type"),
    ],
)
def test_validate_invalid_parameters(
    repository_directory, item_type_in_scope, target_environment, param_file_name, result, msg
):
    """Test the validation of invalid or error-prone parameter files"""
    param_obj = Parameter(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        environment=target_environment,
        parameter_file_name=param_file_name,
    )

    # Target environment not present in find_replace parameter (error-prone case)
    if param_file_name == "no_target_env_parameter.yml":
        assert param_obj._validate_parameter("find_replace") == (
            result,
            constants.PARAMETER_MSGS[msg].format("find_replace"),
        )

    # Missing required value in find_replace parameter
    if param_file_name == "missing_find_val_parameter.yml":
        for param_dict in param_obj.environment_parameter.get("find_replace"):
            assert param_obj._validate_required_values("find_replace", param_dict) == (
                result,
                constants.PARAMETER_MSGS[msg].format("find_value", "find_replace"),
            )
        assert param_obj._validate_parameter_file() == result

    # Mismatched optional filters in find_replace parameter (error-prone case)
    if param_file_name == "mismatch_filter_parameter.yml":
        assert param_obj._validate_parameter("find_replace") == (
            result,
            constants.PARAMETER_MSGS[msg].format("find_replace"),
        )

    # Missing required value in spark_pool parameter
    if param_file_name == "missing_replace_val_parameter.yml":
        assert param_obj._validate_parameter("spark_pool") == (
            result,
            constants.PARAMETER_MSGS[msg].format("replace_value", "spark_pool"),
        )

    # Invalid parameter name
    if param_file_name == "invalid_name_parameter.yml":
        assert param_obj._validate_parameter_names() == (
            result,
            constants.PARAMETER_MSGS[msg].format("spark_pool_param"),
        )

    # Errors in YAML content structure
    if param_file_name == "invalid_yaml_struc_parameter.yml":
        is_valid, msg = param_obj._validate_parameter_load()
        try:
            with Path.open(repository_directory / param_file_name, encoding="utf-8") as yaml_file:
                yaml_content = yaml_file.read()
                yaml.full_load(yaml_content)
        except yaml.YAMLError as e:
            error_message = str(e)

        assert is_valid == result
        assert msg == constants.PARAMETER_MSGS["invalid load"].format(error_message)

    # Mismatched quotes in YAML content
    if param_file_name == "invalid_yaml_char_parameter.yml":
        is_valid, msg = param_obj._validate_parameter_load()
        try:
            with Path.open(repository_directory / param_file_name, encoding="utf-8") as yaml_file:
                yaml_content = yaml_file.read()
                yaml.full_load(yaml_content)
        except yaml.YAMLError as e:
            error_message = str(e)

        assert is_valid == result
        assert msg == constants.PARAMETER_MSGS["invalid load"].format(error_message)

    # Invalid is_regex value in find_replace parameter
    if param_file_name == "invalid_is_regex_parameter.yml":
        assert param_obj._validate_parameter("find_replace") == (
            result,
            constants.PARAMETER_MSGS[msg].format("is_regex", "string", "find_replace"),
        )
