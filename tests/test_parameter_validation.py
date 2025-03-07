# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pytest

from fabric_cicd._parameterization import ParameterValidation, _parameterization_utils

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
      item_name: "World"
"""


@pytest.fixture
def parameter_file(tmp_path):
    parameter_file_path = tmp_path / "parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE)
    return parameter_file_path


@pytest.fixture
def parameter_dict(tmp_path):
    parameter_file_path = tmp_path / "parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE)
    return _parameterization_utils.load_parameters_to_dict(parameter_file_path)
    # return {"find_replace": [{"find_value": "db52be81-c2b2-4261-84fa-840c67f4bbd0", "replace_value": {"PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733", "PROD": "5d6a1b16-447f-464a-b959-45d0fed35ca0"}, "item_type": "Notebook", "item_name": ["Hello World", "Hello World Subfolder"], "file_path": "/Hello World.Notebook/notebook-content.py"}], "spark_pool": [{"instance_pool_id": "72c68dbc-0775-4d59-909d-a47896f4573b", "replace_value": {"PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"}, "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"}}, "item_name": "World"}]}
    # return parameter_file_path


@pytest.fixture
def parameter_validation_object(parameter_file):
    repository_directory = str(parameter_file.parent)
    item_type_in_scope = ["DataPipeline", "Environment", "Notebook"]
    environment = "PPE"
    return ParameterValidation(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        environment=environment,
    )


def test_parameter_file_load_validation(parameter_validation_object):
    assert parameter_validation_object._validate_parameter_file_load() is True


def test_parameter_keys_validation(parameter_validation_object):
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace", ("find_value", "replace_value"), "Key Error in find_replace"
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "spark_pool", ("instance_pool_id", "replace_value"), "Key Error in spark_pool"
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace", ("find_value", "replace_value", "item_type"), "Key Error in find_replace"
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "spark_pool", ("instance_pool_id", "replace_value", "item_name"), "Key Error in spark_pool"
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace",
            ("find_value", "replace_value", "item_type", "item_name", "file_path"),
            "Key Error in find_replace",
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "spark_pool", ("instance_pool_id", "replace_value", "item_name"), "Key Error in spark_pool"
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace",
            ("find_value", "replace_value", "item_type", "item_name", "file_path"),
            "Key Error in find_replace",
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "spark_pool", ("instance_pool_id", "replace_value", "item_name"), "Key Error in spark_pool"
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace",
            ("find_value", "replace_value", "item_type", "item_name", "file_path"),
            "Key Error in find_replace",
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "spark_pool", ("instance_pool_id", "replace_value", "item_name"), "Key Error in spark_pool"
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "find_replace",
            ("find_value", "replace_value", "item_type", "item_name", "file_path"),
            "Key Error in find_replace",
        )
        is True
    )
    assert (
        parameter_validation_object._validate_parameter_keys(
            "spark_pool", ("instance_pool_id", "replace_value", "item_name"), "Key Error in spark_pool"
        )
        is True
    )


# Fixtures

# Tests:
# 1 test_validate_parameter_file_load
# 2 test_validate_all_parameters
# 3 test_validate_parameter_keys
# 4 test_validate_replace_value
#

"""
@patch("parameter_validation.load_parameters_to_dict")
def test_validate_parameter_file_load(mock_load_parameters_to_dict):
    # Mock the load_parameters_to_dict function
    mock_load_parameters_to_dict.return_value = {"param1": "value1"}

    pv = ParameterValidation("valid_directory", ["type1", "type2"], "valid_environment")
    assert pv._validate_parameter_file_load() is True


def test_validate_all_parameters():
    pv = ParameterValidation("valid_directory", ["type1", "type2"], "valid_environment")
    pv.environment_parameter = {"find_replace": [{"find_value": "value1", "replace_value": {"env1": "value2"}}]}
    assert pv._validate_all_parameters() is True


def test_validate_parameter_keys():
    pv = ParameterValidation("valid_directory", ["type1", "type2"], "valid_environment")
    assert pv._validate_parameter_keys("find_replace", ("find_value", "replace_value"), "Error") is True


def test_validate_replace_value():
    pv = ParameterValidation("valid_directory", ["type1", "type2"], "valid_environment")
    param_dict = {"find_value": "value1", "replace_value": {"env1": "value2"}}
    assert pv._validate_replace_value(param_dict, "find_replace", "Error") is True

"""
# if __name__ == "__main__":
#    pytest.main()
