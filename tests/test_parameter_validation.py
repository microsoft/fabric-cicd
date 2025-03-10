# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from pathlib import Path

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
      item_name: "World"
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

SAMPLE_PARAMETER_FILE_INVALID = """
find_replace:
    # Required Fields 
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value_1:
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
      file_path: "/World.Environment/Setting/Sparkcompute.yml"
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

SAMPLE_NOTEBOOK_FILE = """
# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "a277ea4a-e87f-8537-4ce0-39db11d4aade",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

print("Hello World db52be81-c2b2-4261-84fa-840c67f4bbd0")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
"""

FIND_REPLACE_DICT = {
    "find_value": "db52be81-c2b2-4261-84fa-840c67f4bbd0",
    "replace_value": {
        "PPE": "81bbb339-8d0b-46e8-bfa6-289a159c0733",
        "PROD": "5d6a1b16-447f-464a-b959-45d0fed35ca0",
    },
    "item_type": "Notebook",
    "item_name": "Hello World",
    "file_path": "/Hello World.Notebook/notebook-content.py",
}

SPARK_POOL_DICT = {
    "instance_pool_id": "72c68dbc-0775-4d59-909d-a47896f4573b",
    "replace_value": {
        "PPE": {"type": "Capacity", "name": "CapacityPool_Large_PPE"},
        "PROD": {"type": "Capacity", "name": "CapacityPool_Large_PROD"},
    },
    "item_name": "World",
}


@pytest.fixture
def repository_directory(tmp_path):
    # Create the sample workspace structure
    workspace_dir = tmp_path / "sample" / "workspace"
    workspace_dir.mkdir(parents=True, exist_ok=True)

    # Create the parameter file
    parameter_file_path = workspace_dir / "parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE)

    # Create the parameter file with old structure
    parameter_file_path = workspace_dir / "old_parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE_OLD)

    # Create the parameter file with invalid structure
    parameter_file_path = workspace_dir / "invalid_parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE_INVALID)

    # Create the notebook file
    notebook_dir = workspace_dir / "Hello World.Notebook"
    notebook_dir.mkdir(parents=True, exist_ok=True)

    notebook_platform_file_path = notebook_dir / ".platform"
    notebook_platform_file_path.write_text(SAMPLE_PLATFORM_FILE)

    notebook_file_path = notebook_dir / "notebook-content.py"
    notebook_file_path.write_text(SAMPLE_NOTEBOOK_FILE)

    return workspace_dir


@pytest.fixture
def parameter_validation_object(repository_directory):
    item_type_in_scope = ["DataPipeline", "Environment", "Notebook"]
    environment = "PPE"
    parameter_file_name = "parameter.yml"

    return ParameterValidation(
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
        environment=environment,
        parameter_file_name=parameter_file_name,
    )


@pytest.fixture
def parameter_validation_object_old_file(repository_directory):
    item_type_in_scope = ["DataPipeline", "Environment", "Notebook"]
    environment = "PPE"
    parameter_file_name = "old_parameter.yml"

    return ParameterValidation(
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
        environment=environment,
        parameter_file_name=parameter_file_name,
    )


@pytest.fixture
def parameter_validation_object_invalid_file(repository_directory):
    item_type_in_scope = ["DataPipeline", "Environment", "Notebook"]
    environment = "PPE"
    parameter_file_name = "invalid_parameter.yml"

    return ParameterValidation(
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
        environment=environment,
        parameter_file_name=parameter_file_name,
    )


def test_parameter_file_validation(parameter_validation_object):
    assert parameter_validation_object._validate_parameter_file() is True


def test_old_parameter_file_validation(parameter_validation_object_old_file):
    assert parameter_validation_object_old_file._validate_parameter_file() is True


def test_invalid_parameter_file_validation(parameter_validation_object_invalid_file):
    assert parameter_validation_object_invalid_file._validate_parameter_file() is False


def test_parameter_file_load_validation(parameter_validation_object):
    assert parameter_validation_object._validate_parameter_file_load() is True


def test_parameter_all_validation(parameter_validation_object):
    assert parameter_validation_object._validate_all_parameters() is True


def test_parameter_validation(parameter_validation_object):
    # assert parameter_validation_object._validate_all_parameters() is True
    assert parameter_validation_object._validate_parameter("find_replace", "find_value") is True
    assert parameter_validation_object._validate_parameter("spark_pool", "instance_pool_id") is True


def test_parameter_keys_validation(parameter_validation_object):
    find_replace_keys = ("find_value", "replace_value", "item_type", "item_name", "file_path")
    spark_pool_keys = ("instance_pool_id", "replace_value", "item_name")

    assert parameter_validation_object._validate_parameter_keys("find_replace", find_replace_keys, "N/A") is True
    assert parameter_validation_object._validate_parameter_keys("spark_pool", spark_pool_keys, "N/A") is True


def test_replace_value_validation(parameter_validation_object):
    assert parameter_validation_object._validate_replace_value(FIND_REPLACE_DICT, "find_replace", "") is True
    assert parameter_validation_object._validate_replace_value(SPARK_POOL_DICT, "spark_pool", "") is True


def test_optional_parameter_validation(parameter_validation_object):
    assert parameter_validation_object._validate_optional_parameters(FIND_REPLACE_DICT, "find_replace", "") is True
    assert parameter_validation_object._validate_optional_parameters(SPARK_POOL_DICT, "spark_pool", "") is False


def test_item_type_validation(parameter_validation_object):
    assert parameter_validation_object._validate_item_type("Notebook") is True
    # assert parameter_validation_object._validate_item_type("Environment") is True  # False


def test_item_name_validation(parameter_validation_object):
    assert parameter_validation_object._validate_item_name("Hello World") is True
    # assert parameter_validation_object._validate_item_name("Hello World 2") is False


def test_file_path_validation(parameter_validation_object):
    absolute_path = str(
        parameter_validation_object.repository_directory / Path("Hello World.Notebook/notebook-content.py")
    )
    normalized_relative_path_1 = "Hello World.Notebook/notebook-content.py"
    normalized_relative_path_2 = "Hello World.Notebook\\notebook-content.py"
    relative_path_forward = "/Hello World.Notebook/notebook-content.py"
    relative_path_back = "\\Hello World.Notebook\\notebook-content.py"
    # non_existent_path = "/Hello World 2.Notebook/notebook-content.py"
    # invalid_path = "/Hello World.Notebook/notebook-content/.py"

    assert parameter_validation_object._validate_file_path(absolute_path) is True
    assert parameter_validation_object._validate_file_path(normalized_relative_path_1) is True
    assert parameter_validation_object._validate_file_path(normalized_relative_path_2) is True
    assert parameter_validation_object._validate_file_path(relative_path_forward) is True
    assert parameter_validation_object._validate_file_path(relative_path_back) is True
    # assert parameter_validation_object._validate_file_path(non_existent_path) is False
    # assert parameter_validation_object._validate_file_path(invalid_path) is False


def test_environment_validation(parameter_validation_object):
    assert parameter_validation_object._validate_environment("PPE") is True
    assert parameter_validation_object._validate_environment("N/A") is False
