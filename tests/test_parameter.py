# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


from unittest.mock import Mock

import pytest

from fabric_cicd._parameterization import ParameterValidation

# Integration tests for the Parameter Validation class

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

SAMPLE_PLATFORM_FILE = "Sample Notebook .Platform File"

SAMPLE_NOTEBOOK_FILE = """
# Fabric notebook source

# METADATA ********************

# CELL ********************

print("Hello World db52be81-c2b2-4261-84fa-840c67f4bbd0")

# METADATA ********************
"""


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

    def debug(self, message):
        self.messages.append(message)

    def warning(self, message):
        self.messages.append(message)

    def error(self, message):
        self.messages.append(message)


@pytest.fixture
def setup_mocks(monkeypatch, mocker):
    dl = DummyLogger()
    mock_logger = mocker.Mock()
    mock_logger.isEnabledFor.return_value = True
    mock_logger.info.side_effect = dl.info
    mock_logger.debug.side_effect = dl.debug
    mock_logger.warning.side_effect = dl.warning
    mock_logger.error.side_effect = dl.error
    monkeypatch.setattr("fabric_cicd._parameterization._parameter_validation.logger", mock_logger)
    mock_requests = mocker.patch("requests.request")
    return dl, mock_requests


@pytest.fixture
def setup_repository_directory(tmp_path):
    # Create the sample workspace structure
    workspace_dir = tmp_path / "sample" / "workspace"
    workspace_dir.mkdir(parents=True, exist_ok=True)

    # Create the parameter file
    parameter_file_path = workspace_dir / "parameter.yml"
    parameter_file_path.write_text(SAMPLE_PARAMETER_FILE)

    # Create the notebook file
    notebook_dir = workspace_dir / "Hello World.Notebook"
    notebook_dir.mkdir(parents=True, exist_ok=True)

    notebook_platform_file_path = notebook_dir / ".platform"
    notebook_platform_file_path.write_text(SAMPLE_PLATFORM_FILE)

    notebook_file_path = notebook_dir / "notebook-content.py"
    notebook_file_path.write_text(SAMPLE_NOTEBOOK_FILE)

    return workspace_dir


def test_integration(setup_mocks, setup_repository_directory):
    dl, mock_requests = setup_mocks
    mock_requests.return_value = Mock()
    dl.info("Parameter file validation passed")

    pv = ParameterValidation(
        repository_directory=str(setup_repository_directory),
        item_type_in_scope=["Notebook", "Environment", "DataPipeline"],
        environment="PPE",
        parameter_file_name="parameter.yml",
    )

    assert pv._validate_parameter_file() == True
