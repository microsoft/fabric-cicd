# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for Semantic Model parameter replacement by name using key_value_replace."""

import json
from pathlib import Path
from unittest import mock

import pytest

from fabric_cicd._parameter._utils import replace_key_value


class TestSemanticModelParameters:
    """Tests for replacing Semantic Model parameters by name."""

    @pytest.fixture
    def mock_workspace(self):
        """Creates a mock FabricWorkspace for testing."""
        mock_ws = mock.MagicMock()
        mock_ws.repository_directory = Path("/mock/repository")
        mock_ws.workspace_id = "mock-workspace-id"
        mock_ws.environment = "PROD"
        return mock_ws

    @pytest.fixture
    def semantic_model_json_simple(self):
        """Sample Semantic Model JSON with parameters in simple format."""
        return json.dumps({
            "model": {
                "parameters": [
                    {
                        "name": "DatabaseServer",
                        "currentValue": "sql-dev.contoso.net",
                        "description": "Database server connection",
                    },
                    {
                        "name": "DatabaseName",
                        "currentValue": "SalesDW_DEV",
                        "description": "Database name",
                    },
                ]
            }
        })

    @pytest.fixture
    def semantic_model_json_expressions(self):
        """Sample Semantic Model JSON with parameters as expressions (M query parameters)."""
        return json.dumps({
            "name": "Sales Model",
            "compatibilityLevel": 1550,
            "model": {
                "culture": "en-US",
                "expressions": [
                    {
                        "name": "DatabaseServer",
                        "kind": "m",
                        "expression": '"sql-dev.contoso.net" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
                    },
                    {
                        "name": "DatabaseName",
                        "kind": "m",
                        "expression": '"SalesDW_DEV" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
                    },
                ],
            },
        })

    def test_replace_parameter_by_name_simple_format(self, mock_workspace, semantic_model_json_simple):
        """Test replacing a Semantic Model parameter by name using simple parameters format."""
        param_dict = {
            "find_key": '$.model.parameters[?(@.name=="DatabaseServer")].currentValue',
            "replace_value": {"PPE": "sql-ppe.contoso.net", "PROD": "sql-prod.contoso.net"},
        }

        result = replace_key_value(mock_workspace, param_dict, semantic_model_json_simple, "PROD")
        result_dict = json.loads(result)

        # Verify the DatabaseServer parameter was replaced
        assert result_dict["model"]["parameters"][0]["currentValue"] == "sql-prod.contoso.net"
        # Verify the DatabaseName parameter was NOT replaced
        assert result_dict["model"]["parameters"][1]["currentValue"] == "SalesDW_DEV"

    def test_replace_multiple_parameters_by_name(self, mock_workspace, semantic_model_json_simple):
        """Test replacing multiple Semantic Model parameters by name in a single operation."""
        # First replacement
        param_dict_1 = {
            "find_key": '$.model.parameters[?(@.name=="DatabaseServer")].currentValue',
            "replace_value": {"PPE": "sql-ppe.contoso.net", "PROD": "sql-prod.contoso.net"},
        }

        result = replace_key_value(mock_workspace, param_dict_1, semantic_model_json_simple, "PROD")

        # Second replacement on the result
        param_dict_2 = {
            "find_key": '$.model.parameters[?(@.name=="DatabaseName")].currentValue',
            "replace_value": {"PPE": "SalesDW_PPE", "PROD": "SalesDW_PROD"},
        }

        result = replace_key_value(mock_workspace, param_dict_2, result, "PROD")
        result_dict = json.loads(result)

        # Verify both parameters were replaced
        assert result_dict["model"]["parameters"][0]["currentValue"] == "sql-prod.contoso.net"
        assert result_dict["model"]["parameters"][1]["currentValue"] == "SalesDW_PROD"

    def test_replace_parameter_expressions_format(self, mock_workspace, semantic_model_json_expressions):
        """Test replacing a Semantic Model parameter by name using expressions format (M query parameters)."""
        param_dict = {
            "find_key": '$.model.expressions[?(@.name=="DatabaseServer")].expression',
            "replace_value": {
                "PPE": '"sql-ppe.contoso.net" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
                "PROD": '"sql-prod.contoso.net" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
            },
        }

        result = replace_key_value(mock_workspace, param_dict, semantic_model_json_expressions, "PROD")
        result_dict = json.loads(result)

        # Verify the DatabaseServer expression was replaced
        expected_expression = (
            '"sql-prod.contoso.net" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'
        )
        assert result_dict["model"]["expressions"][0]["expression"] == expected_expression
        # Verify the DatabaseName expression was NOT replaced
        assert '"SalesDW_DEV"' in result_dict["model"]["expressions"][1]["expression"]

    def test_replace_with_different_environments(self, mock_workspace, semantic_model_json_simple):
        """Test that parameter replacement works correctly for different environments."""
        param_dict = {
            "find_key": '$.model.parameters[?(@.name=="DatabaseServer")].currentValue',
            "replace_value": {
                "PPE": "sql-ppe.contoso.net",
                "PROD": "sql-prod.contoso.net",
                "UAT": "sql-uat.contoso.net",
            },
        }

        # Test PPE environment
        mock_workspace.environment = "PPE"
        result_ppe = replace_key_value(mock_workspace, param_dict, semantic_model_json_simple, "PPE")
        result_dict_ppe = json.loads(result_ppe)
        assert result_dict_ppe["model"]["parameters"][0]["currentValue"] == "sql-ppe.contoso.net"

        # Test UAT environment
        mock_workspace.environment = "UAT"
        result_uat = replace_key_value(mock_workspace, param_dict, semantic_model_json_simple, "UAT")
        result_dict_uat = json.loads(result_uat)
        assert result_dict_uat["model"]["parameters"][0]["currentValue"] == "sql-uat.contoso.net"

    def test_parameter_not_found_no_error(self, mock_workspace, semantic_model_json_simple):
        """Test that searching for a non-existent parameter doesn't cause errors."""
        param_dict = {
            "find_key": '$.model.parameters[?(@.name=="NonExistentParameter")].currentValue',
            "replace_value": {"PROD": "some-value"},
        }

        # Should not raise an error, just won't find anything to replace
        result = replace_key_value(mock_workspace, param_dict, semantic_model_json_simple, "PROD")
        result_dict = json.loads(result)

        # Verify original parameters are unchanged
        assert result_dict["model"]["parameters"][0]["currentValue"] == "sql-dev.contoso.net"
        assert result_dict["model"]["parameters"][1]["currentValue"] == "SalesDW_DEV"

    def test_replace_connection_string_by_name(self, mock_workspace):
        """Test replacing connection properties by data source name."""
        semantic_model_with_datasource = json.dumps({
            "model": {
                "dataSources": [
                    {
                        "name": "SQL Server",
                        "type": "structured",
                        "connectionDetails": {
                            "protocol": "tds",
                            "address": {"server": "sql-dev.contoso.net", "database": "SalesDW"},
                        },
                    }
                ]
            }
        })

        param_dict = {
            "find_key": '$.model.dataSources[?(@.name=="SQL Server")].connectionDetails.address.server',
            "replace_value": {"PPE": "sql-ppe.contoso.net", "PROD": "sql-prod.contoso.net"},
        }

        result = replace_key_value(mock_workspace, param_dict, semantic_model_with_datasource, "PROD")
        result_dict = json.loads(result)

        # Verify the server address was replaced
        assert (
            result_dict["model"]["dataSources"][0]["connectionDetails"]["address"]["server"] == "sql-prod.contoso.net"
        )
        # Verify the database name was NOT replaced
        assert result_dict["model"]["dataSources"][0]["connectionDetails"]["address"]["database"] == "SalesDW"
