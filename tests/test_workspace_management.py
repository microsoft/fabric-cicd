# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for workspace management functions."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from azure.identity import DefaultAzureCredential

from fabric_cicd import (
    add_workspace_role_assignment,
    assign_workspace_to_capacity,
    create_workspace,
    create_workspaces_from_config,
)
from fabric_cicd._common._exceptions import InputError


class TestCreateWorkspace:
    """Tests for create_workspace function."""

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_create_workspace_basic(self, mock_endpoint_class):
        """Test basic workspace creation without optional parameters."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        mock_endpoint.invoke.return_value = {
            "body": {
                "id": "test-workspace-id",
                "displayName": "Test Workspace",
                "description": "",
            },
            "status_code": 201,
        }

        # Execute
        result = create_workspace(display_name="Test Workspace")

        # Verify
        assert result["workspace_id"] == "test-workspace-id"
        assert result["workspace_name"] == "Test Workspace"
        assert result["capacity_id"] == ""
        assert result["description"] == ""

        # Verify API call
        mock_endpoint.invoke.assert_called_once()
        call_args = mock_endpoint.invoke.call_args
        assert call_args[1]["method"] == "POST"
        assert "workspaces" in call_args[1]["url"]
        assert call_args[1]["body"]["displayName"] == "Test Workspace"

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_create_workspace_with_description(self, mock_endpoint_class):
        """Test workspace creation with description."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        mock_endpoint.invoke.return_value = {
            "body": {
                "id": "test-workspace-id",
                "displayName": "Test Workspace",
                "description": "Test description",
            },
            "status_code": 201,
        }

        # Execute
        result = create_workspace(display_name="Test Workspace", description="Test description")

        # Verify
        assert result["description"] == "Test description"
        call_args = mock_endpoint.invoke.call_args
        assert call_args[1]["body"]["description"] == "Test description"

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_create_workspace_with_capacity(self, mock_endpoint_class):
        """Test workspace creation with capacity assignment."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        capacity_id = "12345678-1234-1234-1234-123456789012"
        mock_endpoint.invoke.return_value = {
            "body": {
                "id": "test-workspace-id",
                "displayName": "Test Workspace",
                "description": "",
                "capacityId": capacity_id,
            },
            "status_code": 201,
        }

        # Execute
        result = create_workspace(display_name="Test Workspace", capacity_id=capacity_id)

        # Verify
        assert result["capacity_id"] == capacity_id
        call_args = mock_endpoint.invoke.call_args
        assert call_args[1]["body"]["capacityId"] == capacity_id

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_create_workspace_with_custom_credential(self, mock_endpoint_class):
        """Test workspace creation with custom token credential."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        mock_endpoint.invoke.return_value = {
            "body": {
                "id": "test-workspace-id",
                "displayName": "Test Workspace",
                "description": "",
            },
            "status_code": 201,
        }
        mock_credential = Mock(spec=DefaultAzureCredential)

        # Execute
        result = create_workspace(display_name="Test Workspace", token_credential=mock_credential)

        # Verify
        assert result["workspace_id"] == "test-workspace-id"
        # Verify credential was passed to endpoint
        mock_endpoint_class.assert_called_once()
        assert mock_endpoint_class.call_args[1]["token_credential"] == mock_credential

    def test_create_workspace_invalid_display_name(self):
        """Test workspace creation with invalid display name."""
        with pytest.raises(InputError, match="display_name must be a non-empty string"):
            create_workspace(display_name="")

        with pytest.raises(InputError, match="display_name must be a non-empty string"):
            create_workspace(display_name=None)

    def test_create_workspace_invalid_description(self):
        """Test workspace creation with invalid description type."""
        with pytest.raises(InputError, match="description must be a string"):
            create_workspace(display_name="Test", description=123)

    def test_create_workspace_invalid_capacity_id(self):
        """Test workspace creation with invalid capacity ID."""
        with pytest.raises(InputError, match="capacity_id must be a valid GUID format"):
            create_workspace(display_name="Test", capacity_id="invalid-guid")


class TestAssignWorkspaceToCapacity:
    """Tests for assign_workspace_to_capacity function."""

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_assign_workspace_to_capacity(self, mock_endpoint_class):
        """Test assigning workspace to capacity."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        workspace_id = "12345678-1234-1234-1234-123456789012"
        capacity_id = "87654321-4321-4321-4321-210987654321"
        mock_endpoint.invoke.return_value = {"body": {}, "status_code": 200}

        # Execute
        result = assign_workspace_to_capacity(workspace_id=workspace_id, capacity_id=capacity_id)

        # Verify
        assert result["workspace_id"] == workspace_id
        assert result["capacity_id"] == capacity_id
        assert result["status_code"] == 200

        # Verify API call
        call_args = mock_endpoint.invoke.call_args
        assert call_args[1]["method"] == "POST"
        assert "assignToCapacity" in call_args[1]["url"]
        assert call_args[1]["body"]["capacityId"] == capacity_id

    def test_assign_workspace_invalid_workspace_id(self):
        """Test capacity assignment with invalid workspace ID."""
        with pytest.raises(InputError, match="workspace_id must be a valid GUID format"):
            assign_workspace_to_capacity(workspace_id="invalid", capacity_id="12345678-1234-1234-1234-123456789012")

    def test_assign_workspace_invalid_capacity_id(self):
        """Test capacity assignment with invalid capacity ID."""
        with pytest.raises(InputError, match="capacity_id must be a valid GUID format"):
            assign_workspace_to_capacity(workspace_id="12345678-1234-1234-1234-123456789012", capacity_id="invalid")


class TestAddWorkspaceRoleAssignment:
    """Tests for add_workspace_role_assignment function."""

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_add_workspace_role_user_admin(self, mock_endpoint_class):
        """Test adding user as admin."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        workspace_id = "12345678-1234-1234-1234-123456789012"
        principal_id = "87654321-4321-4321-4321-210987654321"
        mock_endpoint.invoke.return_value = {"body": {}, "status_code": 200}

        # Execute
        result = add_workspace_role_assignment(
            workspace_id=workspace_id, principal_id=principal_id, principal_type="User", role="Admin"
        )

        # Verify
        assert result["workspace_id"] == workspace_id
        assert result["principal_id"] == principal_id
        assert result["principal_type"] == "User"
        assert result["role"] == "Admin"
        assert result["status_code"] == 200

        # Verify API call
        call_args = mock_endpoint.invoke.call_args
        assert call_args[1]["method"] == "POST"
        assert "roleAssignments" in call_args[1]["url"]
        assert call_args[1]["body"]["principal"]["id"] == principal_id
        assert call_args[1]["body"]["principal"]["type"] == "User"
        assert call_args[1]["body"]["role"] == "Admin"

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_add_workspace_role_group_member(self, mock_endpoint_class):
        """Test adding group as member."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        workspace_id = "12345678-1234-1234-1234-123456789012"
        principal_id = "87654321-4321-4321-4321-210987654321"
        mock_endpoint.invoke.return_value = {"body": {}, "status_code": 200}

        # Execute
        result = add_workspace_role_assignment(
            workspace_id=workspace_id, principal_id=principal_id, principal_type="Group", role="Member"
        )

        # Verify
        assert result["principal_type"] == "Group"
        assert result["role"] == "Member"

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_add_workspace_role_service_principal_contributor(self, mock_endpoint_class):
        """Test adding service principal as contributor."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        workspace_id = "12345678-1234-1234-1234-123456789012"
        principal_id = "87654321-4321-4321-4321-210987654321"
        mock_endpoint.invoke.return_value = {"body": {}, "status_code": 200}

        # Execute
        result = add_workspace_role_assignment(
            workspace_id=workspace_id, principal_id=principal_id, principal_type="ServicePrincipal", role="Contributor"
        )

        # Verify
        assert result["principal_type"] == "ServicePrincipal"
        assert result["role"] == "Contributor"

    def test_add_workspace_role_invalid_workspace_id(self):
        """Test role assignment with invalid workspace ID."""
        with pytest.raises(InputError, match="workspace_id must be a valid GUID format"):
            add_workspace_role_assignment(
                workspace_id="invalid",
                principal_id="12345678-1234-1234-1234-123456789012",
                principal_type="User",
                role="Admin",
            )

    def test_add_workspace_role_invalid_principal_id(self):
        """Test role assignment with invalid principal ID."""
        with pytest.raises(InputError, match="principal_id must be a valid GUID format"):
            add_workspace_role_assignment(
                workspace_id="12345678-1234-1234-1234-123456789012",
                principal_id="invalid",
                principal_type="User",
                role="Admin",
            )

    def test_add_workspace_role_invalid_principal_type(self):
        """Test role assignment with invalid principal type."""
        with pytest.raises(InputError, match="principal_type must be one of"):
            add_workspace_role_assignment(
                workspace_id="12345678-1234-1234-1234-123456789012",
                principal_id="87654321-4321-4321-4321-210987654321",
                principal_type="InvalidType",
                role="Admin",
            )

    def test_add_workspace_role_invalid_role(self):
        """Test role assignment with invalid role."""
        with pytest.raises(InputError, match="role must be one of"):
            add_workspace_role_assignment(
                workspace_id="12345678-1234-1234-1234-123456789012",
                principal_id="87654321-4321-4321-4321-210987654321",
                principal_type="User",
                role="InvalidRole",
            )


class TestCreateWorkspacesFromConfig:
    """Tests for create_workspaces_from_config function."""

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_create_workspaces_from_config_basic(self, mock_endpoint_class, tmp_path):
        """Test creating workspaces from basic config file."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint

        # Mock responses for workspace creation
        def mock_invoke(method, url, body):
            if "workspaces" in url and method == "POST":
                return {
                    "body": {
                        "id": f"workspace-{body['displayName']}-id",
                        "displayName": body["displayName"],
                        "description": body.get("description", ""),
                        "capacityId": body.get("capacityId", ""),
                    },
                    "status_code": 201,
                }
            return {"body": {}, "status_code": 200}

        mock_endpoint.invoke.side_effect = mock_invoke

        # Create config file
        config_content = """
workspaces:
  - display_name: "Workspace 1"
    description: "First workspace"
  - display_name: "Workspace 2"
    description: "Second workspace"
"""
        config_file = tmp_path / "config.yml"
        config_file.write_text(config_content)

        # Execute
        results = create_workspaces_from_config(str(config_file))

        # Verify
        assert len(results) == 2
        assert results[0]["workspace_name"] == "Workspace 1"
        assert results[1]["workspace_name"] == "Workspace 2"

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_create_workspaces_from_config_with_capacity(self, mock_endpoint_class, tmp_path):
        """Test creating workspaces with capacity assignment."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        capacity_id = "12345678-1234-1234-1234-123456789012"

        def mock_invoke(method, url, body):
            if "workspaces" in url and method == "POST":
                return {
                    "body": {
                        "id": f"workspace-{body['displayName']}-id",
                        "displayName": body["displayName"],
                        "description": body.get("description", ""),
                        "capacityId": body.get("capacityId", ""),
                    },
                    "status_code": 201,
                }
            return {"body": {}, "status_code": 200}

        mock_endpoint.invoke.side_effect = mock_invoke

        # Create config file
        config_content = f"""
workspaces:
  - display_name: "Workspace 1"
    description: "First workspace"
    capacity_id: "{capacity_id}"
"""
        config_file = tmp_path / "config.yml"
        config_file.write_text(config_content)

        # Execute
        results = create_workspaces_from_config(str(config_file))

        # Verify
        assert len(results) == 1
        assert results[0]["capacity_id"] == capacity_id

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_create_workspaces_from_config_with_roles(self, mock_endpoint_class, tmp_path):
        """Test creating workspaces with role assignments."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        principal_id = "12345678-1234-1234-1234-123456789012"
        workspace_id = "87654321-4321-4321-4321-210987654321"

        def mock_invoke(method, url, body):
            if "workspaces" in url and "roleAssignments" not in url and method == "POST":
                return {
                    "body": {
                        "id": workspace_id,
                        "displayName": body["displayName"],
                        "description": body.get("description", ""),
                    },
                    "status_code": 201,
                }
            return {"body": {}, "status_code": 200}

        mock_endpoint.invoke.side_effect = mock_invoke

        # Create config file
        config_content = f"""
workspaces:
  - display_name: "Workspace 1"
    description: "First workspace"
    role_assignments:
      - principal_id: "{principal_id}"
        principal_type: "User"
        role: "Admin"
"""
        config_file = tmp_path / "config.yml"
        config_file.write_text(config_content)

        # Execute
        results = create_workspaces_from_config(str(config_file))

        # Verify
        assert len(results) == 1
        # Verify role assignment was called
        role_calls = [call for call in mock_endpoint.invoke.call_args_list if "roleAssignments" in str(call)]
        assert len(role_calls) == 1

    def test_create_workspaces_from_config_file_not_found(self):
        """Test with non-existent config file."""
        with pytest.raises(InputError, match="Configuration file not found"):
            create_workspaces_from_config("nonexistent.yml")

    def test_create_workspaces_from_config_invalid_yaml(self, tmp_path):
        """Test with invalid YAML."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("invalid: yaml: content:")

        with pytest.raises(InputError, match="Invalid YAML"):
            create_workspaces_from_config(str(config_file))

    def test_create_workspaces_from_config_missing_workspaces_key(self, tmp_path):
        """Test config without 'workspaces' key."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("other_key: value")

        with pytest.raises(InputError, match="Configuration must contain a 'workspaces' key"):
            create_workspaces_from_config(str(config_file))

    def test_create_workspaces_from_config_empty_workspaces(self, tmp_path):
        """Test config with empty workspaces list."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("workspaces: []")

        with pytest.raises(InputError, match="'workspaces' list cannot be empty"):
            create_workspaces_from_config(str(config_file))

    def test_create_workspaces_from_config_missing_display_name(self, tmp_path):
        """Test config with workspace missing display_name."""
        config_content = """
workspaces:
  - description: "Missing name"
"""
        config_file = tmp_path / "config.yml"
        config_file.write_text(config_content)

        with pytest.raises(InputError, match="must contain 'display_name'"):
            create_workspaces_from_config(str(config_file))

    @patch("fabric_cicd.workspace_management.FabricEndpoint")
    def test_create_workspaces_from_config_invalid_role_assignment(self, mock_endpoint_class, tmp_path):
        """Test config with invalid role assignment structure."""
        # Setup mock
        mock_endpoint = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint
        workspace_id = "87654321-4321-4321-4321-210987654321"

        def mock_invoke(method, url, body):
            if "workspaces" in url and "roleAssignments" not in url and method == "POST":
                return {
                    "body": {
                        "id": workspace_id,
                        "displayName": body["displayName"],
                        "description": body.get("description", ""),
                    },
                    "status_code": 201,
                }
            return {"body": {}, "status_code": 200}

        mock_endpoint.invoke.side_effect = mock_invoke

        config_content = """
workspaces:
  - display_name: "Workspace 1"
    role_assignments:
      - invalid_key: "value"
"""
        config_file = tmp_path / "config.yml"
        config_file.write_text(config_content)

        with pytest.raises(InputError, match="must contain 'principal_id'"):
            create_workspaces_from_config(str(config_file))
