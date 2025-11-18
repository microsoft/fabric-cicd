# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides workspace management functions for creating and configuring workspaces."""

import logging
from pathlib import Path
from typing import Optional

import yaml
from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from fabric_cicd import constants
from fabric_cicd._common._check_utils import check_valid_guid
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._fabric_endpoint import FabricEndpoint

logger = logging.getLogger(__name__)


def create_workspace(
    display_name: str,
    description: str = "",
    capacity_id: Optional[str] = None,
    token_credential: Optional[TokenCredential] = None,
) -> dict:
    """
    Create a new Fabric workspace with optional capacity assignment.

    Args:
        display_name: The display name for the workspace.
        description: Optional description for the workspace.
        capacity_id: Optional capacity ID to assign to the workspace.
        token_credential: Optional token credential to use for API requests.

    Returns:
        dict: A dictionary containing workspace details including:
            - workspace_id (str): The ID of the created workspace
            - workspace_name (str): The display name of the workspace
            - capacity_id (str): The capacity ID if assigned
            - description (str): The workspace description

    Examples:
        Basic usage - Create workspace without capacity
        >>> from fabric_cicd import create_workspace
        >>> result = create_workspace(
        ...     display_name="Customer-Workspace-001",
        ...     description="Customer deployment workspace"
        ... )
        >>> print(result["workspace_id"])

        Create workspace with capacity assignment
        >>> from fabric_cicd import create_workspace
        >>> result = create_workspace(
        ...     display_name="Customer-Workspace-001",
        ...     description="Customer deployment workspace",
        ...     capacity_id="your-capacity-id"
        ... )

        Use with FabricWorkspace for deployment
        >>> from fabric_cicd import create_workspace, FabricWorkspace, publish_all_items
        >>> result = create_workspace(display_name="New-Workspace")
        >>> workspace = FabricWorkspace(
        ...     workspace_id=result["workspace_id"],
        ...     repository_directory="/path/to/repo"
        ... )
        >>> publish_all_items(workspace)

        With custom token credential
        >>> from azure.identity import ClientSecretCredential
        >>> from fabric_cicd import create_workspace
        >>> credential = ClientSecretCredential(
        ...     client_id="your-client-id",
        ...     client_secret="your-client-secret",
        ...     tenant_id="your-tenant-id"
        ... )
        >>> result = create_workspace(
        ...     display_name="Customer-Workspace-001",
        ...     capacity_id="your-capacity-id",
        ...     token_credential=credential
        ... )
    """
    # Validate inputs
    if not display_name or not isinstance(display_name, str):
        msg = "display_name must be a non-empty string"
        raise InputError(msg, logger)

    if not isinstance(description, str):
        msg = "description must be a string"
        raise InputError(msg, logger)

    if capacity_id is not None and not check_valid_guid(capacity_id):
        msg = f"capacity_id must be a valid GUID format: {capacity_id}"
        raise InputError(msg, logger)

    # Initialize endpoint
    endpoint = FabricEndpoint(
        token_credential=(
            # CodeQL [SM05139] Public library needing to have a default auth when user doesn't provide token. Not internal Azure product.
            DefaultAzureCredential() if token_credential is None else token_credential
        )
    )

    # Prepare request body
    body = {"displayName": display_name}

    if description:
        body["description"] = description

    if capacity_id:
        body["capacityId"] = capacity_id

    logger.info(f"Creating workspace '{display_name}'")

    # Create workspace
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/create-workspace
    response = endpoint.invoke(method="POST", url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces", body=body)

    workspace_id = response["body"]["id"]
    logger.info(f"{constants.INDENT}Workspace created with ID: {workspace_id}")

    return {
        "workspace_id": workspace_id,
        "workspace_name": response["body"]["displayName"],
        "capacity_id": response["body"].get("capacityId", ""),
        "description": response["body"].get("description", ""),
    }


def assign_workspace_to_capacity(
    workspace_id: str,
    capacity_id: str,
    token_credential: Optional[TokenCredential] = None,
) -> dict:
    """
    Assign a workspace to a capacity.

    Args:
        workspace_id: The ID of the workspace to assign.
        capacity_id: The capacity ID to assign the workspace to.
        token_credential: Optional token credential to use for API requests.

    Returns:
        dict: A dictionary containing assignment details.

    Examples:
        Basic usage
        >>> from fabric_cicd import assign_workspace_to_capacity
        >>> result = assign_workspace_to_capacity(
        ...     workspace_id="your-workspace-id",
        ...     capacity_id="your-capacity-id"
        ... )
    """
    # Validate inputs
    if not check_valid_guid(workspace_id):
        msg = f"workspace_id must be a valid GUID format: {workspace_id}"
        raise InputError(msg, logger)

    if not check_valid_guid(capacity_id):
        msg = f"capacity_id must be a valid GUID format: {capacity_id}"
        raise InputError(msg, logger)

    # Initialize endpoint
    endpoint = FabricEndpoint(
        token_credential=(
            # CodeQL [SM05139] Public library needing to have a default auth when user doesn't provide token. Not internal Azure product.
            DefaultAzureCredential() if token_credential is None else token_credential
        )
    )

    body = {"capacityId": capacity_id}

    logger.info(f"Assigning workspace '{workspace_id}' to capacity '{capacity_id}'")

    # Assign capacity
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/assign-to-capacity
    response = endpoint.invoke(
        method="POST",
        url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}/assignToCapacity",
        body=body,
    )

    logger.info(f"{constants.INDENT}Capacity assigned")

    return {"workspace_id": workspace_id, "capacity_id": capacity_id, "status_code": response["status_code"]}


def add_workspace_role_assignment(
    workspace_id: str,
    principal_id: str,
    principal_type: str,
    role: str,
    token_credential: Optional[TokenCredential] = None,
) -> dict:
    """
    Add a role assignment to a workspace.

    Args:
        workspace_id: The ID of the workspace.
        principal_id: The ID of the user, group, or service principal.
        principal_type: Type of principal ('User', 'Group', or 'ServicePrincipal').
        role: The role to assign ('Admin', 'Member', 'Contributor', or 'Viewer').
        token_credential: Optional token credential to use for API requests.

    Returns:
        dict: A dictionary containing role assignment details.

    Examples:
        Add user as workspace admin
        >>> from fabric_cicd import add_workspace_role_assignment
        >>> result = add_workspace_role_assignment(
        ...     workspace_id="your-workspace-id",
        ...     principal_id="user-object-id",
        ...     principal_type="User",
        ...     role="Admin"
        ... )

        Add service principal as member
        >>> result = add_workspace_role_assignment(
        ...     workspace_id="your-workspace-id",
        ...     principal_id="service-principal-object-id",
        ...     principal_type="ServicePrincipal",
        ...     role="Member"
        ... )

        Add group as contributor
        >>> result = add_workspace_role_assignment(
        ...     workspace_id="your-workspace-id",
        ...     principal_id="group-object-id",
        ...     principal_type="Group",
        ...     role="Contributor"
        ... )
    """
    # Validate inputs
    if not check_valid_guid(workspace_id):
        msg = f"workspace_id must be a valid GUID format: {workspace_id}"
        raise InputError(msg, logger)

    if not check_valid_guid(principal_id):
        msg = f"principal_id must be a valid GUID format: {principal_id}"
        raise InputError(msg, logger)

    if principal_type not in constants.WORKSPACE_PRINCIPAL_TYPES:
        msg = f"principal_type must be one of {constants.WORKSPACE_PRINCIPAL_TYPES}, got: {principal_type}"
        raise InputError(msg, logger)

    if role not in constants.WORKSPACE_ROLES:
        msg = f"role must be one of {constants.WORKSPACE_ROLES}, got: {role}"
        raise InputError(msg, logger)

    # Initialize endpoint
    endpoint = FabricEndpoint(
        token_credential=(
            # CodeQL [SM05139] Public library needing to have a default auth when user doesn't provide token. Not internal Azure product.
            DefaultAzureCredential() if token_credential is None else token_credential
        )
    )

    body = {"principal": {"id": principal_id, "type": principal_type}, "role": role}

    logger.info(f"Adding {role} role for {principal_type} '{principal_id}' to workspace '{workspace_id}'")

    # Add role assignment
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/workspace-role-assignments/add-workspace-role-assignment
    response = endpoint.invoke(
        method="POST", url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}/roleAssignments", body=body
    )

    logger.info(f"{constants.INDENT}Role assignment added")

    return {
        "workspace_id": workspace_id,
        "principal_id": principal_id,
        "principal_type": principal_type,
        "role": role,
        "status_code": response["status_code"],
    }


def create_workspaces_from_config(
    config_file_path: str,
    token_credential: Optional[TokenCredential] = None,
    roles_file_path: Optional[str] = None,
) -> list[dict]:
    """
    Create multiple workspaces from a configuration file.

    Args:
        config_file_path: Path to the YAML configuration file.
        token_credential: Optional token credential to use for API requests.
        roles_file_path: Optional path to a separate YAML file defining reusable role templates.

    Returns:
        list[dict]: A list of dictionaries containing details for each created workspace.

    Configuration file structure (YAML):
        ```yaml
        workspaces:
          - display_name: "Customer-Workspace-001"
            description: "Customer 1 deployment workspace"
            capacity_id: "your-capacity-id"  # Optional
            role_assignments:  # Optional - inline definitions
              - principal_id: "user-object-id"
                principal_type: "User"
                role: "Admin"
            role_templates:  # Optional - reference role templates from roles file
              - "admin_team"
              - "viewer_group"

          - display_name: "Customer-Workspace-002"
            description: "Customer 2 deployment workspace"
            capacity_id: "your-capacity-id"
            role_templates:  # Use common role templates
              - "admin_team"
        ```

    Roles file structure (YAML):
        ```yaml
        role_templates:
          admin_team:
            - principal_id: "admin-user-guid"
              principal_type: "User"
              role: "Admin"
            - principal_id: "admin-group-guid"
              principal_type: "Group"
              role: "Admin"
          viewer_group:
            - principal_id: "viewer-group-guid"
              principal_type: "Group"
              role: "Viewer"
        ```

    Examples:
        Create workspaces from config file
        >>> from fabric_cicd import create_workspaces_from_config
        >>> results = create_workspaces_from_config(
        ...     config_file_path="workspace_config.yml"
        ... )
        >>> for result in results:
        ...     print(f"Created: {result['workspace_name']} - {result['workspace_id']}")

        Create workspaces with role templates
        >>> results = create_workspaces_from_config(
        ...     config_file_path="workspace_config.yml",
        ...     roles_file_path="roles.yml"
        ... )

        ISV scenario - Deploy to multiple customer workspaces
        >>> from fabric_cicd import create_workspaces_from_config, FabricWorkspace, publish_all_items
        >>> results = create_workspaces_from_config("customer_workspaces.yml", roles_file_path="roles.yml")
        >>> for result in results:
        ...     workspace = FabricWorkspace(
        ...         workspace_id=result["workspace_id"],
        ...         repository_directory="/path/to/artifacts"
        ...     )
        ...     publish_all_items(workspace)
    """
    # Validate config file path
    config_path = Path(config_file_path)
    if not config_path.exists():
        msg = f"Configuration file not found: {config_file_path}"
        raise InputError(msg, logger)

    if not config_path.is_file():
        msg = f"Configuration path is not a file: {config_file_path}"
        raise InputError(msg, logger)

    # Load configuration
    try:
        with Path.open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        msg = f"Invalid YAML in configuration file: {e}"
        raise InputError(msg, logger) from e
    except Exception as e:
        msg = f"Error reading configuration file: {e}"
        raise InputError(msg, logger) from e

    # Validate configuration structure
    if not isinstance(config, dict):
        msg = "Configuration must be a dictionary"
        raise InputError(msg, logger)

    if "workspaces" not in config:
        msg = "Configuration must contain a 'workspaces' key"
        raise InputError(msg, logger)

    if not isinstance(config["workspaces"], list):
        msg = "'workspaces' must be a list"
        raise InputError(msg, logger)

    if not config["workspaces"]:
        msg = "'workspaces' list cannot be empty"
        raise InputError(msg, logger)

    # Load role templates if provided
    role_templates = {}
    if roles_file_path:
        roles_path = Path(roles_file_path)
        if not roles_path.exists():
            msg = f"Roles file not found: {roles_file_path}"
            raise InputError(msg, logger)

        if not roles_path.is_file():
            msg = f"Roles path is not a file: {roles_file_path}"
            raise InputError(msg, logger)

        try:
            with Path.open(roles_path, encoding="utf-8") as f:
                roles_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            msg = f"Invalid YAML in roles file: {e}"
            raise InputError(msg, logger) from e
        except Exception as e:
            msg = f"Error reading roles file: {e}"
            raise InputError(msg, logger) from e

        if not isinstance(roles_config, dict):
            msg = "Roles file must contain a dictionary"
            raise InputError(msg, logger)

        if "role_templates" in roles_config:
            if not isinstance(roles_config["role_templates"], dict):
                msg = "'role_templates' must be a dictionary"
                raise InputError(msg, logger)
            role_templates = roles_config["role_templates"]
            logger.info(f"Loaded {len(role_templates)} role template(s) from {roles_file_path}")

    # Create workspaces
    results = []
    for idx, workspace_config in enumerate(config["workspaces"]):
        if not isinstance(workspace_config, dict):
            msg = f"Workspace configuration at index {idx} must be a dictionary"
            raise InputError(msg, logger)

        if "display_name" not in workspace_config:
            msg = f"Workspace configuration at index {idx} must contain 'display_name'"
            raise InputError(msg, logger)

        display_name = workspace_config["display_name"]
        description = workspace_config.get("description", "")
        capacity_id = workspace_config.get("capacity_id")
        role_assignments = workspace_config.get("role_assignments", [])
        role_template_refs = workspace_config.get("role_templates", [])

        # Create workspace
        result = create_workspace(
            display_name=display_name,
            description=description,
            capacity_id=capacity_id,
            token_credential=token_credential,
        )

        # Collect all role assignments (inline + from templates)
        all_role_assignments = []

        # Add inline role assignments
        if role_assignments:
            if not isinstance(role_assignments, list):
                msg = f"role_assignments for workspace '{display_name}' must be a list"
                raise InputError(msg, logger)
            all_role_assignments.extend(role_assignments)

        # Add role assignments from templates
        if role_template_refs:
            if not isinstance(role_template_refs, list):
                msg = f"role_templates for workspace '{display_name}' must be a list"
                raise InputError(msg, logger)

            for template_name in role_template_refs:
                if not isinstance(template_name, str):
                    msg = f"Role template name must be a string in workspace '{display_name}', got: {type(template_name).__name__}"
                    raise InputError(msg, logger)

                if template_name not in role_templates:
                    msg = f"Role template '{template_name}' not found in roles file for workspace '{display_name}'"
                    raise InputError(msg, logger)

                template_roles = role_templates[template_name]
                if not isinstance(template_roles, list):
                    msg = f"Role template '{template_name}' must contain a list of role assignments"
                    raise InputError(msg, logger)

                all_role_assignments.extend(template_roles)

        # Apply all role assignments
        if all_role_assignments:
            for role_idx, role_config in enumerate(all_role_assignments):
                if not isinstance(role_config, dict):
                    msg = f"Role assignment at index {role_idx} for workspace '{display_name}' must be a dictionary"
                    raise InputError(msg, logger)

                required_keys = ["principal_id", "principal_type", "role"]
                for key in required_keys:
                    if key not in role_config:
                        msg = f"Role assignment at index {role_idx} for workspace '{display_name}' must contain '{key}'"
                        raise InputError(msg, logger)

                add_workspace_role_assignment(
                    workspace_id=result["workspace_id"],
                    principal_id=role_config["principal_id"],
                    principal_type=role_config["principal_type"],
                    role=role_config["role"],
                    token_credential=token_credential,
                )

        results.append(result)

    logger.info(f"Successfully created {len(results)} workspace(s)")
    return results
