# Workspace Management

This guide covers creating and managing Fabric workspaces programmatically, particularly for ISV scenarios requiring large-scale workspace deployment.

## Overview

The workspace management features enable you to:

- Create workspaces programmatically
- Assign workspaces to capacities
- Add role assignments (users, groups, service principals)
- Create multiple workspaces from configuration files
- Deploy artifacts to newly created workspaces

This is particularly useful for ISV scenarios where you need to deploy customer-specific workspaces at scale (e.g., 500+ workspaces).

## Basic Workspace Creation

### Create a Single Workspace

```python
from fabric_cicd import create_workspace

# Create a workspace
result = create_workspace(
    display_name="Customer-Workspace-001",
    description="Customer deployment workspace"
)

print(f"Workspace ID: {result['workspace_id']}")
print(f"Workspace Name: {result['workspace_name']}")
```

### Create Workspace with Capacity Assignment

```python
from fabric_cicd import create_workspace

# Create workspace and assign to capacity
result = create_workspace(
    display_name="Customer-Workspace-001",
    description="Customer deployment workspace",
    capacity_id="your-capacity-id"
)

print(f"Workspace ID: {result['workspace_id']}")
print(f"Capacity ID: {result['capacity_id']}")
```

### Use Custom Authentication

```python
from azure.identity import ClientSecretCredential
from fabric_cicd import create_workspace

# Setup service principal authentication
credential = ClientSecretCredential(
    client_id="your-client-id",
    client_secret="your-client-secret",
    tenant_id="your-tenant-id"
)

# Create workspace with custom credential
result = create_workspace(
    display_name="Customer-Workspace-001",
    capacity_id="your-capacity-id",
    token_credential=credential
)
```

## Managing Workspace Access

### Add User as Admin

```python
from fabric_cicd import add_workspace_role_assignment

add_workspace_role_assignment(
    workspace_id="your-workspace-id",
    principal_id="user-object-id",
    principal_type="User",
    role="Admin"
)
```

### Add Group as Member

```python
from fabric_cicd import add_workspace_role_assignment

add_workspace_role_assignment(
    workspace_id="your-workspace-id",
    principal_id="group-object-id",
    principal_type="Group",
    role="Member"
)
```

### Add Service Principal as Contributor

```python
from fabric_cicd import add_workspace_role_assignment

add_workspace_role_assignment(
    workspace_id="your-workspace-id",
    principal_id="service-principal-object-id",
    principal_type="ServicePrincipal",
    role="Contributor"
)
```

### Available Roles

- **Admin**: Full control over the workspace
- **Member**: Can publish and manage content
- **Contributor**: Can publish content
- **Viewer**: Read-only access

### Supported Principal Types

- **User**: Individual user account
- **Group**: Azure AD security group
- **ServicePrincipal**: Service principal/application

## Capacity Management

### Assign Existing Workspace to Capacity

```python
from fabric_cicd import assign_workspace_to_capacity

result = assign_workspace_to_capacity(
    workspace_id="your-workspace-id",
    capacity_id="your-capacity-id"
)

print(f"Assigned workspace {result['workspace_id']} to capacity {result['capacity_id']}")
```

## Deploying to Created Workspaces

### Create Workspace and Deploy Items

```python
from fabric_cicd import create_workspace, FabricWorkspace, publish_all_items

# Create workspace
result = create_workspace(
    display_name="Customer-Workspace-001",
    capacity_id="your-capacity-id"
)

# Initialize FabricWorkspace with the new workspace ID
workspace = FabricWorkspace(
    workspace_id=result["workspace_id"],
    repository_directory="/path/to/artifacts",
    item_type_in_scope=["Notebook", "DataPipeline", "Environment"]
)

# Deploy items
publish_all_items(workspace)
```

## Bulk Workspace Creation

### Using Configuration Files

For ISV scenarios requiring multiple workspace deployments, use configuration files:

#### Create Configuration File

Create a YAML file (e.g., `customer_workspaces.yml`):

```yaml
workspaces:
  - display_name: "Customer-001-Workspace"
    description: "Customer 1 production workspace"
    capacity_id: "12345678-1234-1234-1234-123456789012"
    role_assignments:
      - principal_id: "87654321-4321-4321-4321-210987654321"
        principal_type: "User"
        role: "Admin"
      - principal_id: "11111111-1111-1111-1111-111111111111"
        principal_type: "Group"
        role: "Member"

  - display_name: "Customer-002-Workspace"
    description: "Customer 2 production workspace"
    capacity_id: "12345678-1234-1234-1234-123456789012"
    role_assignments:
      - principal_id: "22222222-2222-2222-2222-222222222222"
        principal_type: "User"
        role: "Admin"

  - display_name: "Customer-003-Workspace"
    description: "Customer 3 production workspace"
    capacity_id: "12345678-1234-1234-1234-123456789012"
```

#### Create Workspaces from Configuration

```python
from fabric_cicd import create_workspaces_from_config

# Create all workspaces defined in the config
results = create_workspaces_from_config(
    config_file_path="customer_workspaces.yml"
)

# Display results
for result in results:
    print(f"Created: {result['workspace_name']} - {result['workspace_id']}")
```

## ISV Scenario: Large-Scale Deployment

### Complete ISV Deployment Pipeline

This example shows how to create multiple customer workspaces and deploy artifacts to each:

```python
from fabric_cicd import (
    create_workspaces_from_config,
    FabricWorkspace,
    publish_all_items,
    unpublish_all_orphan_items
)

# Step 1: Create all customer workspaces
print("Creating customer workspaces...")
results = create_workspaces_from_config(
    config_file_path="customer_workspaces.yml"
)

# Step 2: Deploy artifacts to each workspace
repository_path = "/path/to/artifacts"
item_types = ["Environment", "Notebook", "DataPipeline", "Lakehouse"]

for result in results:
    print(f"\nDeploying to {result['workspace_name']}...")
    
    # Initialize workspace
    workspace = FabricWorkspace(
        workspace_id=result["workspace_id"],
        repository_directory=repository_path,
        item_type_in_scope=item_types,
        environment="PRODUCTION"
    )
    
    # Publish all items
    publish_all_items(workspace)
    
    # Clean up orphaned items
    unpublish_all_orphan_items(workspace)
    
    print(f"✓ Deployment complete for {result['workspace_name']}")

print(f"\n✓ Successfully deployed to {len(results)} workspaces")
```

### Dynamic Configuration Generation

For scenarios where you need to generate workspace configurations programmatically:

```python
import yaml
from fabric_cicd import create_workspaces_from_config, FabricWorkspace, publish_all_items

# Generate configuration for multiple customers
customers = [
    {"name": "Customer-001", "admin_id": "user-guid-1"},
    {"name": "Customer-002", "admin_id": "user-guid-2"},
    {"name": "Customer-003", "admin_id": "user-guid-3"},
    # ... up to 500 customers
]

capacity_id = "your-capacity-id"
config = {"workspaces": []}

for customer in customers:
    workspace_config = {
        "display_name": f"{customer['name']}-Workspace",
        "description": f"Production workspace for {customer['name']}",
        "capacity_id": capacity_id,
        "role_assignments": [
            {
                "principal_id": customer["admin_id"],
                "principal_type": "User",
                "role": "Admin"
            }
        ]
    }
    config["workspaces"].append(workspace_config)

# Save configuration
with open("generated_workspaces.yml", "w") as f:
    yaml.dump(config, f)

# Create workspaces
results = create_workspaces_from_config("generated_workspaces.yml")

# Deploy to all
repository_path = "/path/to/artifacts"
for result in results:
    workspace = FabricWorkspace(
        workspace_id=result["workspace_id"],
        repository_directory=repository_path
    )
    publish_all_items(workspace)
```

### Parallel Deployment for Performance

For faster deployment across many workspaces:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from fabric_cicd import (
    create_workspaces_from_config,
    FabricWorkspace,
    publish_all_items
)

def deploy_to_workspace(workspace_info, repo_path):
    """Deploy artifacts to a single workspace."""
    try:
        workspace = FabricWorkspace(
            workspace_id=workspace_info["workspace_id"],
            repository_directory=repo_path
        )
        publish_all_items(workspace)
        return {"success": True, "workspace": workspace_info["workspace_name"]}
    except Exception as e:
        return {"success": False, "workspace": workspace_info["workspace_name"], "error": str(e)}

# Create workspaces
results = create_workspaces_from_config("customer_workspaces.yml")
repository_path = "/path/to/artifacts"

# Deploy in parallel (adjust max_workers based on capacity limits)
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {
        executor.submit(deploy_to_workspace, result, repository_path): result
        for result in results
    }
    
    for future in as_completed(futures):
        deployment_result = future.result()
        if deployment_result["success"]:
            print(f"✓ {deployment_result['workspace']}")
        else:
            print(f"✗ {deployment_result['workspace']}: {deployment_result['error']}")
```

## Environment-Specific Deployment

Deploy different configurations based on environment:

```python
from fabric_cicd import create_workspace, FabricWorkspace, publish_all_items

# Create workspaces for different environments
environments = {
    "DEV": "dev-capacity-id",
    "TEST": "test-capacity-id",
    "PROD": "prod-capacity-id"
}

for env, capacity in environments.items():
    # Create workspace
    result = create_workspace(
        display_name=f"Customer-001-{env}",
        description=f"Customer 1 {env} environment",
        capacity_id=capacity
    )
    
    # Deploy with environment-specific parameters
    workspace = FabricWorkspace(
        workspace_id=result["workspace_id"],
        repository_directory="/path/to/artifacts",
        environment=env
    )
    publish_all_items(workspace)
```

## Best Practices

### 1. Error Handling

```python
from fabric_cicd import create_workspace
from fabric_cicd._common._exceptions import InputError

try:
    result = create_workspace(
        display_name="Customer-Workspace",
        capacity_id="your-capacity-id"
    )
except InputError as e:
    print(f"Validation error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### 2. Logging

```python
from fabric_cicd import create_workspace, change_log_level

# Enable debug logging for detailed information
change_log_level("DEBUG")

result = create_workspace(display_name="Customer-Workspace")
```

### 3. Configuration Validation

Validate your configuration file structure before creating workspaces:

```yaml
workspaces:
  - display_name: "Valid-Workspace-Name"  # Required
    description: "Optional description"     # Optional
    capacity_id: "valid-guid-format"        # Optional, must be valid GUID
    role_assignments:                        # Optional
      - principal_id: "valid-guid"          # Required if role_assignments present
        principal_type: "User"               # Must be: User, Group, or ServicePrincipal
        role: "Admin"                        # Must be: Admin, Member, Contributor, or Viewer
```

### 4. Capacity Planning

- Ensure your capacity has sufficient resources before creating multiple workspaces
- Monitor capacity utilization during bulk deployments
- Consider rate limiting for very large deployments (500+ workspaces)

### 5. Authentication

For production ISV scenarios, use service principal authentication:

```python
from azure.identity import ClientSecretCredential
from fabric_cicd import create_workspaces_from_config

credential = ClientSecretCredential(
    client_id="your-client-id",
    client_secret="your-client-secret",
    tenant_id="your-tenant-id"
)

results = create_workspaces_from_config(
    config_file_path="customer_workspaces.yml",
    token_credential=credential
)
```

## API Reference

For detailed API documentation, see the [Code Reference](../code_reference.md#workspace-management) section.

### Related Functions

- [`create_workspace`](../code_reference.md#create_workspace): Create a single workspace
- [`create_workspaces_from_config`](../code_reference.md#create_workspaces_from_config): Create multiple workspaces from YAML config
- [`assign_workspace_to_capacity`](../code_reference.md#assign_workspace_to_capacity): Assign workspace to capacity
- [`add_workspace_role_assignment`](../code_reference.md#add_workspace_role_assignment): Add user/group/SP to workspace
- [`FabricWorkspace`](../code_reference.md#fabricworkspace): Initialize workspace for deployment
- [`publish_all_items`](../code_reference.md#publish_all_items): Deploy items to workspace

## Troubleshooting

### Common Issues

**Issue**: "capacity_id must be a valid GUID format"
- **Solution**: Ensure capacity ID is in the correct format: `12345678-1234-1234-1234-123456789012`

**Issue**: "workspace_id must be a valid GUID format"
- **Solution**: Verify the workspace ID format matches the GUID pattern

**Issue**: "principal_type must be one of ['User', 'Group', 'ServicePrincipal']"
- **Solution**: Check the `principal_type` value in role assignments matches exactly (case-sensitive)

**Issue**: "role must be one of ['Admin', 'Member', 'Contributor', 'Viewer']"
- **Solution**: Verify the role name is spelled correctly and matches one of the supported values

**Issue**: "Configuration file not found"
- **Solution**: Verify the path to your YAML configuration file is correct and accessible

**Issue**: Failed to acquire AAD token
- **Solution**: Ensure you're authenticated with `az login` or provide valid service principal credentials
