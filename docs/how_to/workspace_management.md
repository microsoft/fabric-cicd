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

For ISV scenarios requiring multiple workspace deployments, use configuration files.

#### Method 1: Inline Role Assignments

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
```

```python
from fabric_cicd import create_workspaces_from_config

results = create_workspaces_from_config(
    config_file_path="customer_workspaces.yml"
)
```

#### Method 2: Role Templates (Recommended for ISV Scenarios)

For scenarios where you need the same roles across multiple workspaces (e.g., admin access for all customer workspaces), use role templates.

**Create a roles template file** (`roles.yml`):

```yaml
role_templates:
  # Admin team - applies to all workspaces
  admin_team:
    - principal_id: "12345678-1234-1234-1234-123456789012"
      principal_type: "User"
      role: "Admin"
    - principal_id: "87654321-4321-4321-4321-210987654321"
      principal_type: "Group"
      role: "Admin"
  
  # Development team
  dev_team:
    - principal_id: "22222222-2222-2222-2222-222222222222"
      principal_type: "Group"
      role: "Contributor"
  
  # Analytics viewers
  analytics_viewers:
    - principal_id: "33333333-3333-3333-3333-333333333333"
      principal_type: "Group"
      role: "Viewer"
```

**Reference templates in workspace config**:

```yaml
workspaces:
  - display_name: "Customer-001-Workspace"
    description: "Customer 1 production workspace"
    capacity_id: "12345678-1234-1234-1234-123456789012"
    role_templates:  # Reference role templates
      - "admin_team"
      - "dev_team"
      - "analytics_viewers"

  - display_name: "Customer-002-Workspace"
    description: "Customer 2 production workspace"
    capacity_id: "12345678-1234-1234-1234-123456789012"
    role_templates:
      - "admin_team"  # All workspaces get admin access
      - "analytics_viewers"
  
  # You can also combine templates with inline assignments
  - display_name: "Customer-003-Workspace"
    description: "Customer 3 with mixed role assignment"
    capacity_id: "12345678-1234-1234-1234-123456789012"
    role_templates:
      - "admin_team"
    role_assignments:  # Customer-specific roles
      - principal_id: "44444444-4444-4444-4444-444444444444"
        principal_type: "User"
        role: "Member"
```

```python
from fabric_cicd import create_workspaces_from_config

# Create workspaces with role templates
results = create_workspaces_from_config(
    config_file_path="customer_workspaces.yml",
    roles_file_path="roles.yml"  # Provide roles template file
)

for result in results:
    print(f"Created: {result['workspace_name']} - {result['workspace_id']}")
```

**Benefits of Role Templates:**
- **Reusability**: Define common roles once, use across all workspaces
- **Consistency**: Ensure all workspaces have required admin access
- **Maintainability**: Update admin team in one place, applies to all workspaces
- **Scalability**: Perfect for ISV scenarios with 500+ workspaces needing the same roles

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

### ISV Scenario with Role Templates

For ISV scenarios with 500+ workspaces requiring consistent role assignments across all deployments:

**1. Define shared role templates** (`isv_roles.yml`):

```yaml
role_templates:
  # ISV admin team - manages all customer workspaces
  isv_admins:
    - principal_id: "isv-admin-group-guid"
      principal_type: "Group"
      role: "Admin"
    - principal_id: "isv-automation-sp-guid"
      principal_type: "ServicePrincipal"
      role: "Admin"
  
  # Monitoring and alerting
  monitoring_service:
    - principal_id: "monitoring-sp-guid"
      principal_type: "ServicePrincipal"
      role: "Viewer"
  
  # Support team - can troubleshoot
  support_team:
    - principal_id: "support-group-guid"
      principal_type: "Group"
      role: "Contributor"
  
  # Data engineering team
  data_engineers:
    - principal_id: "data-eng-group-guid"
      principal_type: "Group"
      role: "Contributor"
```

**2. Define customer workspaces** (`customer_workspaces.yml`):

```yaml
workspaces:
  # Customer A - Production + Dev
  - display_name: "CustomerA-Production"
    description: "Customer A production environment"
    capacity_id: "capacity-prod-guid"
    role_templates:  # All production workspaces get these
      - "isv_admins"
      - "monitoring_service"
      - "support_team"
    role_assignments:  # Customer-specific access
      - principal_id: "customerA-admin-guid"
        principal_type: "User"
        role: "Admin"
      - principal_id: "customerA-users-guid"
        principal_type: "Group"
        role: "Viewer"
  
  - display_name: "CustomerA-Development"
    description: "Customer A development environment"
    capacity_id: "capacity-dev-guid"
    role_templates:
      - "isv_admins"
      - "data_engineers"  # Dev environments allow data engineering
    role_assignments:
      - principal_id: "customerA-dev-team-guid"
        principal_type: "Group"
        role: "Contributor"
  
  # Customer B
  - display_name: "CustomerB-Production"
    description: "Customer B production environment"
    capacity_id: "capacity-prod-guid"
    role_templates:
      - "isv_admins"
      - "monitoring_service"
      - "support_team"
    role_assignments:
      - principal_id: "customerB-admin-guid"
        principal_type: "User"
        role: "Admin"
  
  # ... repeat for 500+ customers
```

**3. Automated deployment pipeline**:

```python
from fabric_cicd import (
    create_workspaces_from_config,
    FabricWorkspace,
    publish_all_items
)
from azure.identity import ClientSecretCredential

# Use service principal for automated deployment
credential = ClientSecretCredential(
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret"
)

# Create all customer workspaces with consistent role assignments
print("Creating customer workspaces...")
results = create_workspaces_from_config(
    config_file_path="customer_workspaces.yml",
    roles_file_path="isv_roles.yml",  # Shared roles across all customers
    token_credential=credential
)

# Track results
successful = [r for r in results if r.get('workspace_id')]
failed = [r for r in results if 'error' in r]

print(f"✓ Created {len(successful)} workspaces")
if failed:
    print(f"✗ Failed: {len(failed)} workspaces")
    for f in failed:
        print(f"  - {f['workspace_name']}: {f.get('error')}")

# Deploy artifacts to each workspace
repository_path = "/path/to/customer/artifacts"
item_types = ["Environment", "Notebook", "DataPipeline", "Lakehouse"]

for result in successful:
    print(f"\nDeploying to {result['workspace_name']}...")
    
    workspace = FabricWorkspace(
        workspace_id=result["workspace_id"],
        repository_directory=repository_path,
        item_type_in_scope=item_types,
        environment="PRODUCTION"
    )
    
    publish_all_items(workspace)
    print(f"✓ Deployed to {result['workspace_name']}")

print(f"\n✓ Complete: {len(successful)} customer workspaces deployed")
```





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


