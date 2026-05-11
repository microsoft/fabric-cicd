---
description: Assist with Microsoft Fabric CI/CD operations including workspace management, Git integration, deployment, and Fabric REST API usage.
---

# Skills for Fabric

You are an expert assistant for Microsoft Fabric CI/CD automation. Help users with workspace creation, Git connection, deployment, and using the fabric-cicd Python library.

## Workspace Management

### Creating Workspaces

To create Fabric workspaces (dev/test/prod):

```
python fabriccicd.py create           # Production mode
python fabriccicd.py create --realm   # Realm (daily) mode
```

The script creates three workspaces (`fabric-cicd-dev`, `fabric-cicd-test`, `fabric-cicd-prod`) and assigns security group roles in production mode.

**Fabric API used:** `POST https://api.fabric.microsoft.com/v1/workspaces`

### Connecting Git

To connect a DEV workspace to Azure DevOps Git:

```
python fabriccicd.py connect           # Production mode
python fabriccicd.py connect --realm   # Realm mode
```

This performs three steps:

1. Connect workspace to Git repository
2. Initialize the Git connection
3. Sync workspace from Git (pull latest)

**Fabric APIs used:**

- `POST /v1/workspaces/{id}/git/connect`
- `POST /v1/workspaces/{id}/git/initializeConnection`
- `POST /v1/workspaces/{id}/git/updateFromGit`

### Deploying Items

To deploy Fabric items to a workspace:

```
python fabriccicd.py deploy DEV           # Deploy to DEV
python fabriccicd.py deploy PROD          # Deploy to PROD
python fabriccicd.py deploy DEV --realm   # Deploy to DEV (realm)
```

### Running All Steps

To create workspaces, connect Git, and deploy in one command:

```
python fabriccicd.py all DEV
python fabriccicd.py all DEV --realm
```

## Configuration

All settings are in `fabriccicd.config.yml` (next to the script):

```yaml
# Production mode settings
prod:
    capacity_id: "your-capacity-id"
    security_group_id: "your-group-id"
    workspaces:
        DEV: "dev-workspace-id"
        TEST: "test-workspace-id"
        PROD: "prod-workspace-id"

# Realm mode settings
realm:
    capacity_id: "realm-capacity-id"
    realm_id: "your-realm-id"
    workspaces:
        DEV: "realm-dev-id"
        TEST: "realm-test-id"
        PROD: "realm-prod-id"

# Shared settings
shared:
    repo_path: "C:\\path\\to\\repo\\fabric"
    workspace_prefix: "fabric-cicd"
    git:
        provider: "AzureDevOps"
        organization: "msazure"
        project: "One"
        repository: "Asimov-vNext-Deployment"
        branch: "dev"
        directory: "fabric"
```

## fabric-cicd Python Library

### Basic Usage

```python
from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

token_credential = AzureCliCredential()
workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    environment="DEV",
    repository_directory="/path/to/workspace/items",
    item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
    token_credential=token_credential,
)

publish_all_items(workspace)
unpublish_all_orphan_items(workspace)
```

### Config-Based Deployment

```python
from azure.identity import AzureCliCredential
from fabric_cicd import deploy_with_config

token_credential = AzureCliCredential()
result = deploy_with_config(
    config_file_path="config.yml",
    environment="dev",
    token_credential=token_credential,
)
```

### Public API Exports

Only import from top-level `fabric_cicd`:

- `FabricWorkspace` - Workspace management
- `publish_all_items` - Deploy items
- `unpublish_all_orphan_items` - Remove orphaned items
- `deploy_with_config` - Config-based deployment
- `DeploymentResult`, `DeploymentStatus` - Result types
- `ItemType` - Supported Fabric item types
- `FeatureFlag`, `append_feature_flag` - Feature flags
- `change_log_level`, `configure_external_file_logging`, `disable_file_logging` - Logging

### Authentication

- **Local development:** `AzureCliCredential()` — run `az login` first
- **CI/CD pipelines:** `ClientSecretCredential()` with service principal
- **Testing/imports:** No authentication needed

## Microsoft Fabric REST API Reference

### Key Endpoints

| Operation        | Method | URL                                            |
| ---------------- | ------ | ---------------------------------------------- |
| Create workspace | POST   | `/v1/workspaces`                               |
| List workspaces  | GET    | `/v1/workspaces`                               |
| Delete workspace | DELETE | `/v1/workspaces/{id}`                          |
| Role assignments | POST   | `/v1/workspaces/{id}/roleAssignments`          |
| Connect to Git   | POST   | `/v1/workspaces/{id}/git/connect`              |
| Initialize Git   | POST   | `/v1/workspaces/{id}/git/initializeConnection` |
| Update from Git  | POST   | `/v1/workspaces/{id}/git/updateFromGit`        |
| Commit to Git    | POST   | `/v1/workspaces/{id}/git/commitToGit`          |
| Get Git status   | GET    | `/v1/workspaces/{id}/git/status`               |
| Create item      | POST   | `/v1/workspaces/{id}/items`                    |
| List items       | GET    | `/v1/workspaces/{id}/items`                    |

### Base URLs

- **Production:** `https://api.fabric.microsoft.com/v1`
- **Realm (daily):** `https://dailyapi.powerbi.com/v1`

### Authentication

All API calls require a Bearer token with scope `https://api.fabric.microsoft.com/.default`:

```python
from azure.identity import AzureCliCredential
cred = AzureCliCredential()
token = cred.get_token("https://api.fabric.microsoft.com/.default").token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
```

### Realm Mode

For realm/daily environments, add the realm header:

```python
headers["x-ms-fabric-realm-id"] = "your-realm-id"
```

### Full API Documentation

https://learn.microsoft.com/en-us/rest/api/fabric/

## Troubleshooting

| Problem                      | Solution                                       |
| ---------------------------- | ---------------------------------------------- |
| `CredentialUnavailableError` | Run `az login` first                           |
| Import errors                | Use `uv run python` prefix                     |
| 409 on workspace create      | Workspace already exists (script handles this) |
| Git already connected        | Script skips connection step automatically     |
| Long-running operation       | Script polls status until complete             |
