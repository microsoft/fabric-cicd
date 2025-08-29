fabric-cicd is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library supports code-first Continuous Integration / Continuous Deployment (CI/CD) automations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal is to assist CI/CD developers who prefer not to interact directly with the Microsoft Fabric APIs.

## Base Expectations

-   Full deployment every time, without considering commit diffs
-   Deploys into the tenant of the executing identity
-   Only supports items that have Source Control, and Public Create/Update APIs

## Supported Item Types

<!--BEGIN-SUPPORTED-ITEM-TYPES-->
<!--END-SUPPORTED-ITEM-TYPES-->

## Installation

**Requirements**: Python 3.9 to 3.12

To install fabric-cicd, run:

```bash
pip install fabric-cicd
```

## Basic Example

```python
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id = "your-workspace-id",
    environment = "your-target-environment",
    repository_directory = "your-repository-directory",
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"],
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
```

> **Note**: The `environment` parameter is required for parameter replacement to work properly. It must match one of the environment keys defined in your `parameter.yml` file (e.g., "PPE", "PROD", "DEV"). If you don't need parameter replacement, you can omit this parameter.
