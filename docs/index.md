fabric-cicd is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library supports code-first Continuous Integration / Continuous Deployment (CI/CD) automations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal is to assist CI/CD developers who prefer not to interact directly with the Microsoft Fabric APIs.

## Base Expectations

-   Full deployment every time, without considering commit diffs
-   Deploys into the tenant of the executing identity

## Supported Item Types

The following item types are supported by the library:

-   Notebooks
-   Data Pipelines
-   Environments
-   Semantic Models
-   Reports

## Installation

To install fabric-cicd, run:

```bash
pip install fabric-cicd
```
While in private preview, to install fabric-cicd, run:

```bash
pip install --upgrade --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ fabric-cicd
```

## Basic Example
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

```python
# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id = "your-workspace-id",
    repository_directory = "your-repository-directory",
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"],
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
```
