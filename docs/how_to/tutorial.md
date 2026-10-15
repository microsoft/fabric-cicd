# My First Deployment

This step-by-step tutorial walks you through your first deployment with fabric-cicd — from installing the library to deploying Fabric items to a target workspace. By the end, you will have a working local deployment script that you can later integrate into a CI/CD pipeline.

## Prerequisites

Before you begin, make sure you have the following:

- **A supported Python version** installed (see [supported versions](../index.md#installation) and [download Python](https://www.python.org/downloads/))
- **Azure CLI** installed and logged in ([install guide](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli))
- **A Microsoft Fabric workspace** with at least one item (e.g., a Notebook or Data Pipeline)
- **Git Source Control** configured on a development workspace ([setup guide](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started))
- **Contributor permissions** on the target workspace where items will be deployed

## Step 1: Set Up Your Git Repository

fabric-cicd deploys items from files that were committed through [Fabric Source Control](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration). If you haven't already, connect your development workspace to a Git repository and commit your items.

Once committed, clone the repository to your local machine:

```bash
git clone https://github.com/your-org/your-repo.git
cd your-repo
```

Your repository should contain a directory structure like this, where each folder represents a Fabric item:

```
/your-workspace-directory
    /Hello World.Notebook
        notebook-content.py
        .platform
    /Run Hello World.DataPipeline
        pipeline-content.json
        .platform
    /World.Environment
        ...
```

!!! tip

    The directory names follow the pattern `<item-name>.<item-type>`. These are created automatically when you commit items through the Fabric Source Control UI.

## Step 2: Install fabric-cicd

Create and activate a virtual environment, then install the library:

=== "Windows"

    ```bash
    python -m venv .venv
    .venv\Scripts\activate
    pip install fabric-cicd
    ```

=== "macOS / Linux"

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install fabric-cicd
    ```

The installation includes required dependencies such as `azure-identity`, which manages authentication.

## Step 3: Authenticate with Azure

fabric-cicd requires an explicit credential to interact with the Fabric APIs. For local development, the simplest approach is Azure CLI:

```bash
az login
```

Sign in with an account that has access to your Fabric workspace.

!!! note

    The identity you sign in with must have **Contributor** (or higher) permissions on the target Fabric workspace.

## Step 4: Find Your Workspace ID

You need the ID of the **target workspace** — the workspace you want to deploy items _to_. This is different from the development workspace connected to Git.

To find it, open the target workspace in the [Fabric portal](https://app.fabric.microsoft.com). The workspace ID is in the URL:

```
https://app.fabric.microsoft.com/groups/<workspace-id>/list
```

Copy this ID — you'll use it in the next step.

!!! note

    You can use the workspace name instead of its ID by setting `workspace` rather than `workspace_id` in the configuration file.

## Step 5: Configure the Deployment

Create a file called `config.yml` in your repository root:

```yaml
core:
    workspace_id: "your-workspace-id" # Alternatively, use workspace: "your-workspace-name"
    repository_directory: "your-workspace-directory"
    item_types_in_scope: # Optional
        - Notebook
        - DataPipeline
        - Environment

publish:
    skip: false

unpublish:
    skip: false
```

!!! note

    Publishing and unpublishing run by default. Set `publish.skip` or `unpublish.skip` to `true` to disable that operation. Unpublishing removes items from the target workspace when they are no longer present in the repository.

Then create a file called `deploy.py` in the same directory:

```python
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import deploy_with_config

# Path to the deployment configuration
config_file_path = str(Path(__file__).resolve().parent / "config.yml")

# Target deployment environment
environment = "PPE"

# Deploy using the configuration file
deploy_with_config(
    config_file_path=config_file_path,
    token_credential=AzureCliCredential(),
    environment=environment,  # Optional
)
```

Replace the placeholder values:

| Placeholder                | Replace with                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------ |
| `your-workspace-id`        | The workspace ID from Step 4                                                               |
| `your-workspace-directory` | The folder containing your Fabric items                                                    |
| `item_types_in_scope` list | The item types you want to deploy (see [Supported Item Types](../reference/item_types.md)) |

!!! tip

    `item_types_in_scope` controls which item types are published and unpublished. If omitted, all supported item types are in scope.

See [Configuration Deployment](config_deployment.md) for all available configuration options.

## Step 6: Run the Deployment

Execute your script:

```bash
python deploy.py
```

You should see log output indicating each item being published to the target workspace. A successful run looks something like:

```text
[info]   15:40:20 - Loading configuration from config.yml for environment 'PPE'
[info]   15:40:21 - ########## Publishing Item 11/31: Notebook #############################################
[info]   15:40:21 - Publishing Notebook 'Hello World'
         15:40:22 - Published Notebook 'Hello World'
[info]   15:40:23 - ########## Publishing Item 20/31: DataPipeline #########################################
[info]   15:40:23 - Publishing DataPipeline 'Run Hello World'
         15:40:24 - Published DataPipeline 'Run Hello World'
[info]   15:40:25 - ########## Unpublishing Orphaned Items #################################################
[info]   15:40:25 - Unpublishing Notebook 'Old Notebook'
         15:40:26 - Unpublished Notebook 'Old Notebook'
[info]   15:40:27 - Config-based deployment completed successfully
```

!!! warning

    If you see `Failed to acquire Microsoft Entra token`, run `az login` and retry.

## Step 7: Add Parameter Replacement (Optional)

If you deploy to multiple environments (e.g., DEV, PPE, PROD), you can use a `parameter.yml` file to replace environment-specific values during deployment.

Create a `parameter.yml` file in the root of your workspace directory:

```yaml
find_replace:
    - find_value: "dev-lakehouse-id"
      replace_value:
          PPE: "ppe-lakehouse-id"
          PROD: "prod-lakehouse-id"
```

Then reference it from `config.yml`:

```yaml
core:
    workspace_id: "your-workspace-id"
    repository_directory: "your-workspace-directory"
    parameter: "your-workspace-directory/parameter.yml"
```

Set `environment = "PPE"` in `deploy.py`. During deployment, any occurrence of `dev-lakehouse-id` in your item definitions will be replaced with `ppe-lakehouse-id`.

For full details, see the [Parameterization](parameterization.md) guide.

## Alternative: Programmatic Deployment

If you prefer to define and run the deployment directly in Python instead of using a YAML configuration file:

```python
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

repository_directory = str(Path(__file__).resolve().parent / "your-workspace-directory")
environment = "PPE"

target_workspace = FabricWorkspace(
    workspace_id="your-workspace-id",  # Alternatively, use workspace_name="your-workspace-name"
    repository_directory=repository_directory,
    token_credential=AzureCliCredential(),
    environment=environment,  # Optional
    item_type_in_scope=["Notebook", "DataPipeline", "Environment"],  # Optional
)

publish_all_items(target_workspace)
unpublish_all_orphan_items(target_workspace)
```

## What's Next?

Now that you have a working local deployment, here are the recommended next steps:

- **Understand the philosophy** — Learn why fabric-cicd uses [full deployments instead of diffs](deployment_overview.md#deployment-philosophy).
- **Explore authentication options** — Review [authentication examples](../example/authentication.md) for service principals, managed identities, and Fabric Notebooks
- **Explore optional features** — Learn about [feature flags](optional_feature.md#feature-flags), [selective deployment](optional_feature.md#selective-deployment-features), and [bulk publish](optional_feature.md#bulk-publish)
- **Troubleshoot deployments** — Follow the [debugging and troubleshooting guidance](troubleshooting.md#debugging-deployments) for logging, common errors, and diagnostic scripts
- **Automate with CI/CD** — Integrate your script into a [release pipeline](../example/release_pipeline.md) using GitHub Actions or Azure DevOps
