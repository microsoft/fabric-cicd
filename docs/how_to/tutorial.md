# My First Deployment

This step-by-step tutorial walks you through your first deployment with fabric-cicd — from installing the library to deploying Fabric items to a target workspace. By the end, you will have a working local deployment script that you can later integrate into a CI/CD pipeline.

## Prerequisites

Before you begin, make sure you have the following:

- **Python 3.9 or later** installed ([download](https://www.python.org/downloads/))
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

This also installs `azure-identity`, which you'll use for authentication.

## Step 3: Authenticate with Azure

fabric-cicd requires an explicit credential to interact with the Fabric APIs. For local development, the simplest approach is Azure CLI:

```bash
az login
```

This opens a browser where you sign in with the account that has access to your Fabric workspace. Once signed in, your session is cached and fabric-cicd can use it.

!!! note
The identity you sign in with must have **Contributor** (or higher) permissions on the target Fabric workspace.

## Step 4: Find Your Workspace ID

You need the ID of the **target workspace** — the workspace you want to deploy items _to_. This is different from the development workspace connected to Git.

To find it, open the target workspace in the [Fabric portal](https://app.fabric.microsoft.com). The workspace ID is in the URL:

```
https://app.fabric.microsoft.com/groups/<workspace-id>/list
```

Copy this ID — you'll use it in the next step.

## Step 5: Write the Deployment Script

Create a file called `deploy.py` in your repository root:

```python
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# Authenticate using Azure CLI
token_credential = AzureCliCredential()

# Path to the directory containing your Fabric items
repository_directory = str(Path(__file__).resolve().parent / "your-workspace-directory")

# Initialize the workspace connection
target_workspace = FabricWorkspace(
    workspace_id="your-workspace-id",       # Replace with your workspace ID from Step 4
    repository_directory=repository_directory,
    item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
    token_credential=token_credential,
)

# Deploy all in-scope items to the target workspace
publish_all_items(target_workspace)

# Remove items from the workspace that are no longer in the repository
unpublish_all_orphan_items(target_workspace)
```

Replace the placeholder values:

| Placeholder                | Replace with                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------ |
| `your-workspace-id`        | The workspace ID from Step 4                                                               |
| `your-workspace-directory` | The folder name containing your Fabric items                                               |
| `item_type_in_scope` list  | The item types you want to deploy (see [Supported Item Types](../reference/item_types.md)) |

!!! tip
The `item_type_in_scope` parameter controls which item types are deployed. Only items matching these types will be published or unpublished. Start with a small set and expand as needed.

## Step 6: Run the Deployment

Execute your script:

```bash
python deploy.py
```

You should see log output indicating each item being published to the target workspace. A successful run looks something like:

```
INFO - Publishing Hello World.Notebook...
INFO - Publishing Run Hello World.DataPipeline...
INFO - Unpublish orphan check complete.
```

!!! warning
If you see a `CredentialUnavailableError`, run `az login` again — your session may have expired.

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

Then add the `environment` parameter to your `FabricWorkspace`:

```python
target_workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    environment="PPE",  # Matches the key in parameter.yml
    repository_directory=repository_directory,
    item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
    token_credential=token_credential,
)
```

During deployment, any occurrence of `dev-lakehouse-id` in your item definitions will be replaced with `ppe-lakehouse-id`.

For full details, see the [Parameterization](parameterization.md) guide.

## What's Next?

Now that you have a working local deployment, here are the recommended next steps:

- **Automate with CI/CD** — Integrate your script into a [release pipeline](../example/release_pipeline.md) using GitHub Actions or Azure DevOps
- **Add parameterization** — Configure environment-specific values with [parameter.yml](parameterization.md)
- **Understand the philosophy** — Read about [full deployments vs. diffs](deployment_philosophy.md) to understand how fabric-cicd manages workspace state
- **Explore authentication options** — Review [authentication examples](../example/authentication.md) for service principals, managed identities, and Fabric Notebooks
