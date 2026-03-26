# Authentication Examples

The following are the most common authentication flows for fabric-cicd. However, because fabric-cicd supports any [TokenCredential](https://learn.microsoft.com/en-us/dotnet/api/azure.core.tokencredential), there are multiple authentication methods available beyond the ones described here. These examples provide starting points that should be adapted for your specific environment and security requirements.

> **⚠️ DEPRECATION NOTICE:** Due to security best practices, the **Default Credential** (`DefaultAzureCredential`) authentication method is deprecated and will be removed in a future release. Please migrate to one of the explicit credential methods described below.

**Notes:**

- Explicit `token_credential` parameter is used for all scenarios except Fabric Notebook runtime, which handles authentication automatically.
- Avoid hardcoding credentials. Use environment variables or secret management services. SPN + Secret auth can also be achieved via `az login --service-principal` or `Connect-AzAccount -ServicePrincipal` in the CLI/PowerShell flows below.

## CLI Credential

This approach utilizes the CLI credential flow, meaning it only refers to the authentication established with `az login`. This is agnostic of the executing user; it can be UPN, SPN, Managed Identity, etc. Whatever is used to log in will be used.

=== "Local"

    ```python
    '''Log in with Azure CLI (az login) prior to execution'''

    from pathlib import Path

    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    # Sample values for FabricWorkspace parameters
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use Azure CLI credential to authenticate
    token_credential = AzureCliCredential()

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

=== "Azure DevOps"

    ```python
    '''
    Log in with Azure CLI (az login) prior to execution
    OR (Preferred) Use Az CLI ADO Tasks with a Service Connection
    '''

    import sys
    import os
    from pathlib import Path

    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, change_log_level

    # Force unbuffered output like `python -u`
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)

    # Enable debugging if defined in Azure DevOps pipeline
    if os.getenv("SYSTEM_DEBUG", "false").lower() == "true":
        change_log_level("DEBUG")

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    # Sample values for FabricWorkspace parameters
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use Azure CLI credential to authenticate
    token_credential = AzureCliCredential()

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

=== "GitHub"

    ```python
    '''
    Log in with Azure CLI (az login) prior to execution
    Requires: azure/login workflow step in GitHub Actions
    '''

    import os
    from pathlib import Path
    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # GitHub Actions sets GITHUB_WORKSPACE automatically
    root_directory = Path(os.getenv("GITHUB_WORKSPACE", ".")).resolve()

    # Sample values for FabricWorkspace parameters
    workspace_id = os.getenv("WORKSPACE_ID")
    environment = os.getenv("ENVIRONMENT", "PROD")
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use Azure CLI credential (assumes 'az login' in workflow step)
    token_credential = AzureCliCredential()

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

## AZ PowerShell Credential

This approach utilizes the AZ PowerShell credential flow, meaning it only refers to the authentication established with `Connect-AzAccount`. This is agnostic of the executing user; it can be UPN, SPN, Managed Identity, etc. Whatever is used to log in will be used.

=== "Local"

    ```python
    '''Log in with Azure PowerShell (Connect-AzAccount) prior to execution'''

    from pathlib import Path

    from azure.identity import AzurePowerShellCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    # Sample values for FabricWorkspace parameters
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use Azure PowerShell credential to authenticate
    token_credential = AzurePowerShellCredential()

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

=== "Azure DevOps"

    ```python
    '''
    Log in with Azure PowerShell (Connect-AzAccount) prior to execution
    OR (Preferred) Use AzPowerShell ADO Tasks with a Service Connection
    '''

    import sys
    import os
    from pathlib import Path

    from azure.identity import AzurePowerShellCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, change_log_level

    # Force unbuffered output like `python -u`
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)

    # Enable debugging if defined in Azure DevOps pipeline
    if os.getenv("SYSTEM_DEBUG", "false").lower() == "true":
        change_log_level("DEBUG")

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    # Sample values for FabricWorkspace parameters
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use Azure PowerShell credential to authenticate
    token_credential = AzurePowerShellCredential()

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

=== "GitHub"

    ```python
    '''
    Log in with Azure PowerShell (Connect-AzAccount) prior to execution
    Requires: azure/powershell workflow step in GitHub Actions
    '''

    import os
    from pathlib import Path
    from azure.identity import AzurePowerShellCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # GitHub Actions sets GITHUB_WORKSPACE automatically
    root_directory = Path(os.getenv("GITHUB_WORKSPACE", ".")).resolve()

    # Sample values for FabricWorkspace parameters
    workspace_id = os.getenv("WORKSPACE_ID")
    environment = os.getenv("ENVIRONMENT", "PROD")
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use Azure PowerShell credential (assumes 'Connect-AzAccount' in workflow step)
    token_credential = AzurePowerShellCredential()

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

## Managed Identity Credential

This approach uses Azure Managed Identity, eliminating the need to manage secrets. Managed identities provide an automatically managed identity in Azure AD for applications to use when connecting to resources.

=== "Azure DevOps"

    ```python
    '''
    Running on Azure DevOps self-hosted agents with system-assigned managed identity
    OR Azure DevOps agents hosted on Azure VMs with managed identity
    '''

    import sys
    import os
    from pathlib import Path

    from azure.identity import ManagedIdentityCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, change_log_level

    # Force unbuffered output like `python -u`
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)

    # Enable debugging if defined in Azure DevOps pipeline
    if os.getenv("SYSTEM_DEBUG", "false").lower() == "true":
        change_log_level("DEBUG")

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    # Sample values for FabricWorkspace parameters
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use system-assigned managed identity
    token_credential = ManagedIdentityCredential()

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

=== "GitHub"

    ```python
    '''
    Running on GitHub self-hosted runners with system-assigned managed identity
    OR GitHub Actions hosted on Azure VMs with managed identity
    '''

    import os
    from pathlib import Path
    from azure.identity import ManagedIdentityCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # GitHub Actions sets GITHUB_WORKSPACE automatically
    root_directory = Path(os.getenv("GITHUB_WORKSPACE", ".")).resolve()

    # Sample values for FabricWorkspace parameters
    workspace_id = os.getenv("WORKSPACE_ID")
    environment = os.getenv("ENVIRONMENT", "PROD")
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use system-assigned managed identity
    token_credential = ManagedIdentityCredential()

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

## Fabric Notebook Authentication

When running fabric-cicd within Microsoft Fabric Notebooks, authentication is handled automatically through the user session context. No explicit `token_credential` parameter is required. Alternatively, if you want to use a different identity than the logged-in user, you can override the automatic authentication by providing an explicit credential.

=== "No Credential"

    ```python
    '''
    fabric-cicd automatically uses the Fabric Notebook session authentication
    Most common pattern: clone repository and deploy from within notebook
    '''

    import tempfile
    import subprocess
    import os
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Sample configuration values
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repo_url = "https://github.com/your-org/your-repo.git"
    repo_ref = "main"
    workspace_directory = "your-workspace-directory"

    # Use context manager for automatic cleanup (even on exceptions)
    with tempfile.TemporaryDirectory(prefix="cloned_repo_") as temp_dir:
        print(f"Created temporary directory: {temp_dir}")

        # Clone the repository
        print(f"Cloning {repo_url} (ref: {repo_ref})...")
        result = subprocess.run(
            ["git", "clone", "--branch", repo_ref, "--single-branch", repo_url, temp_dir],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise Exception(f"Git clone failed: {result.stderr}")

        workspace_root = os.path.join(temp_dir, workspace_directory)

        # Deploy workspace items from cloned repository
        item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

        # Initialize FabricWorkspace - no token_credential needed
        target_workspace = FabricWorkspace(
            workspace_id=workspace_id,
            environment=environment,
            repository_directory=workspace_root,
            item_type_in_scope=item_type_in_scope
        )

        # Publish all items defined in item_type_in_scope
        publish_all_items(target_workspace)

        # Unpublish all items defined in item_type_in_scope not found in repository
        unpublish_all_orphan_items(target_workspace)

    # Directory automatically cleaned up here
    print("Cleaned up temporary directory")
    ```

=== "Credential"

    ```python
    '''
    Override automatic authentication with explicit credential
    Only needed for specific identity requirements
    '''

    import tempfile
    import subprocess
    import os
    from azure.identity import ClientSecretCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Sample configuration values
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repo_url = "https://github.com/your-org/your-repo.git"
    repo_ref = "main"
    workspace_directory = "your-workspace-directory"

    # Use explicit SPN auth (overrides automatic authentication)
    # Retrieve secrets from Azure Key Vault using notebookutils
    key_vault_url = "https://your-keyvault.vault.azure.net/"
    client_id = notebookutils.credentials.getSecret(key_vault_url, "client-id")
    client_secret = notebookutils.credentials.getSecret(key_vault_url, "client-secret")
    tenant_id = notebookutils.credentials.getSecret(key_vault_url, "tenant-id")

    token_credential = ClientSecretCredential(
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id
    )

    # Use context manager for automatic cleanup (even on exceptions)
    with tempfile.TemporaryDirectory(prefix="cloned_repo_") as temp_dir:
        print(f"Created temporary directory: {temp_dir}")

        # Clone the repository
        print(f"Cloning {repo_url} (ref: {repo_ref})...")
        result = subprocess.run(
            ["git", "clone", "--branch", repo_ref, "--single-branch", repo_url, temp_dir],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise Exception(f"Git clone failed: {result.stderr}")

        workspace_root = os.path.join(temp_dir, workspace_directory)

        # Deploy workspace items from cloned repository
        item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

        # Initialize with explicit credential
        target_workspace = FabricWorkspace(
            workspace_id=workspace_id,
            environment=environment,
            repository_directory=workspace_root,
            item_type_in_scope=item_type_in_scope,
            token_credential=token_credential,
        )

        # Publish all items defined in item_type_in_scope
        publish_all_items(target_workspace)

        # Unpublish all items defined in item_type_in_scope not found in repository
        unpublish_all_orphan_items(target_workspace)

    # Directory automatically cleaned up here
    print("Cleaned up temporary directory")
    ```
