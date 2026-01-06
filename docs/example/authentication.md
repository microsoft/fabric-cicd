# Authentication Examples

The following are the most common authentication flows for fabric-cicd. However, because fabric-cicd supports any [TokenCredential](https://learn.microsoft.com/en-us/dotnet/api/azure.core.tokencredential), there are multiple authentication methods available beyond the ones described here.

## Default Credential

This approach utilizes the default credential flow, meaning no explicit TokenCredential is provided. It is the most common authentication method and is particularly useful with deployments where authentication is defined outside of this execution.

=== "Local"

    ```python
    '''Log in with Azure CLI (az login) or Azure PowerShell (Connect-AzAccount) prior to execution'''

    from pathlib import Path

    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    # Sample values for FabricWorkspace parameters
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

=== "Azure DevOps"

    ```python
    '''
    Log in with Azure CLI (az login) or Azure PowerShell (Connect-AzAccount) prior to execution
    OR (Preferred) Use Az CLI or AzPowerShell ADO Tasks with a Service Connection
    '''

    import sys
    import os
    from pathlib import Path

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

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

=== "GitHub"

    ```python
    '''Unconfirmed example at this time, however, the Azure DevOps example is a good starting point'''
    ```

=== "Fabric Notebook"

    ```python
    '''fabric-cicd will automatically generate a TokenCredential based on the user session context.'''

    from pathlib import Path

    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    # Sample values for FabricWorkspace parameters
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

## CLI Credential

This approach utilizes the CLI credential flow, meaning it only refers to the authentication established with az login. This is agnostic of the executing user, it can be UPN, SPN, Managed Identity, etc. Whatever is used to log in will be used.

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
    '''Unconfirmed example at this time, however, the Azure DevOps example is a good starting point'''
    ```

=== "Fabric Notebook"

    ```python
    '''Unconfirmed example at this time, Default Credential flow is recommended'''
    ```

## AZ PowerShell Credential

This approach utilizes the AZ PowerShell credential flow, meaning it only refers to the authentication established with Connect-AzAccount. This is agnostic of the executing user, it can be UPN, SPN, Managed Identity, etc. Whatever is used to log in will be used.

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

    # Use Azure CLI credential to authenticate
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

    # Use Azure CLI credential to authenticate
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
    Unconfirmed example at this time, however, the Azure DevOps example is a good starting point
    '''
    ```

=== "Fabric Notebook"

    ```python
    '''Unconfirmed example at this time, Default Credential flow is recommended'''
    ```

## Explicit SPN Secret Credential

This approach utilizes directly passing in SPN Client Id and Client Secret. Although you can pass in directly, it's not recommended and should store this outside of your code. It's important to consider that SPN + Secret is still possible to leverage in the above AZ PowerShell and AZ CLI flows

=== "Local"

    ```python
    '''Pass the required SPN values directly into the credential object, does not require AZ PowerShell or AZ CLI'''

    from pathlib import Path

    from azure.identity import ClientSecretCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    # Sample values for FabricWorkspace parameters
    workspace_id = "your-workspace-id"
    environment = "your-environment"
    repository_directory = str(root_directory / "your-workspace-directory")
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

    # Use Azure CLI credential to authenticate
    client_id = "your-client-id"
    client_secret = "your-client-secret"
    tenant_id = "your-tenant-id"
    token_credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

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
    Pass the required SPN values directly into the credential object
    OR Store the SPN Secret in Key Vault and reference key vault in Python
    OR Store the SPN Secret in Key Vault, link key vault to ADO variable group, and reference variable group environment variable in Python
    OR (Preferred) Use AZ CLI or AZ PowerShell task and leverage Service Connection (defined above)

    '''

    import sys
    import os
    from pathlib import Path

    from azure.identity import ClientSecretCredential
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
    client_id = "your-client-id"
    client_secret = "your-client-secret"
    tenant_id = "your-tenant-id"
    token_credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

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
    '''Unconfirmed example at this time, however, the Azure DevOps example is a good starting point'''
    ```

=== "Fabric Notebook"

    ```python
    '''Unconfirmed example at this time, Default Credential flow is recommended'''
    ```
