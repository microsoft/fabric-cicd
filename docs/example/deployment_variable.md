# Deployment Variable Examples

A key concept in CI/CD is defining environment-specific deployment variables. The following are examples on how to inject variables from outside of the python script to handle values that are environment specific, or common accross other tooling.

## Branch Based

Leverage the following when you have specific values that you need to define per branch you are deploying from.

=== "Local"

    ```python
    '''Leverages Default Credential Flow for authentication. Determines variables based on locally checked out branch.'''

    from pathlib import Path

    import git  # Depends on pip install gitpython
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Assumes your script is one level down from root
    root_directory = Path(__file__).resolve().parent

    repo = git.Repo(root_directory)
    repo.remotes.origin.pull()
    branch = repo.active_branch.name

    # The defined environment values should match the names found in the parameter.yml file
    if branch == "dev":
        workspace_id = "dev-workspace-id"
        environment = "DEV"
    elif branch == "main":
        workspace_id = "prod-workspace-id"
        environment = "PROD"
    else:
        raise ValueError("Invalid branch to deploy from")

    # Sample values for FabricWorkspace parameters
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
    '''Leverages Default Credential Flow for authentication. Determines variables based on the branch that originated the build.'''

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

    branch = os.getenv("BUILD_SOURCEBRANCHNAME")

    # The defined environment values should match the names found in the parameter.yml file
    if branch == "dev":
        workspace_id = "dev-workspace-id"
        environment = "DEV"
    elif branch == "main":
        workspace_id = "prod-workspace-id"
        environment = "PROD"
    else:
        raise ValueError("Invalid branch to deploy from")

    # Sample values for FabricWorkspace parameters
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

## Passed Arguments

Leverage the following when you want to pass in variables outside of the python script. This is most common for scenarios where you want to use one py script, but have multiple deployments.

=== "Local"

    ```python
    '''Leverages Default Credential Flow for authentication. Accepts parameters passed into Python during execution.'''

    import argparse

    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Accept parsed arguments
    parser = argparse.ArgumentParser(description='Process Azure Pipeline arguments.')
    parser.add_argument('--workspace_id', type=str)
    parser.add_argument('--environment', type=str)
    parser.add_argument('--repository_directory', type=str)
    parser.add_argument('--items_in_scope', type=str)
    args = parser.parse_args()

    # Sample values for FabricWorkspace parameters
    workspace_id = args.workspace_id
    environment = args.environment
    repository_directory = args.repository_directory
    item_type_in_scope = args.items_in_scope.split(",")

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
    '''Leverages Default Credential Flow for authentication. Accepts parameters passed into Python during execution.'''

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

    # Accept parsed arguments
    parser = argparse.ArgumentParser(description='Process Azure Pipeline arguments.')
    parser.add_argument('--workspace_id', type=str)
    parser.add_argument('--environment', type=str)
    parser.add_argument('--repository_directory', type=str)
    parser.add_argument('--items_in_scope', type=str)
    args = parser.parse_args()

    # Sample values for FabricWorkspace parameters
    workspace_id = args.workspace_id
    environment = args.environment
    repository_directory = args.repository_directory
    item_type_in_scope = args.items_in_scope.split(",")

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
