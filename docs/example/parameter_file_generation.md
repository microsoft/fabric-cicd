# Parameter File Generation for Deployment

This document explains the process of generating a `parameter.yml` file dynamically as part of the deployment workflow. The `parameter.yml` file is created using the `generate_parameter_yml_from_variable_library` function, which extracts environment-specific variables from the Variable Library.

## Creating the `.deploy` and `.pipeline` Folders

### `.deploy` Folder

The `.deploy` folder contains the deployment scripts, including:

-   `local_default_auth.py`: Handles local deployments.
-   `workspace_deploy.py`: Handles pipeline deployments.

### `.pipeline` Folder

The `.pipeline` folder contains the Azure DevOps pipeline definition file:

-   `deploy.yml`: Defines the pipeline stages and steps for deploying the workspace.

## Folder Structure

The following folders and files are involved in the process:

```
fabric-cicd/
├── .deploy/                      # Deployment scripts
│   ├── local_default_auth.py     # Local deployment script
│   ├── workspace_deploy.py       # Pipeline deployment script
├── .pipeline/                    # Pipeline definitions
│   └── deploy.yml                # Azure DevOps pipeline
└── sample/
    └── workspace/
        └── [workspace]/
            ├── parameter.yml     # Generated parameter file
            └── vl_[workspace].VariableLibrary/
                ├── variables.json     # Base variables (find values)
                └── valueSets/
                    ├── dev.json       # Dev environment values
                    └── prod.json      # Prod environment values
```

## Function: `generate_parameter_yml_from_variable_library`

The `generate_parameter_yml_from_variable_library` function is responsible for creating the `parameter.yml` file. It performs the following steps:

1. Locates the Variable Library for the specified workspace.
2. Reads base (default) variables from `variables.json`.
3. Reads environment-specific values from `valueSets/{environment}.json`.
4. Generates a properly formatted `parameter.yml` file.
5. Returns the path to the parameter file and workspace ID.

## Default Credential

This approach utilizes the default credential flow, meaning no explicit TokenCredential is provided. It is the most common authentication method and is particularly useful with deployments where authentication is defined outside of this execution.

=== "Local"

    ```python
    '''Log in with Azure CLI (az login) or Azure PowerShell (Connect-AzAccount) prior to execution'''
    import sys
    import json
    from pathlib import Path

    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, append_feature_flag

    def generate_parameter_yml_from_variable_library(repo_root, workspace, environment):
        """Generate parameter.yml file from Variable Library files and return workspace ID.

        Args:
            repo_root (Path): Root folder of the repository
            workspace (str): Workspace name (orchestrate, engineer, insight, etc.)
            environment (str): Deployment environment (dev, prod, etc.)

        Returns:
            tuple: Path to parameter file and workspace ID
        """
        var_lib_path = repo_root / "sample" / "workspace" / workspace / f"vl_{workspace}.VariableLibrary"
        param_path = repo_root / "sample" / "workspace" / workspace / "parameter.yml"

        # Print directory check paths for debugging
        print(f"Checking for Variable Library at: {var_lib_path}")
        print(f"Parameter file will be written to: {param_path}")
        print(f"Parameter file exists: {param_path.exists()}")

        # Check if Variable Library exists
        if not var_lib_path.exists():
            print(f"Variable Library not found at {var_lib_path}, skipping parameter.yml generation")
            return None, None

        # Check if variables.json exists
        variables_json_path = var_lib_path / "variables.json"
        print(f"Checking for variables.json at: {variables_json_path}")
        if not variables_json_path.exists():
            print(f"variables.json not found at {variables_json_path}, skipping parameter.yml generation")
            return None, None

        # Check if environment-specific valueSet exists
        env_file = var_lib_path / "valueSets" / f"{environment}.json"
        print(f"Checking for environment valueSet at: {env_file}")
        if not env_file.exists():
            print(f"Environment valueSet not found at {env_file}, skipping parameter.yml generation")
            return None, None

        print(f"Generating parameter.yml from Variable Library for workspace: {workspace}, environment: {environment}")

        # Load base variables (find values)
        with open(variables_json_path, 'r') as f:
            variables_data = json.load(f)

        # Load environment-specific values (replace values)
        with open(env_file, 'r') as f:
            env_data = json.load(f)

        # Create dictionaries for easier lookup
        var_dict = {}
        for i, var in enumerate(variables_data.get("variables", [])):
            if "value" in var:  # Value is required
                # If name is present, use it; otherwise use index as pseudo-name
                name = var.get("name", f"var_{i}")
                var_dict[name] = var["value"]

        env_dict = {}
        for i, override in enumerate(env_data.get("variableOverrides", [])):
            if "value" in override:  # Value is required
                # If name is present, use it; otherwise use index as pseudo-name
                name = override.get("name", f"var_{i}")
                env_dict[name] = override["value"]

        # Track workspace ID for the current environment
        workspace_id = None
        workspace_id_var_name = f"{workspace}_workspace_id"

        # Check if we have actual content to write
        if not var_dict:
            print("No variables with values found in variables.json, skipping parameter.yml generation")
            return None, None

        print(f"Found {len(var_dict)} variables to process")

        # Generate parameter.yml with precise formatting
        print(f"{'Creating new' if not param_path.exists() else 'Overwriting existing'} parameter.yml file")

        with open(param_path, 'w') as f:
            f.write("find_replace:\n")

            # Counter for find/replace pairs
            pair_count = 0

            # Generate find/replace pairs for all variables
            for name, find_value in var_dict.items():
                replace_value = env_dict.get(name, find_value)

                # Capture workspace ID for return - look for workspace specific ID
                if name == workspace_id_var_name and environment in ["dev", "prod"]:
                    workspace_id = replace_value

                # Only add if find_value exists
                if find_value:
                    # Add a blank line between entries (except first)
                    if pair_count > 0:
                        f.write("\n")

                    # Write the entry
                    f.write(f"    # {name}\n")
                    f.write(f'    - find_value: "{find_value}"\n')
                    f.write(f"      replace_value:\n")
                    f.write(f'        {environment}: "{replace_value}"\n')

                    pair_count += 1

        if pair_count == 0:
            print("No valid find/replace pairs generated. Check if variables have non-empty values.")
            # Remove empty parameter file if no pairs were written
            param_path.unlink(missing_ok=True)
            return None, None

        print(f"Generated parameter.yml with {pair_count} find/replace pairs")

        return param_path, workspace_id

    # Main script starts here

    # Set the environment
    environment = "dev"
    workspace = "engineer"

    repo_root = Path(__file__).resolve().parent.parent
    repository_directory = repo_root / "sample" / "workspace" / workspace

    print(f"Deploying {workspace} workspace")
    print(f"Computed repository_directory: {repository_directory}")

    # Generate parameter.yml from Variable Library and get workspace ID
    param_path, workspace_id = generate_parameter_yml_from_variable_library(repo_root, workspace, environment)

    # Fallback to environment-based workspace IDs if we couldn't get one from Variable Library
    if not workspace_id:
        print("No workspace ID found from Variable Library, using fallback method")
        # Fallback workspace IDs
        fallback_workspace_ids = {
            "engineer": {
                "dev": "00000000-0000-0000-0000-000000000000",
                "prod": ""
            }
        }

        try:
            workspace_id = fallback_workspace_ids[workspace][environment]
            print(f"Using fallback workspace ID: {workspace_id}")
        except KeyError:
            print(f"Error: No fallback workspace ID found for {workspace} in {environment} environment")
            sys.exit(1)

    # Workspace ID override
    # workspace_id = "00000000-0000-0000-0000-000000000000"

    print(f"Using workspace ID: {workspace_id} for {workspace} in {environment} environment")

    # Define item types to deploy based on workspace
    item_type_map = {
        "engineer": ["Notebook", "Lakehouse", "MirroredDatabase", "VariableLibrary"],
        "orchestrate": ["DataPipeline", "VariableLibrary"],
        "insight": ["SemanticModel", "Report", "VariableLibrary"],
    }

    # Get item types for the current workspace or fall back to Notebook
    item_type_in_scope = item_type_map.get(workspace)
    print(f"Item types in scope: {item_type_in_scope}")

    # Append feature flag to the workspace
    append_feature_flag("enable_shortcut_publish")

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)
    ```

## Azure CLI

This approach will work for both the Default Credential Flow and the Azure CLI Credential Flow. However, it is recommended to use the Azure CLI Credential Flow in case there are multiple identities present in the build VM.

=== "Azure DevOps"

    ```python
    import os
    import sys
    import json
    import argparse
    from pathlib import Path

    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, change_log_level, publish_all_items, unpublish_all_orphan_items, append_feature_flag

    def generate_parameter_yml_from_variable_library(repo_root, workspace, environment):
        """
        Generate new parameter.yml file from Variable Library files and return workspace ID.

        Args:
            repo_root (Path): Root folder of the repository
            workspace (str): Workspace name (orchestrate, engineer, insight, etc.)
            environment (str): Deployment environment (dev, prod, etc.)

        Returns:
            tuple: Path to parameter file and workspace ID
        """
        var_lib_path = repo_root / "sample" / "workspace" / workspace / f"vl_{workspace}.VariableLibrary"
        param_path = repo_root / "sample" / "workspace" / workspace / "parameter.yml"

        # Check if Variable Library exists
        if not var_lib_path.exists():
            print(f"Variable Library not found at {var_lib_path}, skipping parameter.yml generation")
            return None, None

        # Check if variables.json exists
        variables_json_path = var_lib_path / "variables.json"
        if not variables_json_path.exists():
            print(f"variables.json not found at {variables_json_path}, skipping parameter.yml generation")
            return None, None

        # Check if environment-specific valueSet exists
        env_file = var_lib_path / "valueSets" / f"{environment}.json"
        if not env_file.exists():
            print(f"Environment valueSet not found at {env_file}, skipping parameter.yml generation")
            return None, None

        print(f"Generating parameter.yml from Variable Library for workspace: {workspace}, environment: {environment}")

        # Load base variables (find values)
        with open(variables_json_path, 'r') as f:
            variables_data = json.load(f)

        # Load environment-specific values (replace values)
        with open(env_file, 'r') as f:
            env_data = json.load(f)

        # Create dictionaries for easier lookup
        var_dict = {}
        for i, var in enumerate(variables_data.get("variables", [])):
            if "value" in var:  # Value is required
                # If name is present, use it; otherwise use index as pseudo-name
                name = var.get("name", f"var_{i}")
                var_dict[name] = var["value"]

        env_dict = {}
        for i, override in enumerate(env_data.get("variableOverrides", [])):
            if "value" in override:  # Value is required
                # If name is present, use it; otherwise use index as pseudo-name
                name = override.get("name", f"var_{i}")
                env_dict[name] = override["value"]

        # Track workspace ID for the current environment
        workspace_id = None
        workspace_id_var_name = f"{workspace}_workspace_id"

        # Generate parameter.yml with precise formatting
        with open(param_path, 'w') as f:
            f.write("find_replace:\n")

            # Counter for find/replace pairs
            pair_count = 0

            # Generate find/replace pairs for all variables
            for name, find_value in var_dict.items():
                replace_value = env_dict.get(name, find_value)

                # Capture workspace ID for return - look for workspace specific ID
                if name == workspace_id_var_name and environment in ["dev", "prod"]:
                    workspace_id = replace_value

                # Only add if find_value exists
                if find_value:
                    # Add a blank line between entries (except first)
                    if pair_count > 0:
                        f.write("\n")

                    # Write the entry
                    f.write(f"    # {name}\n")
                    f.write(f'    - find_value: "{find_value}"\n')
                    f.write(f"      replace_value:\n")
                    f.write(f'        {environment}: "{replace_value}"\n')

                    pair_count += 1

        print(f"Generated parameter.yml with {pair_count} find/replace pairs")

        return param_path, workspace_id

    # Parse command line arguments (for local testing)
    parser = argparse.ArgumentParser(description='Deploy Fabric workspace')
    parser.add_argument('--workspace', default='orchestrate',
                        help='Workspace to deploy (orchestrate, engineer, insight, etc.)')
    args = parser.parse_args()

    # For pipeline, get workspace from environment variable or use default
    workspace = os.getenv("WORKSPACE", args.workspace)

    # Determine the environment based on branch name
    branch = os.getenv("BUILD_SOURCEBRANCHNAME")
    if branch == "main":
        environment = "dev"
    elif branch == "prod":
        environment = "prod"
    else:
        # Default to dev for local testing or unrecognized branches
        environment = "dev"
        print(f"Warning: Unrecognized branch '{branch}', defaulting to dev environment")

    # Force unbuffered output like `python -u`
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)

    # Enable debugging if defined in Azure DevOps pipeline
    if os.getenv("SYSTEM_DEBUG", "false").lower() == "true":
        change_log_level("DEBUG")

    # Use SYSTEM_TEAMFOUNDATIONCOLLECTIONURI if running in Azure DevOps, otherwise default to script's location
    if os.getenv("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI"):  # Running in Azure DevOps
        repo_root = Path(os.getenv("PIPELINE_WORKSPACE", "")) / "build"
    else:
        repo_root = Path(__file__).resolve().parent.parent

    repository_directory = repo_root / "sample" / "workspace" / workspace

    print(f"Deploying {workspace} workspace")
    print(f"Computed repo_root: {repo_root}")
    print(f"Computed repository_directory: {repository_directory}")

    # Generate parameter.yml from Variable Library and get workspace ID
    param_path, workspace_id = generate_parameter_yml_from_variable_library(repo_root, workspace, environment)

    # Fallback to environment-based workspace IDs if we couldn't get one from Variable Library
    if not workspace_id:
        print("No workspace ID found from Variable Library, using fallback method")
        # Fallback workspace IDs
        fallback_workspace_ids = {
            "engineer": {
                "dev": "00000000-0000-0000-0000-000000000000",
                "prod": "00000000-0000-0000-0000-000000000000"
            }
        }

        try:
            workspace_id = fallback_workspace_ids[workspace][environment]
            print(f"Using fallback workspace ID: {workspace_id}")
        except KeyError:
            print(f"Error: No fallback workspace ID found for {workspace} in {environment} environment")
            sys.exit(1)

    print(f"Using workspace ID: {workspace_id} for {workspace} in {environment} environment")

    # Use Azure CLI credential to authenticate
    token_credential = AzureCliCredential()

    # Define item types to deploy based on workspace
    item_type_map = {
        "engineer": ["Notebook", "Lakehouse", "MirroredDatabase", "VariableLibrary"],
        "orchestrate": ["DataPipeline", "VariableLibrary"],
        "insight": ["SemanticModel", "Report", "VariableLibrary"],
    }

    # Get item types for the current workspace or fall back to Notebook
    item_type_in_scope = item_type_map.get(workspace)
    print(f"Item types in scope: {item_type_in_scope}")

    # Append feature flag to the workspace
    append_feature_flag("enable_shortcut_publish")

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace, item_name_exclude_regex=r"^DEBUG.*")
    ```

## Modifications to `deploy.yml`

The `deploy.yml` file in the `.pipeline` folder has been updated to include parameterization of workspace selection, allowing for a single yaml pipeline to be used across multiple workpaces. Better observability has also been added for debugging:

=== "Azure DevOps"

    ```yaml
    parameters:
        - name: workspace
        displayName: Target Fabric Workspace
        type: string
        values:
            - orchestrate
            - engineer
            - insight
            - monitor
    trigger:
        branches:
            include:
                - main
                - prod
    stages:
        - stage: Build
        jobs:
            - job: Build
                pool:
                    vmImage: windows-latest
                steps:
                    - checkout: self
                    - task: PublishPipelineArtifact@1
                    inputs:
                        targetPath: $(System.DefaultWorkingDirectory)
                        artifact: build
                        publishLocation: pipeline
        - stage: Release
        dependsOn: Build
        jobs:
            - job: Release
                pool:
                    vmImage: windows-latest
                steps:
                    - checkout: none
                    - task: DownloadPipelineArtifact@2
                    inputs:
                        artifact: build
                        path: $(Pipeline.Workspace)/build
                    - task: UsePythonVersion@0
                    inputs:
                        versionSpec: "3.12"
                        addToPath: true
                    - script: |
                        pip install fabric-cicd
                    displayName: "Install fabric-cicd"
                    - task: AzureCLI@2
                    displayName: "Deploy Fabric Workspace"
                    inputs:
                        azureSubscription: "ldp_deploy_dev"
                        scriptType: "ps"
                        scriptLocation: "inlineScript"
                        inlineScript: |
                            $env:WORKSPACE="${{ parameters.workspace }}"
                            python -u $(Pipeline.Workspace)/build/.deploy/workspace_deploy.py
                    - task: PowerShell@2
                    displayName: "Display Error Log"
                    condition: failed()
                    inputs:
                        targetType: "inline"
                        script: |
                            if (Test-Path "D:\a\1\s\fabric_cicd.error.log") {
                                Write-Host "===== ERROR LOG CONTENT ====="
                                Get-Content "D:\a\1\s\fabric_cicd.error.log"
                                Write-Host "===== END OF ERROR LOG ======"
                            } else {
                                Write-Host "Error log file not found at D:\a\1\s\fabric_cicd.error.log"
                            }
    ```
