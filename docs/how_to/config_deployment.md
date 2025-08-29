# Configuration File Deployment

## Overview

Configuration-based deployment provides an alternative way to manage the deployment of Fabric items across multiple environments. Instead of using the traditional approach of defining a workspace object with various parameters and then running the publish/unpublish functions, this approach centralizes all deployment settings in a single YAML configuration file and simplifies the deployment into one function call.

Configuration file location (supports any location in git repository):

```
C:/dev/workspace
    /HelloWorld.Notebook
        ...
    /GoodbyeWorld.Notebook
        ...
    /config.yml
```

Basic example of configuration-based deployment:

```python
from fabric_cicd import deploy_with_config

# Deploy using a config file
deploy_with_config(
    config_file_path="C:/dev/workspace/config.yml",
    environment="dev"
)
```

**IMPORTANT:** Configuration-based deployment is currently a preview feature and requires feature flags `enable_experimental_features` and `enable_config_deploy` to be set.

## Configuration File Structure

The configuration file includes several sections, with configurable settings for different aspects of the deployment process. **Note**: Single value or environment-mapped values are supported for any field.

### `core` Section (Required)

The core section defines the fundamental settings for the deployment, most importantly the **target workspace** and **repository directory**. Other optional settings can be configured within the `core` section, which include **item types in scope** and **parameter**.

```yaml
core:
    # Only one workspace identifier field is required
    workspace: <workspace_name>

    workspace_id: <workspace_id>

    # Required - path to the directory containing Fabric items
    repository_directory: <rel_or_abs_path_of_repo_dir>

    # Optional - specific item types to include in deployment
    item_types_in_scope:
        - <item_type_1>
        - <item_type_2>
        - <item_type..>

    # Optional - path to parameter file
    parameter: <rel_or_abs_path_of_param_file>
```

With environment mapping:

```yaml
core:
    # Only one workspace identifier field is required
    workspace:
        <env_1>: <env_1_workspace_name>
        <env..>: <env.._workspace_name>

    workspace_id:
        <env_1>: <env_1_workspace_id>
        <env..>: <env.._workspace_id>

    # Required - path to the directory containing Fabric items
    repository_directory:
        <env_1>: <rel_or_abs_path_of_repo_dir_1>
        <env..>: <rel_or_abs_path_of_repo_dir..>

    # Optional - specific item types to include in deployment
    item_types_in_scope:
        <env_1>:
            - <item_type_1>
            - <item_type..>
        <env..>:
            - <item_type_1>
            - <item_type..>

    # Optional - path to parameter file
    parameter:
        <env_1>: <rel_or_abs_path_of_param_file_1>
        <env..>: <rel_or_abs_path_of_param_file..>
```

<span class="md-h4-nonanchor">Required Fields:</span>

-   Workspace Identifier:
    -   `workspace_id` takes precedence over workspace name when both are provided.
    -   `workspace_id` must be a valid string GUID.
-   Repository Directory Path:
    -   Supports relative or absolute path.
    -   Relative path must be relative to the `config.yml` file location.

<span class="md-h4-nonanchor">Optional Fields:</span>

-   Item Types in Scope:
    -   If `item_types_in_scope` is not specified, all item types will be included.
    -   Item types must be provided as a list, use `-` or `[]` notation.
    -   Only accepts supported item types.
-   Parameter Path:
    -   Supports relative or absolute path.
    -   Relative path must be relative to the `config.yml` file location.

### `publish` Section (Optional)

Controls item publishing behavior with various optional settings to enable/disable publishing operations or selectively publish items.

```yaml
publish:
    # Optional - pattern to exclude items from publishing
    exclude_regex: <regex_pattern_string>

    # Optional - specific items to publish (requires feature flags)
    items_to_include:
        - <item_name.item_type_1>
        - <item_name.item_type..>

    # Optional - control publishing by environment
    skip: <bool_value>
```

With environment mapping:

```yaml
publish:
    # Optional - pattern to exclude items from publishing
    exclude_regex:
        <env_1>: <regex_pattern_string_1>
        <env..>: <regex_pattern_string..>

    # Optional - specific items to publish (requires feature flags)
    items_to_include:
        - <item_name.item_type_1>
        - <item_name.item_type..>

    # Optional - control publishing by environment
    skip:
        <env_1>: <bool_value>
        <env..>: <bool_value>
```

### `unpublish` Section (Optional)

Controls item unpublishing behavior with various optional settings to enable/disable unpublishing or selectively unpublish items.

```yaml
unpublish:
    # Optional - pattern to exclude items from unpublishing
    exclude_regex: <regex_pattern_string>

    # Optional - specific items to unpublish (requires feature flags)
    items_to_include:
        - <item_name.item_type_1>
        - <item_name.item_type..>

    # Optional - control unpublishing by environment
    skip: <bool_value>
```

With environment mapping:

```yaml
unpublish:
    # Optional - pattern to exclude items from unpublishing
    exclude_regex:
        <env_1>: <regex_pattern_string_1>
        <env..>: <regex_pattern_string..>

    # Optional - specific items to unpublish (requires feature flags)
    items_to_include:
        <env_1>:
            - <item_name.item_type_1>
        <env..>:
            - <item_name.item_type..>

    # Optional - control unpublishing by environment
    skip:
        <env_1>: <bool_value>
        <env..>: <bool_value>
```

**Warning:** While selective deployment is supported in `fabric-cicd` it is not recommended due to potential issues with dependency management.

### `features` Section (Optional)

Set a list of specific feature flags.

```yaml
features:
    - <feature_flag_1>
    - <feature_flag..>
```

With environment mapping:

```yaml
features:
    <env_1>:
        - <feature_flag_1>
    <env..>:
        - <feature_flag..>
```

### `constants` Section (Optional)

Override supported library constants.

```yaml
constants:
    CONSTANT_NAME: <constant_value>
```

With environment mapping:

```yaml
constants:
    CONSTANT_NAME:
        <env_1>: <constant_value_1>
        <env..>: <constant_value..>
```

## Sample Configuration File

```yaml
core:
    workspace:
        dev: "Fabric-Dev-Engineering"
        test: "Fabric-Test-Engineering"
        prod: "Fabric-Prod-Engineering"

    workspace_id:
        dev: "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b"
        test: "2f4b9e8d-1a7c-4d3e-b8e2-5c9f7a2d4e1b"
        prod: "7c3e1f8b-2d4a-4b9e-8f2c-1a6c3b7d8e2f"

    repository_directory: "." # relative path

    item_types_in_scope:
        - Notebook
        - DataPipeline
        - Environment
        - Lakehouse

    parameter: "parameter.yml" # relative path

publish:
    # Don't publish items matching this pattern
    exclude_regex: "^DONT_DEPLOY.*"

    items_to_include:
        - "Hello World.Notebook"
        - "Run Hello World.DataPipeline"

    skip:
        dev: true
        test: false
        prod: false

unpublish:
    # Don't unpublish items matching this pattern
    exclude_regex: "^DEBUG.*"

    skip:
        dev: false
        test: false
        prod: true

features:
    - enable_shortcut_publish
    - enable_experimental_features
    - enable_items_to_include

constants:
    DEFAULT_API_ROOT_URL: "https://api.fabric.microsoft.com"
```

## Configuration Override

The `config_override` parameter in `deploy_with_config()` allows you to dynamically modify configuration values at runtime without changing the base configuration file. This is particularly useful for debugging or temporary deployment adjustments.

```python
from fabric_cicd import deploy_with_config

config_override_dict = {
    "core": {
        "item_types_in_scope": ["Notebook", "DataPipeline"]
    },
    "publish": {
        "skip": {
            "dev": False
        }
    }
}

# Deploy with configuration override
deploy_with_config(
    config_file_path="path/to/config.yml",
    environment="dev",
    config_override=config_override_dict
)
```

Important Considerations:

-   **Caution:** Exercise caution when overriding configuration values for _production_ environments.
-   **Support:** Configuration overrides are supported for all sections and settings in the configuration file.
-   **Rules:**
    -   Existing values can be overridden for any field in the configuration.
    -   New values can only be added for optional fields that aren't present in the original configuration.
    -   Required fields must exist in the original configuration in order to override.

## Applying Config File Deployment

### Basic Usage

```python
from fabric_cicd import deploy_with_config

# Deploy using a config file
deploy_with_config(
    config_file_path="path/to/config.yml", # required
    environment="dev" # optional (recommended)
)
```

### With Custom Authentication

```python
from fabric_cicd import deploy_with_config
from azure.identity import ClientSecretCredential

# Create a credential
credential = ClientSecretCredential(
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret"
)

# Deploy with custom credential
deploy_with_config(
    config_file_path="path/to/config.yml",
    environment="prod",
    token_credential=credential
)
```

### With Configuration Override

You can override specific configuration values at runtime:

```python
from fabric_cicd import deploy_with_config

config_override_dict = {
    "core": {
        "item_types_in_scope": ["Notebook", "DataPipeline"]
    },
    "publish": {
        "skip": {
            "test": False  # Override to enable publishing
        }
    }
}

# Deploy with configuration override
deploy_with_config(
    config_file_path="path/to/config.yml",
    environment="test",
    config_override=config_override_dict
)
```

## Troubleshooting Guide

The configuration file undergoes validation prior to reaching the deployment phase. Please note some common issues that may occur:

1. **File Not Found**: Ensure the configuration file path is correct and accessible (must be an absolute path).

2. **Invalid YAML**: Check YAML syntax for errors (indentation, missing quotes, etc.).

3. **Missing Required Fields**: Ensure `core` section is present and contains the required fields (workspace identifier, repository directory path).

4. **Path Resolution Errors**: Relative paths are resolved relative to the `config.yml` file location. Check path inputs are valid and accessible.

5. **Environment Not Found**: The `environment` parameter must match one of the environment keys (like "dev", "test", "prod") used in the configuration mappings.
