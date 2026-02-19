# Configuration Deployment

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
    config_file_path="C:/dev/workspace/config.yml", # required
    environment="dev"
)
```

Raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml) for additional capabilities or a [bug report](https://github.com/microsoft/fabric-cicd/issues/new?template=1-bug.yml) for issues.

## Configuration File Setup

The configuration file includes several sections with configurable settings for different aspects of the deployment process.

**Note**: Configuration values can be specified in two ways: as a single value (applied to any target environment provided) or as an environment mapping. Both approaches can be used within the same configuration file - for example, using environment mappings for workspace IDs while keeping a single value for repository directory.

### Core Settings

The `core` section is **required** as it defines the fundamental settings for the deployment, most importantly the **target workspace** and **repository directory**. Other optional settings can be configured within the `core` section, which include **item types in scope** and **parameter**.

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

<span class="md-h4-nonanchor">With environment mapping:</span>

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

- Workspace Identifier:
    - Workspace ID takes precedence over workspace name when both are provided.
    - `workspace_id` must be a valid string GUID.
- Repository Directory Path:
    - Supports relative or absolute path.
    - Relative path must be relative to the `config.yml` file location.

<span class="md-h4-nonanchor">Optional Fields:</span>

- Item Types in Scope:
    - If `item_types_in_scope` is not specified, all item types will be included by default.
    - Item types must be provided as a list, use `-` or `[]` notation.
    - Only accepts supported item types.
- Parameter Path:
    - Supports relative or absolute path.
    - Relative path must be relative to the `config.yml` file location.

### Publish Settings

`publish` is optional and can be used to control item publishing behavior. It includes various optional settings to enable/disable publishing operations or selectively publish items.

**Note:** Folder-level filtering only applies to items within a Fabric folder. Folder paths must start with `/` (e.g., `/folder_name` or `/folder_name/nested_folder`). `folder_exclude_regex` and `folder_path_to_include` are **mutually exclusive** — providing both for the same environment will result in a validation error.

When using `folder_exclude_regex`, the pattern is matched using `search()` (substring match), so a pattern like `subfolder1` will match any folder path containing "subfolder1" (e.g., `/subfolder1`, `/subfolder1/subfolder2`, `/other/subfolder1`). To target a specific folder, use an anchored pattern with a leading `/` (e.g., `^/subfolder1$`) — this ensures only the exact folder path matches directly. Note that child folders like `/subfolder1/subfolder2` will also be excluded automatically since their parent folder was excluded, preserving a consistent folder hierarchy.

When using `folder_path_to_include` with nested paths (e.g., `/subfolder1/subfolder2`), ancestor folders (e.g., `/subfolder1`) are automatically created to preserve the correct folder hierarchy, but items directly under the ancestor folder are **not** published unless the ancestor folder is also explicitly included in the list.

```yaml
publish:
    # Optional - pattern to exclude items from publishing
    exclude_regex: <regex_pattern_string>

    # Optional - pattern to exclude specific folder paths with items from publishing (requires feature flags)
    folder_exclude_regex: <regex_pattern_string>

    # Optional - specific folder paths with items to publish (requires feature flags)
    folder_path_to_include:
        - </subfolder_1>
        - </subfolder_2>
        - </subfolder_2/subfolder_3> # publish items found in nested folder - subfolder_3

    # Optional - specific items to publish (requires feature flags)
    items_to_include:
        - <item_name.item_type_1>
        - <item_name.item_type..>

    # Optional - pattern to exclude Lakehouse shortcuts from publishing (requires feature flags)
    shortcut_exclude_regex: <regex_pattern_string>

    # Optional - control publishing by environment
    skip: <bool_value>
```

<span class="md-h4-nonanchor">With environment mapping:</span>

```yaml
publish:
    # Optional - pattern to exclude items from publishing
    exclude_regex:
        <env_1>: <regex_pattern_string_1>
        <env..>: <regex_pattern_string..>

    # Optional - pattern to exclude specific folder paths with items from publishing (requires feature flags)
    folder_exclude_regex:
        <env_1>: <regex_pattern_string_1>
        <env..>: <regex_pattern_string..>

    # Optional - specific folder paths with items to publish (requires feature flags)
    folder_path_to_include:
        <env_1>:
            - </subfolder_1>
            - </subfolder_2/subfolder_3>
        <env..>:
            - </subfolder_1>

    # Optional - specific items to publish (requires feature flags)
    items_to_include:
        <env_1>:
            - <item_name.item_type_1>
            - <item_name.item_type..>
        <env..>:
            - <item_name.item_type_1>
            - <item_name.item_type..>

    # Optional - pattern to exclude Lakehouse Shortcuts from publishing (requires feature flags)
    shortcut_exclude_regex:
        <env_1>: <regex_pattern_string_1>
        <env..>: <regex_pattern_string..>

    # Optional - control publishing by environment
    skip:
        <env_1>: <bool_value>
        <env..>: <bool_value>
```

### Unpublish Settings

`unpublish` is optional and can be used to control item unpublishing behavior. It includes various optional settings to enable/disable unpublishing or selectively unpublish items.

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

<span class="md-h4-nonanchor">With environment mapping:</span>

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
            - <item_name.item_type..>
        <env..>:
            - <item_name.item_type_1>
            - <item_name.item_type..>

    # Optional - control unpublishing by environment
    skip:
        <env_1>: <bool_value>
        <env..>: <bool_value>
```

**Warning:** While selective deployment is supported in `fabric-cicd` it is not recommended due to potential issues with dependency management.

### Features Setting

`features` is optional and can be used to set a list of specific feature flags.

```yaml
features:
    - <feature_flag_1>
    - <feature_flag..>
```

<span class="md-h4-nonanchor">With environment mapping:</span>

```yaml
features:
    <env_1>:
        - <feature_flag_1>
        - <feature_flag..>
    <env..>:
        - <feature_flag_1>
        - <feature_flag..>
```

### Constants Setting

`constants` is optional and can be used to override supported library constants.

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

## Environment-Specific Values

All configuration fields support environment-specific values using a mapping format:

```yaml
core:
    workspace_id:
        dev: "dev-workspace-id"
        test: "test-workspace-id"
        prod: "prod-workspace-id"
```

### Required vs Optional Fields

Fields are categorized as **required** or **optional**, which affects how missing environment values are handled when environment is passed into `deploy_with_config()`:

| Field                                   | Required | Environment Missing Behavior    |
| --------------------------------------- | -------- | ------------------------------- |
| `core.workspace_id` or `core.workspace` | ✅       | Validation error                |
| `core.repository_directory`             | ✅       | Validation error                |
| `core.item_types_in_scope`              | ❌       | Warning logged, setting skipped |
| `core.parameter`                        | ❌       | Warning logged, setting skipped |
| `publish.exclude_regex`                 | ❌       | Debug logged, setting skipped   |
| `publish.folder_exclude_regex`          | ❌       | Debug logged, setting skipped   |
| `publish.shortcut_exclude_regex`        | ❌       | Debug logged, setting skipped   |
| `publish.folder_path_to_include`        | ❌       | Debug logged, setting skipped   |
| `publish.items_to_include`              | ❌       | Debug logged, setting skipped   |
| `publish.skip`                          | ❌       | Defaults to `False`             |
| `unpublish.exclude_regex`               | ❌       | Debug logged, setting skipped   |
| `unpublish.items_to_include`            | ❌       | Debug logged, setting skipped   |
| `unpublish.skip`                        | ❌       | Defaults to `False`             |
| `features`                              | ❌       | Debug logged, setting skipped   |
| `constants`                             | ❌       | Debug logged, setting skipped   |

### Selective Environment Configuration

Optional fields allow you to apply settings to specific environments without affecting others. This is useful when you want different behavior per environment:

```yaml
core:
    workspace_id:
        dev: "dev-workspace-id"
        test: "test-workspace-id"
        prod: "prod-workspace-id"
    repository_directory: "./workspace" # Same for all environments

publish:
    # Only exclude legacy folders in prod environment
    folder_exclude_regex:
        prod: "^/legacy_.*"
        # dev and test not specified - no folder exclusion applied

    # Skip publish in dev, run in test and prod
    skip:
        dev: true
        # test and prod default to false
```

In this example:

- Deploying to `dev`: No folder exclusion applied, `skip` = `true`
- Deploying to `test`: No folder exclusion applied, `skip` = `false`
- Deploying to `prod`: `folder_exclude_regex` = `"^/legacy_.*"`, `skip` = `false`

### Logging Behavior

When an optional field uses environment mapping and does not include the target environment:

- **Important optional fields** (`item_types_in_scope`, `parameter`): A **warning** is logged to alert users that the setting is being skipped.
- **Other optional fields**: A **debug** message is logged, visible only when debug logging is enabled.

Example log output when deploying to `prod` with the config above:

```
[Debug] - No value for 'folder_exclude_regex' in environment 'prod'. Available environments: ['dev']. This setting will be skipped.
```

To enable debug logging:

```python
change_log_level()
```

## Sample `config.yml` File

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

    # Use folder_exclude_regex OR folder_path_to_include, not both for the same environment
    folder_exclude_regex:
        dev: "^/DONT_DEPLOY_FOLDER"

    folder_path_to_include:
        prod:
            - "/DEPLOY_FOLDER"
            - "/DEPLOY_FOLDER/DEPLOY_NESTED_FOLDER"

    items_to_include:
        - "Hello World.Notebook"
        - "Run Hello World.DataPipeline"

    shortcut_exclude_regex:
        test: "^temp_.*"

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
    - enable_exclude_folder
    - enable_include_folder
    - enable_shortcut_exclude

constants:
    DEFAULT_API_ROOT_URL: "https://api.fabric.microsoft.com"
```

## Configuration File Deployment

### Basic Usage

```python
from fabric_cicd import deploy_with_config

# Deploy using a config file
deploy_with_config(
    config_file_path="path/to/config.yml", # required
    environment="dev" # optional (recommended)
)
```

### Custom Authentication

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

### Configuration Override

The `config_override` parameter in `deploy_with_config()` allows you to dynamically modify configuration values at runtime without changing the base configuration file. This is particularly useful for debugging or making temporary deployment adjustments.

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

**Important Considerations:**

- **Caution:** Exercise caution when overriding configuration values for _production_ environments.
- **Support:** Configuration overrides are supported for all sections and settings in the configuration file.
- **Rules:**
    - Existing values can be overridden for any field in the configuration.
    - New values can only be added for optional fields that aren't present in the original configuration.
    - Required fields must exist in the original configuration in order to override.

## Troubleshooting Guide

The configuration file undergoes validation prior to reaching the deployment phase. Please note some common issues that may occur:

**1. File Not Found**: Ensure the configuration file path is correct and accessible (must be an absolute path).

**2. Invalid YAML**: Check YAML syntax for errors (indentation, missing quotes, etc.).

**3. Missing Required Fields**: Ensure `core` section is present and contains the required fields (workspace identifier, repository directory path).

**4. Path Resolution Errors**: Relative paths are resolved relative to the `config.yml` file location. Check path inputs are valid and accessible.

**5. Environment Not Found**: The `environment` parameter must match one of the environment keys (like "dev", "test", "prod") used in the configuration mappings.
