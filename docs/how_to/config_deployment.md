# Configuration File Deployment

## Overview

Configuration-based deployment provides a simplified, consistent way to manage Microsoft Fabric deployments across multiple environments. Instead of writing custom deployment scripts for each environment, you can define all your deployment settings in a single YAML configuration file that works across your development, testing, and production environments.

This approach allows you to:

-   Define environment-specific workspace settings
-   Control which items are published and unpublished
-   Configure feature flags and constants
-   Apply environment-specific parameter substitutions
-   Maintain consistent deployment processes across environments

## Configuration File Structure

The configuration file is organized into several sections, each controlling different aspects of the deployment process:

### Required Sections

#### `core` Section (Required)

The core section defines the fundamental settings for your deployment:

```yaml
core:
    # At least one of these is required (workspace_id takes precedence if both are present)
    workspace:
        dev: "Fabric-Dev-Workspace"
        test: "Fabric-Test-Workspace"
        prod: "Fabric-Prod-Workspace"

    workspace_id:
        dev: "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b"
        test: "2f4b9e8d-1a7c-4d3e-b8e2-5c9f7a2d4e1b"
        prod: "7c3e1f8b-2d4a-4b9e-8f2c-1a6c3b7d8e2f"

    # Required - path to the directory containing your Fabric items
    repository_directory: "./workspace"
```

Required fields:

-   Either `workspace` or `workspace_id` (environment-mapped)
-   `repository_directory`: Path to the directory containing your Fabric items

### Optional Sections

#### Additional `core` Options (Optional)

```yaml
core:
    # ... required fields ...

    # Optional - specific item types to include in deployment
    item_types_in_scope:
        - Notebook
        - DataPipeline
        - Environment
        - Lakehouse

    # Optional - path to parameter file for substitutions
    parameter: "parameter.yml"
```

#### `publish` Section (Optional)

Controls item publishing behavior:

```yaml
publish:
    # Optional - pattern to exclude items from publishing
    exclude_regex: "^DONT_DEPLOY.*"

    # Optional - specific items to publish (requires feature flags)
    items_to_include:
        - "Hello World.Notebook"
        - "Run Pipeline.DataPipeline"

    # Optional - control publishing by environment
    skip:
        dev: true # Skip publishing in dev
        test: false # Enable publishing in test
        prod: false # Enable publishing in prod
```

#### `unpublish` Section (Optional)

Controls orphan item unpublishing behavior:

```yaml
unpublish:
    # Optional - pattern to exclude items from unpublishing
    exclude_regex: "^DEBUG.*"

    # Optional - specific items to unpublish (requires feature flags)
    items_to_include:
        - "Old Item.Notebook"

    # Optional - control unpublishing by environment
    skip:
        dev: true # Skip unpublishing in dev
        test: false # Enable unpublishing in test
        prod: false # Enable unpublishing in prod
```

#### `features` Section (Optional)

Enable specific feature flags:

```yaml
features:
    - enable_shortcut_publish
    - enable_parameter_environment_variables
```

#### `constants` Section (Optional)

Override library constants:

```yaml
constants:
    DEFAULT_API_ROOT_URL: "https://msitapi.fabric.microsoft.com"
    LAKEHOUSE_WAIT_TIMEOUT: 300
```

## Environment Mapping

The configuration file supports two approaches for environment-specific settings:

### 1. Environment Mapping (Recommended)

Define values for each environment:

```yaml
workspace_id:
    dev: "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b"
    test: "2f4b9e8d-1a7c-4d3e-b8e2-5c9f7a2d4e1b"
    prod: "7c3e1f8b-2d4a-4b9e-8f2c-1a6c3b7d8e2f"
```

### 2. Single Value (No Mapping)

Use the same value for all environments:

```yaml
repository_directory: "./workspace"
```

## Path Handling

The configuration file supports both absolute and relative paths:

### Absolute Paths

Full paths to resources:

```yaml
repository_directory: "C:/Projects/MyFabricProject/workspace"
parameter: "C:/Projects/MyFabricProject/parameters/dev-params.yml"
```

### Relative Paths

Paths relative to the configuration file location:

```yaml
repository_directory: "./workspace"
parameter: "./parameters/dev-params.yml"
```

## Complete Configuration Example

```yaml
# config.yml - Fabric CICD Deployment Configuration
core:
    # Workspace information by environment
    workspace:
        dev: "Fabric-Dev-Engineering"
        test: "Fabric-Test-Engineering"
        prod: "Fabric-Prod-Engineering"

    # Repository and item scope
    repository_directory: "./workspace"
    item_types_in_scope:
        - Notebook
        - DataPipeline
        - Environment
        - Lakehouse
        - SQLDatabase

    # Parameter file for substitutions
    parameter: "parameter.yml"

publish:
    # Exclude certain items from publishing
    exclude_regex: "^DRAFT_|^WIP_"

    # Environment-specific publishing settings
    skip:
        dev: false
        test: false
        prod: false

unpublish:
    # Don't unpublish items matching this pattern
    exclude_regex: "^KEEP_|^LOCKED_"

    # Environment-specific unpublishing settings
    skip:
        dev: false
        test: true # Never unpublish in test
        prod: true # Never unpublish in prod

features:
    - enable_shortcut_publish
    - enable_parameter_environment_variables

constants:
    LAKEHOUSE_WAIT_TIMEOUT: 300
```

## Using the Config File for Deployment

### Basic Usage

```python
from fabric_cicd import deploy_with_config

# Deploy using a config file
deploy_with_config(
    config_file_path="path/to/config.yml",
    environment="dev"
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

# Deploy with configuration override
deploy_with_config(
    config_file_path="path/to/config.yml",
    environment="test",
    config_override={
        "core": {
            "item_types_in_scope": ["Notebook", "DataPipeline"]
        },
        "publish": {
            "skip": {
                "test": False  # Override to enable publishing
            }
        }
    }
)
```

## Benefits of Config-Based Deployment

1. **Environment Consistency**: Use the same configuration file across all environments with environment-specific values.

2. **Simplified CI/CD Integration**: Easy to integrate with CI/CD pipelines by simply changing the environment parameter.

3. **Centralized Configuration**: All deployment settings in one place for easier maintenance.

4. **Version Control**: Track configuration changes alongside your code.

5. **Reduced Duplication**: No need to maintain separate deployment scripts for each environment.

6. **Flexible Overrides**: Override configuration at runtime for special deployment scenarios.

7. **Controlled Rollout**: Easily control which items are published and unpublished in each environment.

## Best Practices

1. **Version Control**: Keep your configuration file in version control with your code.

2. **Environment Validation**: Include all target environments in your configuration.

3. **Path Management**: Use relative paths for portability across different systems.

4. **Exclusion Patterns**: Use careful regex patterns to exclude items from publishing/unpublishing.

5. **Feature Flags**: Be cautious with enabling experimental features in production.

6. **Secrets Management**: Never store credentials in the configuration file - use `token_credential` parameter instead.

7. **Configuration Review**: Review configuration changes before deployment, especially for production environments.

## Troubleshooting

### Common Issues

1. **File Not Found**: Ensure the config file path is correct and accessible.

2. **Environment Not Found**: Verify the environment name exists in the configuration mappings.

3. **Invalid YAML**: Check YAML syntax for errors (indentation, missing quotes, etc.).

4. **Missing Required Fields**: Ensure `core` section contains required fields.

5. **Path Resolution Errors**: Check that paths are valid and accessible.

6. **Feature Flag Requirements**: Confirm required feature flags are enabled when using experimental features.

### Validation Process

The configuration file undergoes several validation checks:

1. File existence and accessibility
2. YAML syntax validation
3. Required section and field validation
4. Environment existence validation
5. Path resolution and validation
6. Feature flag compatibility validation

## Important Notes

-   Config-based deployment requires feature flags `enable_experimental_features` and `enable_config_deploy` to be set.
-   Workspace ID takes precedence over workspace name when both are provided.
-   The `environment` parameter must match one of the environments defined in your mappings.
-   Relative paths are resolved relative to the configuration file location.
-   If `item_types_in_scope` is not specified, all item types will be included.
