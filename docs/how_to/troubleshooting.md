# Troubleshooting

This guide provides comprehensive debugging and troubleshooting resources for both users deploying with fabric-cicd and contributors developing within the repository.

## Debugging Deployments

### Enable Debug Logging

fabric-cicd includes a debug logging feature that provides detailed visibility into all operations, including API calls made during deployment.

**Default Behavior:**

- Without debug logging enabled, fabric-cicd displays only high-level progress messages, warnings and errors
- The `fabric_cicd.error.log` file will contain the same lines printed to the console along with stack traces for any errors

**Enabling Debug Logging:**

To enable debug logging, add the following to your deployment script:

```python
from fabric_cicd import change_log_level

# Enable debug logging (call before other fabric-cicd operations)
change_log_level()
```

When debug logging is enabled, all API calls are logged with detailed request/response information, and additional context about internal operations is displayed. Both the console and the `fabric_cicd.error.log` file will contain the detailed information.

**Important:** Always enable debug logging when troubleshooting deployment issues. The additional output helps identify whether problems originate from API calls, authentication, or configuration. See [Understanding Error Logs](#understanding-error-logs) for details on interpreting log output.

### Testing Deployments Locally

Before running deployments via CI/CD pipelines, users can test the deployment workflow locally by running the provided debug scripts. This helps with:

- Validating configuration changes without affecting production
- Testing parameter file configurations
- Debugging deployment issues
- Verifying authentication and permissions

fabric-cicd includes several debug scripts in the `devtools/` directory that allow users to run deployments against real workspaces in a controlled environment. See [Debug Scripts](#debug-scripts) for detailed information on:

- `debug_local.py` or `debug_local config.py` - Test full deployment workflows
- `debug_parameterization.py` - Validate parameter files without deploying
- `debug_api.py` - Test Fabric REST API calls directly
- `debug_trace_deployment.py` - Perform and end-to-end deployment against a Fabric Workspace and capture HTTP Traces to be used for Integration Tests

**Tip:** Using these scripts locally can catch configuration errors early, saving time in your CI/CD pipeline.

### Sample Workspace Directory

fabric-cicd includes the `sample/workspace/` directory that demonstrates the recommended repository structure for Fabric item source control files. It contains sample items of various supported item types (e.g., Environment, Notebook, Data Pipeline, etc.).

**Repository Directory Structure:**

```
sample/workspace/
├── Sample Pipeline.DataPipeline/
│   ├── .platform
│   └── pipeline-content.json
├── Sample_Notebook.Notebook/
│   ├── .platform
│   └── notebook-content.py
...
└── parameter.yml
```

Each item folder follows the naming convention `ItemName.ItemType/` and contains:

- `.platform` file which contains the item metadata
- Item definition files (e.g., `pipeline-content.json`, `notebook-content.py`)

**Using the Sample:**

Use this sample structure as a template for organizing your Fabric items. To test deployments with the items found in the sample workspace, set `repository_directory = "sample/workspace"` in `debug_local.py` or in `config.yml` when running `debug_local config.py`.

### Understanding Error Logs

When running a deployment, fabric-cicd automatically creates a `fabric_cicd.error.log` file in your working directory. The level of detail captured depends on whether [debug logging is enabled](#enable-debug-logging).

**Tip:** Always enable debug logging when troubleshooting deployment issues to capture full API traces in the log file.

#### Accessing API Traces

When an error occurs during deployment, the console will display:

```
Error: [Brief error message]

See /path/to/fabric_cicd.error.log for full details.
```

Open the `fabric_cicd.error.log` file to view:

1. **Request Details**: The exact API endpoint called, HTTP method, and request body
2. **Response Details**: Status code, response headers, and complete response body
3. **Timing Information**: When the call was made
4. **Stack Trace**: The complete call stack leading to the error
5. **Additional Logs**: Information on internal operations that occurred during deployment

This information is critical for determining if issues are caused by:

- API failures or service issues
- Authentication/authorization problems
- Invalid request payloads
- Network connectivity issues

#### Example Error Log Entry

```
2024-01-06 10:30:45 - ERROR - fabric_cicd.api - API call failed
Request: POST https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items
Headers: {'Authorization': '***', 'Content-Type': 'application/json'}
Body: {"displayName": "MyNotebook", "type": "Notebook", ...}

Response: 400 Bad Request
Body: {"error": {"code": "InvalidRequest", "message": "Item name contains invalid characters"}}

Traceback (most recent call last):
  File "fabric_cicd/publish.py", line 123, in publish_item
    response = api.create_item(...)
  ...
```

### Common Issues and Solutions

#### Authentication Failures

**Symptom**: Errors mentioning "authentication failed" or "401 Unauthorized"

**Solution**:

1. Verify you're logged in with Azure CLI or Az.Accounts PowerShell module:
    ```bash
    az login
    ```
    or
    ```powershell
    Connect-AzAccount
    ```
2. Check that your account has appropriate permissions on the target workspace
3. If using Service Principal authentication, verify client ID, secret, and tenant ID are correct

#### Item Deployment Failures

**Symptom**: Specific items fail to deploy while others succeed

**Solution**:

1. Enable debug logging to see the exact API error
2. Check `fabric_cicd.error.log` for detailed API response
3. Verify the item definition files exist and are properly formatted
4. Check if the item type is included in your `item_type_in_scope` list
5. Ensure item dependencies exist (e.g., a Data Pipeline referencing a Notebook must be deployed along with the Notebook)
6. If deleting and recreating an item with the same name, wait 5 minutes between operations due to Fabric API item name reservation

#### Parameter Substitution Issues

**Symptom**: Deployed items contain literal find value instead of the proper replace value

**Solution**:

1. Verify your `parameter.yml` file is in the correct location (repository directory by default)
2. Check that find values in your files exactly match those in `parameter.yml`
3. Ensure the environment name matches between your script and `parameter.yml`
4. Validate the find value regex and/or dynamic replacement variables in `parameter.yml`
5. Use the [debug_parameterization.py](#debug_parameterizationpy) script to validate parameter files

#### API Rate Limiting

**Symptom**: Deployments fail with "429 Too Many Requests" errors

**Solution**:

1. Consider deploying in smaller batches
2. Check `fabric_cicd.error.log` for retry-after headers in API responses

### Debug Scripts

The `devtools/` directory contains pre-built scripts to help test and validate deployments, parameter files, and Fabric REST APIs locally. These scripts already exist in the repository - you just need to configure them for your scenario.

#### debug_local.py

**Purpose**: Test full deployment workflows locally against a Microsoft Fabric workspace.

**Key Configuration Options**:

| Configuration          | Description                                                        | Required |
| ---------------------- | ------------------------------------------------------------------ | -------- |
| `workspace_id`         | Target Fabric workspace ID                                         | Yes      |
| `environment`          | Target environment (used for parameterization)                     | No       |
| `repository_directory` | Path to Fabric workspace items files (absolute or relative path)   | Yes      |
| `item_type_in_scope`   | Specific item types to deploy (defaults to all supported types)    | No       |
| `token_credential`     | Service Principal credentials (defaults to DefaultAzureCredential) | No       |

**Quick Start**:

1. Open `devtools/debug_local.py`
2. Set `workspace_id`, `environment`, and `repository_directory` at the top
3. Uncomment `change_log_level()` to enable debug logging
4. Add necessary [feature flags](optional_feature.md#feature-flags) required for deployment
5. Uncomment `publish_all_items(target_workspace)` and/or `unpublish_all_orphan_items(target_workspace)` to test deployment
6. Run: `uv run python devtools/debug_local.py`

**Common Configurations**:

```python
# Enable debug logging
change_log_level()

# Add feature flag(s)
append_feature_flag("enable_shortcut_publish")

# Use sample workspace for testing
repository_directory = "sample/workspace"

# Deploy only specific item types
item_type_in_scope = ["Environment", "Notebook", "DataPipeline"]

# Use Service Principal authentication
from azure.identity import ClientSecretCredential
token_credential = ClientSecretCredential(
    client_id="your-client-id",
    client_secret="your-client-secret",
    tenant_id="your-tenant-id"
)

# Override constant value
constants.DEFAULT_API_ROOT_URL = "https://api.fabric.microsoft.com"
```

#### debug_local config.py

**Purpose**: Test configuration-based deployment workflows using a `config.yml` file.

**Key Configuration Options**:

| Configuration      | Description                                                                         | Required |
| ------------------ | ----------------------------------------------------------------------------------- | -------- |
| `config_file`      | Path to your `config.yml` file                                                      | Yes      |
| `environment`      | Target environment (used for parameterization and environment-based configurations) | No       |
| `token_credential` | Service Principal credentials (defaults to DefaultAzureCredential)                  | No       |
| `config_override`  | Dictionary to override configuration values within `config.yml`                     | No       |

**Quick Start**:

1. Open `devtools/debug_local config.py`
2. Set `config_file` path and `environment` (can use the sample `config.yml` file found in `sample/workspace`)
3. Uncomment `change_log_level()` to enable debug logging
4. Ensure required feature flags are enabled (already set in script)
5. Run: `uv run python "devtools/debug_local config.py"`

See [configuration deployment](config_deployment.md) for details on creating `config.yml`.

#### debug_parameterization.py

**Purpose**: Validate parameter file without deploying items - useful for catching parameterization errors early.

**Key Configuration Options**:

| Configuration          | Description                                                                          | Required |
| ---------------------- | ------------------------------------------------------------------------------------ | -------- |
| `repository_directory` | Path to Fabric workspace items files and `parameter.yml` file (default location)     | Yes      |
| `environment`          | Target environment (used for parameterization)                                       | No       |
| `item_type_in_scope`   | Item types to validate (defaults to all)                                             | No       |
| `parameter_file_name`  | Alternate parameter file name within repository directory (default: `parameter.yml`) | No       |
| `parameter_file_path`  | Alternate location of parameter file                                                 | No       |

**Quick Start**:

1. Open `devtools/debug_parameterization.py`
2. Set `repository_directory` and `environment` (the `parameter.yml` file is located in the repository directory in this case)
3. Uncomment `change_log_level()` to view all the validation steps
4. Run: `uv run python devtools/debug_parameterization.py`

See [parameterization](parameterization.md#parameter-file-validation) for more information.

#### debug_api.py

**Purpose**: Test Fabric REST API calls directly without going through full deployment workflows.

**Key Configuration Options**:

| Configuration      | Description                                                         | Required |
| ------------------ | ------------------------------------------------------------------- | -------- |
| `api_url`          | Full API endpoint URL                                               | Yes      |
| `method`           | HTTP method (GET, POST, DELETE, PATCH)                              | Yes      |
| `body`             | Request payload (for POST/PATCH)                                    | Varies   |
| `token_credential` | Service Principal credentials (defaults to DefaultAzureCredential)  | No       |
| other              | View `invoke()` in `FabricEndpoint` class for additional parameters | No       |

**Quick Start**:

1. Open `devtools/debug_api.py`
2. Configure the API endpoint, method, body (if any)
3. Uncomment `change_log_level()` to view API request/response details
4. Run: `uv run python devtools/debug_api.py`

#### debug_trace_deployment.py

**Purpose**: Debug the public API `publish_all_items()` workflow with breakpoints using VS Code's debugger.

**Quick Start**:

1. Open `devtools/debug_trace_deployment.py`
2. Set breakpoint(s) in the code - e.g. prior to `publish_all_items`
3. Update `.vscode/launch.json` with your workspace ID in `FABRIC_WORKSPACE_ID`
4. Press **F5** → Select "Debug: Publish All Items"

## Getting Help

If you're still experiencing issues after following this guide:

1. **Enable debug logging** and capture the complete error log
2. **Check existing issues** on [GitHub](https://github.com/microsoft/fabric-cicd/issues)
3. **Create a new issue** using the appropriate template:
    - [Bug Report](https://github.com/microsoft/fabric-cicd/issues/new?template=1-bug.yml)
    - [Question](https://github.com/microsoft/fabric-cicd/issues/new?template=4-question.yml)

## Additional Resources

- [Contribution Guide](https://github.com/microsoft/fabric-cicd/blob/main/CONTRIBUTING.md) - Setup instructions and PR requirements
- [Feature Flags](optional_feature.md#feature-flags) - Available feature flags for advanced scenarios
- [Getting Started](getting_started.md) - Basic installation and authentication
- [Microsoft Fabric API Documentation](https://learn.microsoft.com/en-us/rest/api/fabric/) - Official API reference
