# Troubleshooting

This guide provides comprehensive debugging and troubleshooting resources for both users deploying with fabric-cicd and contributors developing within the repository.

## Debugging Deployments

### Enable Debug Logging

fabric-cicd includes a debug logging feature that provides detailed visibility into all operations, including API calls made during deployment.

To enable debug logging, add the following to your deployment script:

```python
from fabric_cicd import change_log_level

# Enable debug logging (call before other fabric-cicd operations)
change_log_level()
```

When debug logging is enabled:

-   All API calls are logged to the console with detailed request/response information
-   Additional context about internal operations is displayed

**Important:** Always enable debug logging when troubleshooting deployment issues. The additional output helps identify whether problems originate from API calls, authentication, or configuration.

### Testing Deployments Locally

Before running deployments via CI/CD pipelines, users can test the deployment workflow locally by running the provided debug scripts. This helps with:

-   Validating configuration changes without affecting production
-   Testing parameter file configurations
-   Debugging deployment issues
-   Verifying authentication and permissions

fabric-cicd includes several debug scripts in the `devtools/` directory that allow users to run deployments against real workspaces in a controlled environment. See [Debug Scripts](#debug-scripts) for detailed information on:

-   `debug_local.py` or `debug_local config.py` - Test full deployment workflows
-   `debug_parameterization.py` - Validate parameter files without deploying
-   `debug_api.py` - Test Fabric REST API calls directly

**Tip:** Using these scripts locally can catch configuration errors early, saving time in your CI/CD pipeline.

### Understanding Error Logs

fabric-cicd automatically creates a `fabric_cicd.error.log` file in your working directory. This file contains:

-   **Full stack traces** for all errors encountered
-   **API request/response details** including URLs, headers, and payloads
-   **Complete diagnostic information** not always shown in console output

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

This information is critical for determining if issues are caused by:

-   API failures or service issues
-   Authentication/authorization problems
-   Invalid request payloads
-   Network connectivity issues

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

#### debug_local.py

**Purpose**: Test full deployment workflows locally against a Microsoft Fabric workspace.

**Configuration**:

```python
# 1. Set your workspace ID, environment, and repository directory path
workspace_id = "your-workspace-id"
environment = "DEV"  # Must match environment in parameter.yml
repository_directory = "root/sample/workspace" # In this example, our workspace content sits within the root/sample/workspace directory

# 2. Configure authentication (optional)
# Uncomment to use Service Principal authentication
# token_credential = ClientSecretCredential(
#     client_id="your-client-id",
#     client_secret="your-client-secret",
#     tenant_id="your-tenant-id"
# )

# 3. Enable debug logging (optional)
# change_log_level()

# 4. Set required feature flags, if any (optional)
# append_feature_flag("feature_flag_1")
# append_feature_flag("feature_flag_2")

# 5. Select item types to deploy (optional, otherwise deploys all supported item types)
# item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

# 6. Create the FabricWorkspace object
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=environment,
    repository_directory=repository_directory,
    # Uncomment to deploy specific item types
    # item_type_in_scope=item_type_in_scope,
    # Uncomment to use SPN auth
    # token_credential=token_credential,
)

# 7. Uncomment publish operation to test
# publish_all_items(target_workspace)

# 8. Uncomment unpublish operation to test
# unpublish_all_orphan_items(target_workspace)
```

**Usage**:

```powershell
cd /path/to/fabric-cicd
uv run python devtools/debug_local.py
```

#### debug_local config.py

**Purpose**: Test configuration-based deployment workflows using a `config.yml` file. See [configuration deployment](config_deployment.md) for more information.

**Configuration**:

```python
# 1. Enable debug logging (optional)
# change_log_level()

# 2. Enable required feature flags
append_feature_flag("enable_experimental_features")
append_feature_flag("enable_config_deploy")

# 3. Point to your config file
config_file = "path/to/config.yml"

# 4. Set environment
environment = "dev"

# 5. Run deployment
deploy_with_config(
    config_file_path=config_file,
    environment=environment
)
```

**Usage**:

```powershell
uv run python "devtools/debug_local config.py"
```

#### debug_parameterization.py

**Purpose**: Validate the `parameter.yml` file without running actual deployments. See [parameterization](parameterization.md#parameter-file-validation) for more information.

**Configuration**:

```python
# 1. Enable debug logging (optional)
# change_log_level()

# 2. Set repository directory containing parameter.yml
repository_directory = "path/to/workspace"

# 3. Define item types to validate against (optional)
#item_type_in_scope = ["DataPipeline", "Notebook", "Environment"]

# 4. Set target environment
environment = "PPE"

# 5. Use custom parameter file location (optional)
# parameter_file_path = "path/to/custom/parameter.yml"

# 6. Run the validaion function using the defined input
validate_parameter_file(
    repository_directory=repository_directory,
    # Uncomment to consider specific item types
    # item_type_in_scope=item_type_in_scope,
    # Comment to exclude target environment in validation
    environment=environment,
    # Uncomment to use a different parameter file name within the repository directory (default name: parameter.yml)
    # Assign to the constant in constants.py or pass in a string directly
    # parameter_file_name=constants.PARAMETER_FILE_NAME,
    # Uncomment to use a parameter file from outside the repository (takes precedence over parameter_file_name)
    # parameter_file_path=parameter_file_path,
    # Uncomment to use SPN auth
    # token_credential=token_credential,
)
```

**Usage**:

```powershell
uv run python devtools/debug_parameterization.py
```

#### debug_api.py

**Purpose**: Test Fabric REST API calls directly without going through full deployment workflows.

**Configuration**:

```python
# 1. Enable debug logging (optional)
# change_log_level()

# 2. Configure authentication (optional)
# Replace None with credential when using SPN auth
token_credential = None  # Uses DefaultAzureCredential if None

# 3. Set workspace ID if needed
workspace_id = "your-workspace-id"

# 4. Configure the API endpoint
api_url = f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}..."

# 5. Make the API call
response = fe.invoke(
    method="POST",  # GET, POST, PUT, DELETE, PATCH
    url=api_url,
    body={},  # Request payload
)
```

**Usage**:

```powershell
uv run python devtools/debug_api.py
```

## Getting Help

If you're still experiencing issues after following this guide:

1. **Enable debug logging** and capture the complete error log
2. **Check existing issues** on [GitHub](https://github.com/microsoft/fabric-cicd/issues)
3. **Create a new issue** using the appropriate template:
    - [Bug Report](https://github.com/microsoft/fabric-cicd/issues/new?template=1-bug.yml)
    - [Question](https://github.com/microsoft/fabric-cicd/issues/new?template=4-question.yml)
4. **Include the following** in your issue:
    - fabric-cicd version (`pip show fabric-cicd`)
    - Python version (`python --version`)
    - Relevant portions of `fabric_cicd.error.log` (redact sensitive information)
    - Minimal code to reproduce the issue
    - Clear steps to reporduce the issue

## Additional Resources

-   [Contribution Guide](https://github.com/microsoft/fabric-cicd/blob/main/CONTRIBUTING.md) - Setup instructions and PR requirements
-   [Feature Flags](optional_feature.md#feature-flags) - Available feature flags for advanced scenarios
-   [Getting Started](getting_started.md) - Basic installation and authentication
-   [Microsoft Fabric API Documentation](https://learn.microsoft.com/en-us/rest/api/fabric/) - Official API reference
