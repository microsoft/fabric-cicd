# Troubleshooting

This guide provides comprehensive debugging and troubleshooting resources for both users deploying with fabric-cicd and contributors developing within the repository.

## For Users: Debugging Deployments

### Enabling Debug Logging

fabric-cicd includes a debug logging feature that provides detailed visibility into all operations, including API calls made during deployment.

To enable debug logging, add the following to your deployment script:

```python
from fabric_cicd import change_log_level

# Enable debug logging (must be called before other fabric-cicd operations)
change_log_level("DEBUG")
```

When debug logging is enabled:

-   All API calls are logged to the console with detailed request/response information
-   Complete execution traces are written to `fabric_cicd.error.log`
-   Additional context about internal operations is displayed

!!! tip "Best Practice"
    Always enable debug logging when troubleshooting deployment issues. The additional output helps identify whether problems originate from API calls, authentication, or configuration.

### Understanding Error Logs

fabric-cicd automatically creates a `fabric_cicd.error.log` file in your working directory. This file contains:

-   **Full stack traces** for all errors encountered
-   **Complete API request/response details** including URLs, headers, and payloads
-   **Additional diagnostic information** not shown in console output

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
4. Enable debug logging to see which credential is being used

#### Item Deployment Failures

**Symptom**: Specific items fail to deploy while others succeed

**Solution**:

1. Enable debug logging to see the exact API error
2. Check `fabric_cicd.error.log` for detailed API response
3. Verify the item definition files are properly formatted
4. Ensure item names don't contain special characters or exceed length limits
5. Check if the item type is included in your `item_type_in_scope` list

#### Parameter Substitution Issues

**Symptom**: Deployed items contain literal `@{parameter_name}` instead of values

**Solution**:

1. Verify your `parameter.yml` file is in the correct location (repository directory by default)
2. Check that parameter names in your files exactly match those in `parameter.yml`
3. Ensure the environment name matches between your script and `parameter.yml`
4. Use the [debug_parameterization.py](#debug_parameterizationpy) script to validate parameter files

#### API Rate Limiting

**Symptom**: Deployments fail with "429 Too Many Requests" errors

**Solution**:

1. Add delays between operations if deploying many items
2. Consider deploying in smaller batches
3. Check `fabric_cicd.error.log` for retry-after headers in API responses

## For Contributors: Development Tools

The `devtools/` directory contains several scripts to help contributors test and debug their code changes before submitting pull requests.

### Debug Scripts

#### debug_local.py

**Purpose**: Test full deployment workflows locally against a real Microsoft Fabric workspace.

**Use Cases**:

-   Validate changes to publish/unpublish logic
-   Test against different workspace configurations
-   Debug item type-specific deployment issues
-   Verify feature flag behavior

**Configuration**:

```python
# 1. Set your workspace ID and environment
workspace_id = "your-workspace-id"
environment = "DEV"  # Must match environment in parameter.yml

# 2. Configure authentication (optional)
# Uncomment to use Service Principal authentication
# token_credential = ClientSecretCredential(
#     client_id="your-client-id",
#     client_secret="your-client-secret",
#     tenant_id="your-tenant-id"
# )

# 3. Enable debug logging (optional)
# change_log_level()

# 4. Select item types to deploy
item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

# 5. Uncomment the operation to test
# publish_all_items(target_workspace)
# unpublish_all_orphan_items(target_workspace)
```

**Usage**:

```bash
cd /path/to/fabric-cicd
uv run python devtools/debug_local.py
```

#### debug_local config.py

**Purpose**: Test configuration-based deployment workflows using a `config.yml` file.

**Use Cases**:

-   Validate the experimental `deploy_with_config` feature
-   Test selective deployments with include/exclude patterns
-   Debug configuration file parsing and validation

**Configuration**:

```python
# 1. Enable required feature flags
append_feature_flag("enable_experimental_features")
append_feature_flag("enable_config_deploy")

# 2. Point to your config file
config_file = "path/to/config.yml"

# 3. Set environment
environment = "dev"

# 4. Run deployment
deploy_with_config(
    config_file_path=config_file,
    environment=environment
)
```

**Usage**:

```bash
uv run python devtools/debug_local\ config.py
```

#### debug_parameterization.py

**Purpose**: Validate `parameter.yml` files without performing actual deployments.

**Use Cases**:

-   Test parameter file structure and syntax
-   Verify parameter values for specific environments
-   Debug parameter substitution logic
-   Validate parameter files before committing

**Configuration**:

```python
# 1. Set repository directory containing parameter.yml
repository_directory = "path/to/workspace"

# 2. Define item types to validate against
item_type_in_scope = ["DataPipeline", "Notebook", "Environment"]

# 3. Set target environment
environment = "PPE"

# 4. (Optional) Use custom parameter file location
# parameter_file_path = "path/to/custom/parameter.yml"
```

**Usage**:

```bash
uv run python devtools/debug_parameterization.py
```

This script will:

-   Parse the parameter file
-   Validate structure and required fields
-   Check that all parameters for the specified environment exist
-   Report any errors or warnings

#### debug_api.py

**Purpose**: Test Fabric REST API calls directly without going through full deployment workflows.

**Use Cases**:

-   Debug and validate Fabric API endpoints
-   Test API request/response payloads
-   Prototype new API integrations
-   Verify authentication and authorization
-   Troubleshoot API-specific issues

**Configuration**:

```python
# 1. Configure authentication (optional)
# Replace None with credential when using SPN auth
token_credential = None  # Uses DefaultAzureCredential if None

# 2. Set workspace ID if needed
workspace_id = "your-workspace-id"

# 3. Configure the API endpoint
api_url = f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}..."

# 4. Make the API call
response = fe.invoke(
    method="POST",  # GET, POST, PUT, DELETE, PATCH
    url=api_url,
    body={},  # Request payload
)
```

**Usage**:

```bash
uv run python devtools/debug_api.py
```

This script provides direct access to Fabric REST APIs using the `FabricEndpoint` class, allowing you to:

-   Test any Fabric API endpoint
-   Customize HTTP methods and request bodies
-   Debug API responses without deployment overhead
-   Validate API changes during development

#### pypi_build_release_dev.ps1

**Purpose**: Build and publish development versions of the package to TestPyPI for testing.

**Use Cases**:

-   Test package installation and imports before releasing to production PyPI
-   Validate packaging configuration changes
-   Verify distribution includes all necessary files

**Usage**:

```powershell
# From PowerShell terminal
cd devtools
.\pypi_build_release_dev.ps1
```

!!! warning "Requires TestPyPI Credentials"
    This script requires TestPyPI credentials configured in your environment. It's typically only used by maintainers preparing releases.

### Testing Your Changes

Before submitting a pull request, always test your changes:

1. **Run Unit Tests**:
    ```bash
    uv run pytest -v
    ```

2. **Check Code Formatting**:
    ```bash
    uv run ruff format
    uv run ruff check
    ```

3. **Test Import Functionality**:
    ```bash
    uv run python -c "from fabric_cicd import FabricWorkspace; print('Import successful')"
    ```

4. **Use Debug Scripts**: Test your changes against real scenarios using the appropriate debug script from `devtools/`.

### Running Tests with Debug Output

To see detailed test output and debug information:

```bash
# Run with verbose output
uv run pytest -v -s

# Run specific test file
uv run pytest tests/test_specific.py -v

# Run tests matching a pattern
uv run pytest -k "test_pattern" -v
```

## Additional Resources

-   [Contribution Guide](../contribution.md) - Setup instructions and PR requirements
-   [Feature Flags](optional_feature.md#feature-flags) - Available feature flags for advanced scenarios
-   [Getting Started](getting_started.md) - Basic installation and authentication
-   [Microsoft Fabric API Documentation](https://learn.microsoft.com/en-us/rest/api/fabric/core/) - Official API reference

## Getting Help

If you're still experiencing issues after following this guide:

1. **Enable debug logging** and capture the complete error log
2. **Check existing issues** on [GitHub](https://github.com/microsoft/fabric-cicd/issues)
3. **Create a new issue** using the appropriate template:
    -   [Bug Report](https://github.com/microsoft/fabric-cicd/issues/new?template=1-bug.yml)
    -   [Question](https://github.com/microsoft/fabric-cicd/issues/new?template=4-question.yml)
4. **Include the following** in your issue:
    -   fabric-cicd version (`pip show fabric-cicd`)
    -   Python version (`python --version`)
    -   Relevant portions of `fabric_cicd.error.log` (redact sensitive information)
    -   Minimal code to reproduce the issue
