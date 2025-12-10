# Semantic Model Deployment

## Overview

Semantic model deployment in fabric-cicd supports advanced features including handling destructive changes and automatic refresh after deployment. This guide covers how to work with semantic models in your CI/CD pipelines.

## Handling Destructive Changes

### What are Destructive Changes?

Destructive changes are schema modifications to semantic models that can cause data loss or require the dataset to be dropped and fully reprocessed. Common examples include:

-   Removing or renaming columns
-   Changing a column's data type
-   Altering incremental refresh or partition definitions
-   Removing or modifying hierarchies
-   Disabling Auto date/time features

### Error Detection

When fabric-cicd detects a destructive change during deployment, it will:

1. Log detailed warning messages explaining the issue
2. Provide guidance on resolution options
3. Fail the deployment (to prevent accidental data loss)

The error will typically include:

```
WARNING: Semantic model 'MyModel' deployment failed due to destructive changes that require data purge.
WARNING: Destructive changes include operations like: removing columns, changing data types,
         altering partition definitions, or removing hierarchies.
```

### Resolution Options

When you encounter a destructive change error, you have three options:

#### Option 1: Use XMLA Endpoint to Clear Values (Recommended)

Before redeploying, connect to the semantic model via the XMLA endpoint and execute a TMSL command to clear values:

**Using Tabular Editor or SSMS:**

1. Connect to your Power BI workspace via XMLA endpoint:
    - Endpoint URL: `powerbi://api.powerbi.com/v1.0/myorg/[WorkspaceName]`
2. Execute the following TMSL command:

```json
{
    "refresh": {
        "type": "clearValues",
        "objects": [
            {
                "database": "YourSemanticModelName"
            }
        ]
    }
}
```

3. After clearing values, redeploy using fabric-cicd

**Using PowerShell:**

```powershell
# Install Analysis Services PowerShell module if not already installed
Install-Module -Name Az.AnalysisServices

# Define the XMLA endpoint and model
$workspaceName = "YourWorkspace"
$modelName = "YourSemanticModel"
$xmlaEndpoint = "powerbi://api.powerbi.com/v1.0/myorg/$workspaceName"

# TMSL script to clear values
$tmslScript = @"
{
  "refresh": {
    "type": "clearValues",
    "objects": [
      {
        "database": "$modelName"
      }
    ]
  }
}
"@

# Execute the TMSL command
Invoke-ASCmd -Server $xmlaEndpoint -Query $tmslScript
```

#### Option 2: Delete and Recreate

Manually delete the semantic model from the target workspace, then redeploy. This is the simplest option but results in temporary unavailability.

#### Option 3: Revert Changes

Review your schema changes and revert any incompatible modifications. Consider using additive changes (adding new columns/measures) instead of modifying existing ones.

## Automatic Refresh After Deployment

fabric-cicd supports automatic refresh of semantic models after successful deployment using the `semantic_model_refresh` parameter.

### Basic Usage

Add the following to your `parameter.yml` file:

```yaml
semantic_model_refresh:
    - semantic_model_name: "Sales Model"
    - semantic_model_name: ["Marketing Model", "Finance Model"]
```

This will perform a default full refresh on the specified models after deployment.

### Advanced Refresh Options

For Premium capacities, you can specify custom refresh payloads to control exactly how the refresh is performed:

```yaml
semantic_model_refresh:
    - semantic_model_name: "Sales Model"
      refresh_payload:
          type: "full"
          objects:
              - table: "Sales"
              - table: "Products"
                partition: "Products-2024"
          commitMode: "transactional"
          maxParallelism: 2
          retryCount: 1
```

**Supported refresh payload options:**

-   `type`: Type of refresh (`"full"`, `"calculate"`, `"dataOnly"`, `"automatic"`)
-   `objects`: Array of tables/partitions to refresh (omit for full model refresh)
-   `commitMode`: `"transactional"` or `"partialBatch"`
-   `maxParallelism`: Number of parallel threads (Premium only)
-   `retryCount`: Number of retry attempts on failure
-   `timeout`: Timeout in format `"hh:mm:ss"`

### Refresh Behavior

-   Refresh is initiated asynchronously (HTTP 202 response)
-   Deployment continues while refresh runs in background
-   If refresh fails, deployment does not fail (warning is logged)
-   Use Power BI workspace or API to monitor refresh status

### Complete Example

Here's a complete `parameter.yml` example combining binding and refresh:

```yaml
# Bind semantic models to connections
semantic_model_binding:
    - connection_id: "abc123-guid-here"
      semantic_model_name: "Sales Model"

# Refresh models after deployment
semantic_model_refresh:
    - semantic_model_name: "Sales Model"
      refresh_payload:
          type: "full"
          commitMode: "transactional"
```

## Best Practices

1. **Always test destructive changes** in a development environment first
2. **Use XMLA endpoint** for clearing values before deploying destructive changes to production
3. **Monitor refresh status** after deployment to ensure data is current
4. **Use incremental refresh** where possible to minimize processing time
5. **Document schema changes** in your commit messages or pull requests
6. **Consider using separate models** for frequently changing schemas to isolate impact

## Prerequisites

-   **XMLA Endpoint Access**: Premium, Premium Per User, or Embedded capacity
-   **Permissions**: Contributor or Admin role on the workspace
-   **Authentication**: Azure AD credentials with appropriate permissions

## Troubleshooting

### "Cannot connect to XMLA endpoint"

-   Verify your workspace is in a Premium capacity
-   Check that XMLA read-write is enabled (Workspace Settings → Premium)
-   Ensure your credentials have appropriate permissions

### "Refresh fails after deployment"

-   Check data source connectivity
-   Verify credentials and connections are properly configured
-   Review refresh errors in the Power BI workspace refresh history

### "Still getting destructive change error after clearing values"

-   Ensure you cleared values for the correct semantic model
-   Verify the TMSL command executed successfully
-   Try using Option 2 (delete and recreate) instead

## Additional Resources

-   [Power BI REST API - Refresh Dataset](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset)
-   [TMSL Reference - Refresh Command](https://learn.microsoft.com/en-us/analysis-services/tmsl/refresh-command-tmsl)
-   [XMLA Endpoint Documentation](https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-connect-tools)
-   [Enhanced Refresh API](https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh)
