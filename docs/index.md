fabric-cicd is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library supports code-first Continuous Integration / Continuous Deployment (CI/CD) automations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal is to assist CI/CD developers who prefer not to interact directly with the Microsoft Fabric APIs.

## Base Expectations

-   Full deployment every time, without considering commit diffs
-   Deploys into the tenant of the executing identity
-   Only supports items that have Source Control, and Public Create/Update APIs

## Supported Item Types

<!--BEGIN-SUPPORTED-ITEM-TYPES-->
<!--END-SUPPORTED-ITEM-TYPES-->

## Installation

To install fabric-cicd, run:

```bash
pip install fabric-cicd
```

## Basic Example

```python
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id = "your-workspace-id",
    environment = "your-target-environment",
    repository_directory = "your-repository-directory",
    item_type_in_scope = ["Notebook", "DataPipeline", "Environment"],
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
```

> **Note**: The `environment` parameter is required for parameter replacement to work properly. It must match one of the environment keys defined in your `parameter.yml` file (e.g., "PPE", "PROD", "DEV"). If you don't need parameter replacement, you can omit this parameter.

## Structured Logging

Both `publish_all_items()` and `unpublish_all_orphan_items()` return detailed deployment logs that can be used for monitoring, reporting, and debugging:

```python
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

target_workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    environment="DEV",
    repository_directory="./workspace-items",
    item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
)

# Get structured logs for publish operations
publish_logs = publish_all_items(target_workspace)

# Get structured logs for unpublish operations
unpublish_logs = unpublish_all_orphan_items(target_workspace)

# Process the logs
for log_entry in publish_logs:
    status = "✅ Success" if log_entry.success else "❌ Failed"
    print(f"{status} - {log_entry.item_type}: {log_entry.name}")
    print(f"   Duration: {log_entry.duration_seconds:.2f}s")

    if not log_entry.success:
        print(f"   Error: {log_entry.error}")
        print(f"   Item ID: {log_entry.guid}")
```

### Log Entry Properties

Each log entry contains:

-   `name`: Item display name
-   `item_type`: Type of Fabric item (Notebook, DataPipeline, etc.)
-   `operation_type`: Either "publish" or "unpublish"
-   `success`: Boolean indicating if operation succeeded
-   `start_time`: When the operation started (datetime)
-   `end_time`: When the operation completed (datetime)
-   `duration_seconds`: Operation duration in seconds (float)
-   `guid`: Fabric item ID (available after successful publish)
-   `error`: Error details (only present when success=False)

### Use Cases for Structured Logs

**CI/CD Pipeline Reporting:**

```python
# Count successful vs failed operations
successful = [log for log in publish_logs if log.success]
failed = [log for log in publish_logs if not log.success]

print(f"Published: {len(successful)} items successfully")
print(f"Failed: {len(failed)} items")

# Fail the pipeline if any items failed
if failed:
    for entry in failed:
        print(f"FAILED: {entry.item_type} '{entry.name}' - {entry.error}")
    exit(1)
```

**Performance Monitoring:**

```python
# Track deployment performance
slow_operations = [log for log in publish_logs if log.duration_seconds > 30]
total_time = sum(log.duration_seconds for log in publish_logs)

print(f"Total deployment time: {total_time:.2f} seconds")
if slow_operations:
    print(f"Slow operations detected: {len(slow_operations)}")
```

**Custom Notifications:**

```python
# Send alerts for specific failures
for entry in publish_logs:
    if not entry.success and "timeout" in entry.error.lower():
        send_alert(f"Timeout deploying {entry.item_type}: {entry.name}")
```
