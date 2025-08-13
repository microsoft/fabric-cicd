# Parameterization

## Overview

To handle environment-specific values committed to git, use a `parameter.yml` file. This file supports programmatically changing values based on the `environment` field passed into the `FabricWorkspace` object. If the environment value is not found in the `parameter.yml` file, any dependent replacements will be skipped. This file should sit in the root of the `repository_directory` folder specified in the FabricWorkspace object.

Example of parameter.yml location based on provided repository directory:

```python
from fabric_cicd import FabricWorkspace
workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    repository_directory="C:/dev/workspace",
    item_type_in_scope=["Notebook"]
)
```

```
C:/dev/workspace
    /HelloWorld.Notebook
        ...
    /GoodbyeWorld.Notebook
        ...
    /parameter.yml
```

Raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml) for additional parameterization capabilities.

## Parameter Inputs

### `find_replace`

For generic find-and-replace operations. This will replace every instance of a specified string in every file. Specify the `find_value` and the `replace_value` for each environment (e.g., PPE, PROD). Optional fields, including `item_type`, `item_name`, and `file_path`, can be used as file filters for more fine-grained control over where the replacement occurs. The `is_regex` field can be added and set to `"true"` to enable regex pattern matching for the `find_value`.

Note: A common use case for this function is to replace values in text based file types like notebooks.

```yaml
find_replace:
    # Required fields: value must be a string
    - find_value: <find-this-value>
      replace_value:
          <environment-1-key>: <replace-with-this-value>
          <environment-2-key>: <replace-with-this-value>
      # Optional fields
      # Set to "true" to treat find_value as a regex pattern
      is_regex: "<true|True>"
      # Filter values must be a string or array of strings
      item_type: <item-type-filter-value>
      item_name: <item-name-filter-value>
      file_path: <file-path-filter-value>
```

### `key_value_replace`

Provides the ability to perform key based replacement operations in JSON and YAML files. This will look for a specific key using a valid JSONPath expression and replace every found instance in every file. Specify the `find_value` and the `replace_value` for each environment (e.g., PPE, PROD). Optional fields, including `item_type`, `item_name`, and `file_path`, can be used as file filters for more fine-grained control over where the replacement occurs. Refer to https://jsonpath.com/ for a simple to use JSONPath evaluator.

Note: A common use case for this function is to replace values in key/value file types like Pipelines, Platform files, etc. e.g., find and replace a connection GUID referenced in a data pipeline.

```yaml
key_value_replace:
    # Required fields: key must be JSONPath
    - find_key: <find-this-key>
      replace_value:
          <environment-1-key>: <replace-with-this-value>
          <environment-2-key>: <replace-with-this-value>
      # Optional fields: value must be a string or array of strings
      item_type: <item-type-filter-value>
      item_name: <item-name-filter-value>
      file_path: <file-path-filter-value>
```

### `spark_pool`

Environments attached to custom spark pools need to be parameterized because the `instance_pool_id` in the `Sparkcompute.yml` file isn't supported in the create/update environment APIs. Provide the `instance_pool_id` value, and the pool `type` and `name` values as the `replace_value` for each environment (e.g., PPE, PROD). An optional field, `item_name`, can be used to filter the specific environment item where the replacement will occur.

```yaml
spark_pool:
    # Required fields: value must be a string
    - instance_pool_id: <instance-pool-id-value>
      replace_value:
          <environment-1-key>:
              type: <Capacity-or-Workspace>
              name: <pool-name>
          <environment-2-key>:
              type: <Capacity-or-Workspace>
              name: <pool-name>
      # Optional field: value must be a string or array of strings
      item_name: <item-name-filter-value>
```

## Advanced Find and Replace

### `find_value` Regex

In the `find_replace` parameter, the `find_value` can be set to a regex pattern instead of a literal string to find a value in the files to replace. When a match is found, the `find_value` is assigned to the matched string and can be used to replace all occurrences of that value in the file.

-   **How to** use this feature:
    -   Set the `find_value` to a **valid regex pattern** wrapped in quotes.
    -   Include the optional field `is_regex` and set it to the value `"true"`, see [more details](#regex-pattern-match).
-   **Important:**
    -   The user is solely **responsible for providing a valid and correctly matching regex pattern**. If the pattern is invalid (i.e., it cannot be compiled) or fails to match any content in the target files, deployment will fail.
    -   A valid regex pattern requires the following:
        -   Ensure that all special characters in the regex pattern are properly **escaped**.
        -   The exact value intended to be replaced must be enclosed in parentheses `( )`.
        -   The parentheses creates a **capture group 1**, which must always be used as the replacement target. Capture group 1 should isolate values like a GUID, SQL connection string, etc.
        -   Include the **surrounding context** in the pattern, such as property/field names, quotes, etc. to ensure it matches the correct value and not a value with a similar format elsewhere in the file.
-   **Example:**
    -   Use a regex `find_value` to match a lakehouse ID inside a Notebook file. **Note:** avoid using a pattern that ONLY matches the GUID format as doing so would risk replacing any matching GUID in the file, not just the intended one. Include the surrounding context in your pattern—such as `# META "default_lakehouse": "123456"`—and capture only the `123456` GUID in group 1. This ensures that only the correct, context-specific GUID is replaced.

```yaml
find_replace:
    # A valid regex pattern to match the default_lakehouse ID
    - find_value: \#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          PPE: "e8a7f3c6-9b2d-4f5e-a1b0-7c98d4e6a5f3" # PPE Lakehouse GUID
          PROD: "12c45d67-89ab-4cde-f012-3456789abcde" # PROD Lakehouse GUID
      # Optional field: Set to "true" to treat find_value as a regex pattern
      is_regex: "true" # "<true|True>"
      item_type: "Notebook" # filter on notebook files
      item_name: ["Hello World", "Goodbye World"] # filter on specific notebook files
```

### Dynamic Replacement

The `replace_value` field in the `find_replace` parameter supports fabric-cicd defined _variables_ that reference workspace or deployed item metadata:

-   **Dynamic workspace/item metadata replacement ONLY works for referenced items that exist in the `repository_directory`.**
-   Dynamic replacement works in tandem with `find_value` as either a regex or a literal string.
-   The `replace_value` can contain a mix of input values within the _same_ parameter input, e.g. `PPE` is set to a static string and `PROD` is set to a variable.
-   **Supported variables:**
    -   **Workspace ID variable:** `$workspace.id`, replaces a workspace ID with the workspace ID of the **target environment.**
    -   **Item attribute variable:** `$items.<item_type>.<item_name>.<attribute>`, replaces the item's attribute value with the corresponding attribute value of the item in the deployed/target workspace.
        -   **Supported attributes**: `id` (item ID of the deployed item), `sqlendpoint` (sql connection string of the deployed item, only applicable to lakehouse and warehouse items), and `queryserviceuri` (query uri of the deployed item, only applicable to eventhouse item). Attributes should be lowercase.
        -   Item type and name are **case-sensitive**.
        -   Item type must be valid and in scope.
        -   Item name must be an **exact match** (include spaces, if present).
        -   **Example:** set `$items.Notebook.Hello World.id` to get the item ID of the `"Hello World"` Notebook in the target workspace.
-   **Important**: Deployment will fail in the following cases:
    -   Incorrect variable syntax used, e.g., `$item.Notebook.Hello World.id` instead of `$items.Notebook.Hello World.id`.
    -   The specified **item type** or **name** is invalid or does NOT exist in the deployed workspace, e.g., `$items.Notebook.HelloWorld.id` or `$items.Environment.Hello World.id`.
    -   An invalid attribute name is provided, e.g., `$items.Notebook.Hello World.guid` instead of `$items.Notebook.Hello World.id`.
    -   The attribute value does NOT exist, e.g., `$items.Notebook.Hello World.sqlendpoint` (Notebook items don't have a SQL Endpoint).
-   For example use-cases, see the **Notebook/Dataflow Advanced `find_replace` Parameterization Case.**

```yaml
find_replace:
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" # Lakehouse GUID
      replace_value:
          PPE: "$items.Lakehouse.Sample_LH.id" # PPE Sample_LH Lakehouse GUID
          PROD: "$items.Lakehouse.Sample_LH.id" # PROD Sample_LH Lakehouse GUID
    - find_value: "123e4567-e89b-12d3-a456-426614174000" # Workspace ID
      replace_value:
          PPE: "$workspace.id" # PPE workspace ID
          PROD: "$workspace.id" # PROD workspace ID
    - find_value: "serverconnectionstringexample.com" # SQL endpoint connection string
      replace_value:
          PPE: "$items.Lakehouse.Sample_LH.sqlendpoint" # PPE Sample_LH Lakehouse sql endpoint
          PROD: "$items.Lakehouse.Sample_LH.sqlendpoint" # PROD Sample_LH Lakehouse sql endpoint
    - find_value: "https://trd-a1b2c3d4e5f6g7h8i9.z4.kusto.fabric.microsoft.com" # Eventhouse query service URI
      replace_value:
          PPE: "$items.Eventhouse.Sample_EH.queryserviceuri" # PPE Sample_EH Eventhouse query service URI
          PROD: "$items.Eventhouse.Sample_EH.queryserviceuri" # PROD Sample_EH Eventhouse query service URI
```

### Environment Variable Replacement

In the `find_replace` parameter, if the `enable_environment_variable_replacement` feature flag is set, pipeline/environment variables will be used to replace the values in the `parameter.yml` file with the corresponding values from the variables dictionary. **Only Environment Variable beginning with '$ENV:' will be used as replacement values.** See example below:

```yaml
find_replace:
    # Lakehouse GUID
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "$ENV:ppe_lakehouse"
          PROD: "$ENV:prod_lakehouse"
```

### File Filters

File filtering is supported in all parameters. This feature is optional and can be used to specify the files where replacement is intended to occur.

-   **Supported filters:** `item_type`, `item_name`, and `file_path`, see [more details](#supported-file-filters).
    -   **Note:** only `item_name` filter is supported in `spark_pool` parameter.
-   **Expected behavior:**
    -   If at least one filter value does not match, the replacement will be skipped for that file.
    -   If none of the optional filter fields or values are provided, the value found in _any_ repository file is subject to replacement.
-   **Filter input:**
    -   Input values are **case sensitive**.
    -   Input values must be **string** or **array** (enables one or many values to filter on).
        -   YAML supports array inputs using bracket ( **[ ]** ) or dash ( **-** ) notation.

<span class="md-h4-nonanchor">find_replace/key_value_replace</span>

```yaml
<find_replace | key_value_replace>:
    # Required fields: value must be a string
    - <find_value | find_key>: <find-this-value>
      replace_value:
          <environment-1-key>: <replace-with-this-value>
          <environment-2-key>: <replace-with-this-value>
      # Optional fields
      # Filter values must be a string or array of strings
      item_type: <item-type-filter-value>
      item_name: <item-name-filter-value>
      file_path: <file-path-filter-value>
```

<span class="md-h4-nonanchor">spark_pool</span>

```yaml
spark_pool:
    # Required fields: value must be a string
    - instance_pool_id: <instance-pool-id-value>
      replace_value:
          <environment-1-key>:
              type: <Capacity-or-Workspace>
              name: <pool-name>
          <environment-2-key>:
              type: <Capacity-or-Workspace>
              name: <pool-name>
      # Optional field: value must be a string or array of strings
      item_name: <item-name-filter-value>
```

### \_ALL\_ Environment Key in `replace_value`

The `_ALL_` environment key (case-insensitive) in `replace_value` is supported for all parameter types (`find_replace`, `key_value_replace`, `spark_pool`) and applies the replacement to any target environment. When `_ALL_` is used, it must be the only environment key in the `replace_value` dictionary. Using `ALL` without underscores will be treated as a regular environment key.

Use case: when the same replacement value applies to all target environments (particularly valuable in dynamic replacement scenarios).

```yaml
find_replace:
    # Lakehouse GUID
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          # use _ALL_ or _all_ or _All_
          _ALL_: "$items.Lakehouse.Example_LH.id"
```

## Optional Fields

When optional fields are omitted or left empty, only basic parameterization functionality will be available. To enable advanced features, you must add the specific optional field(s) (if applicable) and set appropriately.

**Important:**

-   String input values should be wrapped in quotes. Remember to escape special characters, such as **\\** in `file_path` inputs.
-   `is_regex` and filter fields can be used in the same parameter configuration.

### Regex Pattern Match

#### `is_regex`

-   Only applicable to the `find_replace` parameter.
-   Include `is_regex` field when setting the `find_value` to a **valid regex pattern.**
-   When the `is_regex` field is set to the **string** value `"true"` or `"True"` (case-insensitive), regex pattern matching is enabled.
-   When regex pattern matching is enabled, the `find_value` is interpreted as a regex pattern rather than a literal string.

### Supported File Filters

#### `item_type`

-   Item types must be valid and within scope of deployment.
-   See valid [types](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item?tabs=HTTP#itemtype).

#### `item_name`

-   Item names must match the exact names of items in the `repository_directory`.

#### `file_path`

-   `file_path` accepts three types of paths within the _repository directory_ boundary:
    -   **Absolute paths:** Full path starting from the drive root.
    -   **Relative paths:** Paths relative to the _repository directory_.
    -   **Wildcard paths:** Paths containing glob patterns.
-   When using _wildcard paths_:
    -   Common patterns include `*` (matches any characters in a filename), `**` (matches any directory depth).
    -   All matched files must exist within the _repository directory_.
    -   When using wildcard patterns, verify your syntax carefully to avoid unexpected matching behavior.
    -   **Examples:** `**/notebook-content.py` matches all notebook files in the repository directory, `Sample Pipelines/*.json` matches json files in the Sample Pipelines folder in the repository directory.

## Parameter File Validation

Validation of the `parameter.yml` file is a built-in feature of fabric-cicd, managed by the `Parameter` class. Validation is utilized in the following scenarios:

**Debuggability:** Users can debug and validate their parameter file to ensure it meets the acceptable structure and input value criteria before running a deployment. Simply run the `debug_parameterization.py` script located in the `devtools` directory.

**Deployment:** At the start of a deployment, an automated validation checks the validity of the `parameter.yml` file, if it is present. This step ensures that valid parameters are loaded, allowing deployment to run smoothly with correctly applied parameterized configurations. If the parameter file is invalid, the deployment will NOT proceed.

## Sample Parameter File

An exhaustive example of all capabilities currently supported in the `parameter.yml` file.

```yaml
find_replace:
    - find_value: "123e4567-e89b-12d3-a456-426614174000" # lakehouse GUID to be replaced
      replace_value:
          PPE: "f47ac10b-58cc-4372-a567-0e02b2c3d479" # PPE lakehouse GUID
          PROD: "9b2e5f4c-8d3a-4f1b-9c3e-2d5b6e4a7f8c" # PROD lakehouse GUID
      item_type: "Notebook" # filter on notebook files
      item_name: ["Hello World", "Goodbye World"] # filter on specific notebook files

    # enable_environment_variable_replacement feature flag to replace workspace ID
    - find_value: "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c" # workspace ID to be replaced
      replace_value:
          PPE: "$ENV:ppe_workspace_id" # PPE workspace ID (ENV variable)
          PROD: "$ENV:prod_workspace_id" # PROD workspace ID (ENV variable)
      file_path: # filter on notebook files with these paths
          - "/Hello World.Notebook/notebook-content.py"
          - "\\Goodbye World.Notebook\\notebook-content.py"

    # lakehouse GUID to be replaced (using regex pattern)
    - find_value: \#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          PPE: "$items.Lakehouse.Example_LH.id" # PPE lakehouse GUID (dynamic)
          PROD: "$items.Lakehouse.Example_LH.id" # PROD lakehouse GUID (dynamic)
      is_regex: "true" # enable regex pattern matching
      item_type: "Notebook" # filter on notebook files
      item_name: ["Hello World", "Goodbye World"] # filter on specific notebook files

    # lakehouse workspace ID to be replaced (using regex pattern)
    - find_value: \#\s*META\s+"default_lakehouse_workspace_id":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          _ALL_: "$workspace.id" # workspace ID of the target environment (dynamic)
      is_regex: "true" # enable regex pattern matching
      item_name: # filter on specific notebook files
          - "Hello World"
          - "Goodbye World"
      file_path: "**/notebook-content.py" # filter on notebook files using wildcard paths

key_value_replace:
    # SQL Server Connection to be replaced
    - find_key: $.properties.activities[?(@.name=="Load_Intake")].typeProperties.source.datasetSettings.externalReferences.connection
      replace_value:
          PPE: "6c517159-d27a-41d5-b71e-ca1ecff6542b" # PPE SQL Server Connection
          PROD: "6c517159-d27a-41d5-b71e-ca1ecff6542b" # PROD SQL Server Connection
      item_type: "DataPipeline" # filter on data pipeline files

spark_pool:
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b" # spark_pool_instance_id to be replaced
      replace_value:
          PPE:
              type: "Capacity" # target spark pool type, only supports Capacity or Workspace
              name: "CapacityPool_Medium" # target spark pool name
          PROD:
              type: "Capacity" # target spark pool type, only supports Capacity or Workspace
              name: "CapacityPool_Large" # target spark pool name
      item_name: "World" # filter on environment file for environment named "World"

    - instance_pool_id: "e7b8f1c4-4a6e-4b8b-9b2e-8f1e5d6a9c3d" # spark_pool_instance_id to be replaced
      replace_value:
          PPE:
              type: "Workspace" # target spark pool type, only supports Capacity or Workspace
              name: "WorkspacePool_Medium" # target spark pool name
      item_name: ["World_1", "World_2", "World_3"] # filter on environment files for environments with these names
```

## Examples by Item Type

### Notebooks

#### `find_replace` Parameterization Case

**Case:** A Notebook is attached to a Lakehouse which resides in different workspaces. The Workspace and Lakehouse GUIDs in the Notebook need to be updated to ensure the Notebook points to the correct Lakehouse once deployed.

**Solution:** In the `notebook-content.py` file, the default_lakehouse `47592d55-9a83-41a8-9b21-e1ef44264161`, and default_lakehouse_workspace_id `2190baad-a374-4114-addd-0dcf0533e69d` must be replaced with the corresponding GUIDs of the Lakehouse in the target environment (PPE/PROD/etc). This replacement is managed by the `find_replace` input in the `parameter.yml` file where fabric-cicd finds every instance of the string within the specified repository files and replaces it with the string for the deployed environment.

<span class="md-h4-nonanchor">parameter.yml file</span>

```yaml
find_replace:
    - find_value: "47592d55-9a83-41a8-9b21-e1ef44264161" # lakehouse GUID to be replaced
      replace_value:
          PPE: "a21e502a-51a5-4455-bb3d-6faf1e3e21fb" # PPE lakehouse GUID
          PROD: "1069f2ff-bb30-42a0-97b3-1f4655705b8a" # PROD lakehouse GUID
      item_type: "Notebook" # filter on notebook files
      item_name: ["Hello World", "Goodbye World"] # filter on specific notebook files
    - find_value: "2190baad-a374-4114-addd-0dcf0533e69d" # workspace ID to be replaced
      replace_value:
          PPE: "5a6ebbe6-9289-4105-b47c-cf158247b911" # PPE workspace ID
          PROD: "f9e8cbe0-2669-4e06-a026-7c75e5af8107" # PROD workspace ID
      file_path: # filter on notebook files with these paths
          - "/Hello World.Notebook/notebook-content.py"
          - "\\Goodbye World.Notebook\\notebook-content.py"
```

<span class="md-h4-nonanchor">notebook-content.py file</span>

```python
# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "47592d55-9a83-41a8-9b21-e1ef44264161",
# META       "default_lakehouse_name": "Example_LH",
# META       "default_lakehouse_workspace_id": "2190baad-a374-4114-addd-0dcf0533e69d"
# META     },
# META     "environment": {
# META       "environmentId": "a277ea4a-e87f-8537-4ce0-39db11d4aade",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Example_LH.Table1 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```

#### Advanced `find_replace` Parameterization Case

**Case:** A Notebook is attached to a Lakehouse which resides in the same workspace. When deploying both the Lakehouse and the Notebook to a target environment (PPE/PROD/etc), the Workspace and Lakehouse GUIDs referenced in the Notebook must be updated to ensure it correctly points to the corresponding Lakehouse in the new environment.

**Solution:** This approach uses `find_value` [**regex**](#find_value-regex)\*\* and [**dynamic variables**](#dynamic-replacement) to manage replacement. In the `find_replace` input in the `parameter.yml` file, the `is_regex` field is set to `"true"`, enabling fabric-cicd to find a string value within the _specified_ repository files that matches the provided regex pattern.

This approach is particularly useful for replacing values that are not known until deployment time, such as item IDs.

\*\*The regex pattern must include a capture group, defined using `()`, and the `find_value` must always match **group 1**. The value captured in this group will be dynamically replaced with the appropriate value for the deployed environment.

<span class="md-h4-nonanchor">parameter.yml file</span>

```yaml
find_replace:
    # lakehouse GUID matching group 1 of regex pattern to be replaced
    - find_value: \#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          PPE: "$items.Lakehouse.Example_LH.id" # PPE lakehouse GUID (dynamic)
          PROD: "$items.Lakehouse.Example_LH.id" # PROD lakehouse GUID (dynamic)
      is_regex: "true"
      item_type: "Notebook" # filter on notebook files
      item_name: ["Hello World", "Goodbye World"] # filter on specific notebook files
    # workspace ID matching group 1 of regex pattern to be replaced
    - find_value: \#\s*META\s+"default_lakehouse_workspace_id":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          PPE: "$workspace.id" # PPE workspace ID (dynamic)
          PROD: "$workspace.id" # PROD workspace ID (dynamic)
      is_regex: "true"
      file_path: # filter on notebook files with these paths
          - "/Hello World.Notebook/notebook-content.py"
          - "\\Goodbye World.Notebook\\notebook-content.py"
```

<span class="md-h4-nonanchor">notebook-content.py file</span>

```python
# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "123e4567-e89b-12d3-a456-426614174000",
# META       "default_lakehouse_name": "Example_LH",
# META       "default_lakehouse_workspace_id": "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c"
# META     },
# META     "environment": {
# META       "environmentId": "a277ea4a-e87f-8537-4ce0-39db11d4aade",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Example_LH.Table1 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```

### Data Pipelines

#### `key_value_replace` Parameterization Case

**Case:** A Data Pipeline is attached to data sources via the Connection Id. Connections are not deployed with fabric-cicd and therefore need to be parameterized. In the `pipeline-content.json` file, the SQL Server Connection Id `c517e095-ed87-4665-95fa-8cdb1e751fba`, must be replaced with the corresponding GUIDs of the SQL Server in the target environment (PPE/PROD/etc).

**Solution:** This replacement is managed by the `find_key` input in the `parameter.yml` file where fabric-cicd finds every instance of the key within the _specified_ repository files and replaces it with the string for the deployed environment.

<span class="md-h4-nonanchor">parameter.yml file</span>

```yaml
key_value_replace:
    - find_key: $.properties.activities[?(@.name=="Copy Data")].typeProperties.source.datasetSettings.externalReferences.connection
      replace_value:
          PPE: "f47ac10b-58cc-4372-a567-0e02b2c3d479" # PPE SQL Connection GUID
          PROD: "9b2e5f4c-8d3a-4f1b-9c3e-2d5b6e4a7f8c" # PROD SQL Connection GUID
      item_type: "DataPipeline" # filter on Data Pipelines files
      item_name: "Example Pipeline" # filter on specific Data Pipelines files
```

<span class="md-h4-nonanchor">pipeline-content.json file</span>

```json
{
    "properties": {
        "activities": [
            {
                "name": "Copy Data",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "queryTimeout": "02:00:00",
                        "partitionOption": "None",
                        "datasetSettings": {
                            "annotations": [],
                            "type": "AzureSqlTable",
                            "schema": [],
                            "typeProperties": {
                                "schema": "Dataprod",
                                "table": "DIM_Calendar",
                                "database": "unified"
                            },
                            "externalReferences": {
                                "connection": "c517e095-ed87-4665-95fa-8cdb1e751fba"
                            }
                        }
                    },
                    "sink": {
                        "type": "LakehouseTableSink",
                        "tableActionOption": "Append",
                        "datasetSettings": {
                            "annotations": [],
                            "linkedService": {
                                "name": "Unified",
                                "properties": {
                                    "annotations": [],
                                    "type": "Lakehouse",
                                    "typeProperties": {
                                        "workspaceId": "2d2e0ae2-9505-4f0c-ab42-e76cc11fb07d",
                                        "artifactId": "31dd665e-95f3-4575-9f46-70ea5903d89b",
                                        "rootFolder": "Tables"
                                    }
                                }
                            },
                            "type": "LakehouseTable",
                            "schema": [],
                            "typeProperties": {
                                "schema": "Dataprod",
                                "table": "DIM_Calendar"
                            }
                        }
                    },
                    "enableStaging": false,
                    "translator": {
                        "type": "TabularTranslator",
                        "typeConversion": true,
                        "typeConversionSettings": {
                            "allowDataTruncation": true,
                            "treatBooleanAsNumber": false
                        }
                    }
                }
            }
        ]
    }
}
```

### Environments

#### `spark_pool` Parameterization Case

**Case:** An Environment is attached to a Capacity level Custom Pool. Source control for Environments does not output the right fields necessary to deploy, so the Spark Pool needs to be parameterized. **Note:** Defining different names per environment is supported in the `parameter.yml` file. In the `Sparkcompute.yaml` file, the referenced instance_pool_id `72c68dbc-0775-4d59-909d-a47896f4573b` points to a capacity custom pool named `CapacityPool_Large` of pool type `Capacity` for the `PROD` environment.

**Solution:** This replacement is managed by the `spark_pool` input in the `parameter.yml` file where fabric-cicd finds every instance of the `instance_pool_id` and replaces it with the pool type and pool name for the _specified_ environment file.

<span class="md-h4-nonanchor">parameter.yml file</span>

```yaml
spark_pool:
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b" # spark_pool_instance_id to be replaced
      replace_value:
          PPE:
              type: "Capacity" # target spark pool type, only supports Capacity or Workspace
              name: "CapacityPool_Medium" # target spark pool name
          PROD:
              type: "Capacity" # target spark pool type, only supports Capacity or Workspace
              name: "CapacityPool_Large" # target spark pool name
      item_name: "World" # filter on environment file for environment named "World"
```

<span class="md-h4-nonanchor">Sparkcompute.yml</span>

```yaml
enable_native_execution_engine: false
instance_pool_id: 72c68dbc-0775-4d59-909d-a47896f4573b
driver_cores: 16
driver_memory: 112g
executor_cores: 16
executor_memory: 112g
dynamic_executor_allocation:
    enabled: false
    min_executors: 31
    max_executors: 31
runtime_version: 1.3
```

### Dataflows

Dataflows can have different kinds of Fabric sources and destinations that need to be parameterized, depending on the scenario.

#### Parameterization Overview

Take a Lakehouse source/destination as an example, the Lakehouse is connected to a Dataflow in the following ways:

1. Connection Id in the `queryMetadata.json` file:
    - Connections are not deployed with fabric-cicd and therefore need to be parameterized.
2. Workspace and item IDs in the `mashup.pq` file:
    - Source and/or destination item references, such as a Dataflow (source only\*\*), Lakehouse, Warehouse, etc. appear in the `mashup.pq` file and need to be parameterized to ensure proper deployment across environments.

**\*\*Note:** A Dataflow that sources from another Dataflow introduces a dependency that may require a specific order of deploying (source first then dependent). A Dataflow is referenced by the item ID in the workspace and the actual workspace ID, this makes re-pointing more complex (see parameterization guidance below).

#### Parameterization Guidance

Connections must be parameterized in addition to item references.

<span class="md-h4-nonanchor">Scenarios When Deploying a Dataflow that contains a source Dataflow reference:</span>

1. Source Dataflow exists in the **same workspace** as the dependent Dataflow:

    - The source Dataflow must be deployed BEFORE the dependent Dataflow (especially during first time deployment).
    - To handle this dependency correctly and prevent deployment errors, set up the `find_replace` parameter with the following requirements (incorrect setup may introduce failure during Dataflow deployment):
        - Set `find_value` to match the `dataflowId` GUID referenced in the `mashup.pq` file (literal string or [regex](#find_value-regex)).
        - Set `replace_value` to the variable `$items.Dataflow.<The Source Dataflow Name>.id`. **Important:** Make sure the **item type** is `"Dataflow"` and the **item name** matches the source Dataflow name in the repository directory exactly (case sensitive, include any spaces).
        - File filters are optional but recommended when using a regex pattern for `find_value`.
        - **You don't need to parameterize the source Dataflow workspace ID here** as the library automatically handles this replacement when you use the items variable in _this_ Dataflow scenario.
    - **How this works:** This parameterization approach ensures correct deployment of interdependent Dataflows while automatically updating references to point to the newly deployed Dataflow in the target workspace.
    - Example parameter input:

    ```yaml
    find_replace:
        # The ID of the source Dataflow referenced in mashup.pq
        - find_value: "0187104d-7a35-4abe-a2ca-a241ec81c8f1"
          # Type = Dataflow and Name = <The Source Dataflow Name>, Attribute = id
          replace_value:
              PPE: "$items.Dataflow.Source Dataflow.id"
              PROD: "$items.Dataflow.Source Dataflow.id"
          # Optional fields:
          file_path:
              - "\\Referencing Dataflow.Dataflow\\mashup.pq"
    ```

2. Source Dataflow exists in a **different workspace** from the dependent Dataflow:

    - When the source Dataflow exists in a different workspace, deployment order doesn't matter.
    - To re-point the source Dataflow from one workspace to another workspace, you can parameterize using the `find_replace` parameter. The Dataflow ID AND Workspace ID of the source Dataflow both need to be parameterized.
    - **Note:** dynamic replacement for item ID and workspace ID will NOT work here since the source Dataflow does not exist in the _repository directory_.

<span class="md-h4-nonanchor">Scenarios When Deploying a Dataflow that contains other Fabric items (e.g., Lakehouse, Warehouse, etc.) references:</span>

1. Source and/or destination item exists in the **same workspace** as the dependent Dataflow:

    - Use the `find_replace` parameter to update references so they point to the corresponding items in the target workspace.
    - You need to parameterize both the item ID and workspace ID found in the `mashup.pq` file.
    - Best practices for Dataflow parameterization:
        - Use a [regex](#find_value-regex) for the `find_value` to avoid hardcoding GUIDs and simplify maintenance
        - Use [dynamic replacement](#dynamic-replacement) to eliminate multi-phase deployments
    - Adding file filters to target specific Dataflow files provides more precise control.

2. Source/destination item exists in a **different workspace** from the dependent Dataflow:

    - Use the `find_replace` parameter to update references so they point to items in the different workspace.
    - Parameterize both the item ID and workspace ID found in the `mashup.pq` file.
    - Use a regex pattern for the `find_value` to avoid hardcoding GUIDs and simplify maintenance.
    - **Note:** dynamic replacement won't work in this scenario - it only works for items in the same workspace as the Dataflow.
    - Adding file filters helps target specific Dataflow files for more precise control.

#### Advanced `find_replace` Parameterization Case

**Case:** A Dataflow points to a destination Lakehouse. The Lakehouse exists in the same workspace as the Dataflow. In the `mashup.pq` file, the following GUIDs need to be replaced:

-   The workspaceId `e6a8c59f-4b27-48d1-ae03-7f92b1c6458d` with the target workspace Id.
-   The lakehouseId `3d72f90e-61b5-42a8-9c7e-b085d4e31fa2` with the corresponding Id of the Lakehouse in the target environment (PPE/PROD/etc).

**Solution:** These replacements are managed using a regex pattern as input for the `find_value` in the `parameter.yml` file, which finds the matching value in the _specified_ repository files and replaces it with the dynamically retrieved workspace or item Id of the target environment.

**Note:** While Connection IDs are shown in this example, they are not the main focus. Connection parameterization may vary depending on your specific scenario.

<span class="md-h4-nonanchor">parameter.yml file</span>

```yaml
find_replace:
    # Lakehouse workspace ID regex - matches the workspaceId GUID
    - find_value: Navigation_1\s*=\s*Pattern\{\[workspaceId\s*=\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"\]\}
      replace_value:
          PPE: "$workspace.id" # PPE workspace ID (dynamic)
          PROD: "$workspace.id"
      is_regex: "true" # Activate find_value regex matching
      file_path: "/Sample Dataflow.Dataflow/mashup.pq"

    # Lakehouse ID regex - matches the lakehouseId GUID
    - find_value: Navigation_2\s*=\s*Navigation_1\{\[lakehouseId\s*=\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"\]\}
      replace_value:
          PPE: "$items.Lakehouse.Sample_LH.id" # Sample_LH Lakehouse ID in PPE (dynamic)
          PROD: "$items.Lakehouse.Sample_LH.id"
      is_regex: "true" # Activate find_value regex matching
      file_path: "/Sample Dataflow.Dataflow/mashup.pq"

    # Connection ID - Cluster ID
    - find_value: "8e4f92a7-3c18-49d5-b6d0-7f2e591ca4e8"
      replace_value:
          PPE: "76a8f5c3-e4b2-48d1-9c7f-382d69a5e7b0" # PPE Cluster ID
          PROD: "f297e14d-6c83-42a5-b718-59d40e3f8c2d" # PROD Cluster ID
      file_path: "/Sample Dataflow.Dataflow/mashup.pq"

    # Connection ID - Datasource ID
    - find_value: "d12c5f7b-90a3-47e6-8d2c-3fb59e01d47a"
      replace_value:
          PPE: "25b9a417-3d8e-4f62-901c-75de6ba84f35" # PPE Datasource ID
          PROD: "cb718d96-5ae2-47fc-8b93-1d24c0f5e8a7" # PROD Datasource ID
      file_path: "/Sample Dataflow.Dataflow/mashup.pq"
```

<span class="md-h4-nonanchor">queryMetadata.json file</span>

```json
{
    "formatVersion": "202502",
    "computeEngineSettings": {},
    "name": "Sample Dataflow",
    "queryGroups": [],
    "documentLocale": "en-US",
    "queriesMetadata": {
        "Table": {
            "queryId": "ba67667b-14c0-4536-a92d-feafc73baa4b",
            "queryName": "Table",
            "loadEnabled": false
        },
        "Table_DataDestination": {
            "queryId": "a157a378-b510-4d95-bb82-5a7c80df8b4c",
            "queryName": "Table_DataDestination",
            "isHidden": true,
            "loadEnabled": false
        }
    },
    "connections": [
        {
            "path": "Lakehouse",
            "kind": "Lakehouse",
            "connectionId": "{\"ClusterId\":\"8e4f92a7-3c18-49d5-b6d0-7f2e591ca4e8\",\"DatasourceId\":\"d12c5f7b-90a3-47e6-8d2c-3fb59e01d47a\"}"
        }
    ]
}
```

<span class="md-h4-nonanchor">mashup.pq file</span>

```pq
[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Table = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45WckksSUzLyS9X0lEyBGKP1JycfKVYHYhEQGZBak5mXipQwgiIw/OLclLAkn75JalJ+fnZQEFjmC4FhHRwam5iXklmsm9+SmoOUN4EiMFsBVTzoRabArGLG0x/LAA=", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Item = _t, Id = _t, Name = _t]),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Item", type text}, {"Id", Int64.Type}, {"Name", type text}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "IsDataflow", each if [Item] = "Dataflow" then true else false), {{"IsDataflow", type logical}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom", "ContainsHello", each if Text.Contains([Name], "Hello") then 1 else 0), {{"ContainsHello", Int64.Type}})
in
  #"Added custom 1";
shared Table_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "e6a8c59f-4b27-48d1-ae03-7f92b1c6458d"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "3d72f90e-61b5-42a8-9c7e-b085d4e31fa2"]}[Data],
  TableNavigation = Navigation_2{[Id = "Items", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
```
