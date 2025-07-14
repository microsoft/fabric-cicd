# Parameterization

To handle environment-specific values committed to git, use a `parameter.yml` file. This file supports programmatically changing values based on the `environment` field passed into the `FabricWorkspace` object. If the environment value is not found in the `parameter.yml` file, any dependent replacements will be skipped. This file should sit in the root of the `repository_directory` folder specified in the FabricWorkspace object.

**Important Notice:** The `parameter.yml` file structure has been recently updated. Please refer to the documentation below for important changes.

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

## Inputs

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

<span class="md-h4-nonanchor">Dynamic Replacement</span>

The `replace_value` field supports variables that reference workspace or deployed item metadata:

-   **`$workspace.id`**: Replace value is the workspace ID of the target environment.
-   **`$items.type.name.attribute`**: Replace value is an attribute of a deployed item.
-   **Format**: Item type and name are **case-sensitive**. Enter the item name exactly as it appears, including spaces. For example: `$items.Notebook.Hello World.id`
-   **Supported attributes**: `id` (item ID) and `sqlendpoint`. Attributes should be lowercase.
-   **Important**: If the specified item type or name does not exist in the deployed workspace, or if an invalid attribute is provided, or if the attribute value does not exist, the deployment will fail.
-   For an in-depth example, see the **advanced notebook example**.

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
```

<span class="md-h4-nonanchor">Environment Variable Replacement</span>

If the `enable_environment_variable_replacement` feature flag is set, pipeline/environment variables will be used to replace the values in the parameter.yml file with the corresponding values from the variables dictionary, see example below:
**Only Environment Variable beginning with '$ENV:' will be used as replacement values.**

```yaml
find_replace:
    # Lakehouse GUID
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "$ENV:ppe_lakehouse"
          PROD: "$ENV:prod_lakehouse"
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

### Optional Fields

-   General parameterization functionality is unaffected when optional fields are omitted or left empty.
-   String values should be wrapped in quotes (make sure to escape characters, such as **\\** in `file_path` inputs).

<span class="md-h4-nonanchor">is_regex</span>

-   The `is_regex` field must be set to the **string** `"true"` to enable regex pattern matching (not case-sensitive, `"True"` is also accepted).
-   When `is_regex` is present and properly set, the `find_value` will be interpreted as a regex pattern. Otherwise, it's interpreted as a literal string.
-   Ensure that all special characters in the regex pattern are properly **escaped**.
-   **Important**: The user is solely **responsible for providing a valid and correctly matching regex pattern**. If the pattern is invalid (i.e., it cannot be compiled) or fails to match any content in the target files, the deployment will fail.
-   **Important**: When using a regex `find_value`, enclose only the exact value you want to replace in parentheses `( )`. This creates **capture group 1**, which must always be used as the replacement target. Be sure to include the **surrounding context** such as property names, quotes, etc. in the pattern to ensure it matches the correct value and not a value with a similar format elsewhere in the file. Capture group 1 should isolate values like a GUID, SQL connection string, etc. Once a match is found, the `find_value` is assigned to the matched string and can be used to replace all occurrences of that value in the file. **For example**, if you're using a regex `find_value` to match a lakehouse ID attached to a notebook, avoid writing a pattern that matches only the GUID format. Doing so would risk replacing any matching GUID in the file, not just the intended one. Instead, include the surrounding context in your pattern—such as `# META "default_lakehouse": "123456"`—and capture only the `123456` GUID in group 1. This ensures that only the correct, context-specific GUID is replaced.

<span class="md-h4-nonanchor">File Filters</span>

-   Optional filter values that _are_ provided must match the corresponding properties in the repository file in order for the replacement to occur in the given file. If at least one filter value does not match, the replacement will be skipped for that file.
-   If none of the optional filter fields or values are provided, the value found in _any_ repository file is subject to replacement.
-   Input values are **case sensitive**.
-   Input values must be **string** or **array** (enables one or many values to filter on).
-   YAML supports array inputs using bracket ( **[ ]** ) or dash ( **-** ) notation.
-   Item types must be valid; see valid [types](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item?tabs=HTTP#itemtype).
-   `file_path` accepts absolute or relative paths. Relative paths must be relative to the _repository directory_.

### Parameter File Validation

Validation of the `parameter.yml` file is a built-in feature of fabric-cicd, managed by the `Parameter` class. Validation is utilized in the following scenarios:

**Debuggability:** Users can debug and validate their parameter file to ensure it meets the acceptable structure and input value requirements before running a deployment. Simply run the `debug_parameterization.py` script located in the `devtools` directory.

**Deployment:** At the start of a deployment, an automated validation checks the validity of the `parameter.yml` file, if it is present. This step ensures that valid parameters are loaded, allowing deployment to run smoothly with correctly applied parameterized configurations. If the parameter file is invalid, the deployment will not proceed.

## Sample File

An exhaustive example of all capabilities currently supported in the `parameter.yml` file.

```yaml
find_replace:
    - find_value: "123e4567-e89b-12d3-a456-426614174000" # lakehouse GUID to be replaced
      replace_value:
          PPE: "f47ac10b-58cc-4372-a567-0e02b2c3d479" # PPE lakehouse GUID
          PROD: "9b2e5f4c-8d3a-4f1b-9c3e-2d5b6e4a7f8c" # PROD lakehouse GUID
      item_type: "Notebook" # filter on notebook files
      item_name: ["Hello World", "Goodbye World"] # filter on specific notebook files
    - find_value: "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c" # workspace ID to be replaced
      replace_value:
          PPE: "$ENV:ppe_workspace_id" # enable_environment_variable_replacement feature flag
          PROD: "$ENV:prod_workspace_id" # enable_environment_variable_replacement feature flag
      file_path: # filter on notebook files with these paths
          - "/Hello World.Notebook/notebook-content.py"
          - "\\Goodbye World.Notebook\\notebook-content.py"
    # lakehouse GUID to be replaced (using regex pattern)
    - find_value: \#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          PPE: "$items.Lakehouse.Example_LH.id" # PPE lakehouse GUID (dynamic)
          PROD: "$items.Lakehouse.Example_LH.id" # PROD lakehouse GUID (dynamic)
      is_regex: "true"
      item_type: "Notebook" # filter on notebook files
      item_name: ["Hello World", "Goodbye World"] # filter on specific notebook files
    # lakehouse workspace ID to be replaced (using regex pattern)
    - find_value: \#\s*META\s+"default_lakehouse_workspace_id":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          PPE: "$workspace.id" # PPE workspace ID (dynamic)
          PROD: "$workspace.id" # PROD workspace ID (dynamic)
      is_regex: "true"
      file_path: # filter on notebook files with these paths
          - "/Hello World.Notebook/notebook-content.py"
          - "\\Goodbye World.Notebook\\notebook-content.py"

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

## Examples

### Notebooks

<span class="md-h4-nonanchor">Basic Example</span>

A Notebook is attached to a Lakehouse which resides in different workspaces. The Workspace and Lakehouse GUIDs in the Notebook need to be updated to ensure the Notebook points to the correct Lakehouse once deployed.

In the `notebook-content.py` file, the default_lakehouse `47592d55-9a83-41a8-9b21-e1ef44264161`, and default_lakehouse_workspace_id `2190baad-a374-4114-addd-0dcf0533e69d` must be replaced with the corresponding GUIDs of the Lakehouse in the target environment (PPE/PROD/etc).

This replacement is managed by the `find_replace` input in the `parameter.yml` file where fabric-cicd finds every instance of the string within the _specified_ repository files and replaces it with the string for the deployed environment.

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

<span class="md-h4-nonanchor">Advanced Example</span>

A Notebook is attached to a Lakehouse which resides in the same workspace. When deploying both the Lakehouse and the Notebook to a target environment (PPE/PROD/etc), the Workspace and Lakehouse GUIDs referenced in the Notebook must be updated to ensure it correctly points to the corresponding Lakehouse in the new environment.

This approach uses a regex pattern and dynamic variables to manage replacement. In the `find_replace` input in the `parameter.yml` file, the `is_regex` field is set to `"true"`, enabling fabric-cicd to find a string value within the _specified_ repository files that matches the provided regex pattern.

The regex pattern must include a capture group, defined using `()`, and the `find_value` must always match **group 1**. The value captured in this group will be dynamically replaced with the appropriate value for the deployed environment.

This approach is particularly useful for replacing values that are not known until deployment time, such as item IDs.

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

A Data Pipeline is attached to data sources via the Connection Id. Connections are not deployed with fabric-cicd and therefore need to be parameterized.

In the `pipeline-content.json` file, the SQL Server Connection Id `c517e095-ed87-4665-95fa-8cdb1e751fba`, must be replaced with the corresponding GUIDs of the SQL Server in the target environment (PPE/PROD/etc).

This replacement is managed by the `find_key` input in the `parameter.yml` file where fabric-cicd finds every instance of the key within the _specified_ repository files and replaces it with the string for the deployed environment.

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

An Environment is attached to a Capacity level Custom Pool. Source control for Environments does not output the right fields necessary to deploy, so the Spark Pool needs to be parameterized.

**Note:** Defining different names per environment is now supported in the `parameter.yml` file.

In the `Sparkcompute.yaml` file, the referenced instance_pool_id `72c68dbc-0775-4d59-909d-a47896f4573b` points to a capacity custom pool named `CapacityPool_Large` of pool type `Capacity` for the `PROD` environment.

This replacement is managed by the `spark_pool` input in the `parameter.yml` file where fabric-cicd finds every instance of the `instance_pool_id` and replaces it with the pool type and pool name for the _specified_ environment file.

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

Dataflows connect to source/destination items, such as a Lakehouse through:

1. Connection Id in the `queryMetadata.json` file.
2. Workspace and item Ids in the `mashup.pq` file.

Connections are not deployed with fabric-cicd and therefore need to be parameterized. Referenced items in the `mashup.pq` file that exist in the same workspace as the Dataflow need to be parameterized to ensure proper deployment across environments.

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

For example, in the `mashup.pq` file, the following GUIDs need to be replaced:

-   The workspaceId `e6a8c59f-4b27-48d1-ae03-7f92b1c6458d` with the target workspace Id.
-   The lakehouseId `3d72f90e-61b5-42a8-9c7e-b085d4e31fa2` with the corresponding Id of the Lakehouse in the target environment (PPE/PROD/etc).

These replacements are managed using a regex pattern as input for the `find_value` in the `parameter.yml` file, which finds the matching value in the _specified_ repository files and replaces it with the dynamically retrieved workspace or item Id of the target environment.

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
```
