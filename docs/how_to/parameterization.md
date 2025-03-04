# Parameterization

To handle environment-specific values committed to git, use a `parameter.yml` file. This file supports programmatically changing values based on the `environment` field passed into the `FabricWorkspace` object. If the environment value is not found in the `parameter.yml` file, any dependent replacements will be skipped. This file should sit within the same `repository_directory` folder specified in the FabricWorkspace object.

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

For generic find-and-replace operations. This will replace every instance of a specified string in every file. Specify the `find` value as the key and the `replace` value for each environment.

Note: A common use case for this function is to replace connection strings, e.g., find and replace a connection GUID referenced in a data pipeline.

```yaml
find_replace:
    <find-this-value>:
        <environment-1>: <replace-with-this-value>
        <environment-2>: <replace-with-this-value>
```

### `spark_pool`

Environments attached to custom spark pools need to be parameterized because the `instance-pool-id` in the `Sparkcompute.yml` file isn't supported in the create/update environment APIs. Provide the `instance-pool-id` as the key, and the pool type and name as the values.

Environment parameterization (PPE/PROD) is not supported. If needed, raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml).

```yaml
spark_pool:
    <instance-pool-id>:
        type: <Capacity-or-Workspace>
        name: <pool-name>
```

## Sample File

An exhaustive example of all capabilities currently supported in the `parameter.yml` file.

```yaml
find_replace:
    "123e4567-e89b-12d3-a456-426614174000": # lakehouse GUID to be replaced
        PPE: "f47ac10b-58cc-4372-a567-0e02b2c3d479" # PPE lakehouse GUID
        PROD: "9b2e5f4c-8d3a-4f1b-9c3e-2d5b6e4a7f8c" # PROD lakehouse GUID
    "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c": # workspace ID to be replaced
        PPE: "d4e5f6a7-b8c9-4d1e-9f2a-3b4c5d6e7f8a" # PPE workspace ID
        PROD: "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d" # PROD workspace ID

spark_pool:
    "72c68dbc-0775-4d59-909d-a47896f4573b": # spark_pool_instance_id to be replaced
        type: "Capacity" # target spark pool type, only supports Capacity or Workspace
        name: "CapacityPool_Large" # target spark pool name
    "e7b8f1c4-4a6e-4b8b-9b2e-8f1e5d6a9c3d": # spark_pool_instance_id to be replaced
        type: "Workspace" # target spark pool type, only supports Capacity or Workspace
        name: "WorkspacePool_Medium" # target spark pool name
```

## Examples

### Notebooks

A Notebook is attached to a Lakehouse which resides in different workspaces. The Workspace and Lakehouse GUIDs in the Notebook need to be updated to ensure the Notebook points to the correct Lakehouse once deployed.

In the `notebook-content.py` file, the default_lakehouse `123e4567-e89b-12d3-a456-426614174000`, and default_lakehouse_workspace_id `8f5c0cec-a8ea-48cd-9da4-871dc2642f4c` must be replaced with the corresponding GUIDs of the Lakehouse in the target environment (PPE/Prod/etc).

This replacement is managed by the `find_replace` input in the `parameter.yml` file where fabric-cicd finds every instance of the string within the repository files and replaces it with the string for the deployed environment.

<span class="md-h4-nonanchor">parameter.yml file</span>

```yaml
find_replace:
    "123e4567-e89b-12d3-a456-426614174000": # lakehouse GUID to be replaced
        PPE: "f47ac10b-58cc-4372-a567-0e02b2c3d479" # PPE lakehouse GUID
        PROD: "9b2e5f4c-8d3a-4f1b-9c3e-2d5b6e4a7f8c" # PROD lakehouse GUID
    "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c": # workspace ID to be replaced
        PPE: "d4e5f6a7-b8c9-4d1e-9f2a-3b4c5d6e7f8a" # PPE workspace ID
        PROD: "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d" # PROD workspace ID
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

### Environments

An Environment is attached to a Capacity level Custom Pool. Source control for Environments does not output the right fields necessary to deploy, so the Spark Pool needs to be parameterized. For now, the recommendation is to maintain the same pool name in all environments. Defining different names per environment is currently not supported in the `parameter.yml` file.

In the `Sparkcompute.yaml` file, the referenced instance_pool_id `72c68dbc-0775-4d59-909d-a47896f4573b` points to a capacity custom pool named `CapacityPool_Large` of pool type `Capacity`.

This replacement is managed by the `spark_pool` input in the `parameter.yml` file where fabric-cicd finds every instance of the instance_pool_id and replaces it with the pool type and pool name.

<span class="md-h4-nonanchor">parameter.yml file</span>

```yaml
spark_pool:
    "72c68dbc-0775-4d59-909d-a47896f4573b": # spark_pool_instance_id to be replaced
        type: "Capacity" # target spark pool type, only supports Capacity or Workspace
        name: "CapacityPool_Large" # target spark pool name
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
