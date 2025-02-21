# Parameterization

To handle environment-specific values committed to git, use a `parameter.yml` file. This file supports programmatically changing values based on the `environment` field in the `FabricWorkspace` class. If the environment value is not found in the `parameter.yml` file, any dependent replacements will be skipped.

Raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml) for additional parameterization capabilities.

## find_replace

For generic find-and-replace operations. This will replace every instance of a specified string in every file. Specify the `find` value as the key and the `replace` value for each environment. See the [Example](example.md) page for a complete yaml file.

Note: A common use case for this function is to replace connection strings. I.e. find and replace a connection guid referenced in data pipeline.

```yaml
find_replace:
    <find-this-value>:
        <environment-1>: <replace-with-this-value>
        <environment-2>: <replace-with-this-value>
```

## spark_pool

Environments attached to custom spark pools need to be parameterized because the `instance-pool-id` in the `Sparkcompute.yml` file isn't supported in the create/update environment APIs. Provide the `instance-pool-id` as the key, and the pool type and name as the values.

Environment parameterization(PPE/PROD) is not supported. If needed, raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml).

```yaml
spark_pool:
    <instance-pool-id>:
        type: <Capacity-or-Workspace>
        name: <pool-name>
```

## find_replace Example

`Hello World` notebook is attached to the `Hello_World_LH` lakehouse. When deploying `Hello World` from a feature workspace to environment workspaces PPE and PROD, the attached lakehouse needs to be updated to point to the correct lakehouse in the respective environments.

In the `notebook-content.py` file, the referenced lakehouse GUID `123e4567-e89b-12d3-a456-426614174000` needs to be replaced with the GUID of the `Hello_World_LH` lakehouse found in the PPE/PROD workspace. This replacement is handled by finding all instances of the GUID supplied by `Parameters.yml` in all files in the repository directory and replacing it with the GUID associated with the deployed environment.

Note: In this example, a `find_replace` operation would also be required to update the lakehouse workspace ID found in the notebook file.

### Parameters.yml

```yaml
find_replace:
    123e4567-e89b-12d3-a456-426614174000: # lakehouse GUID to be replaced
        PPE: f47ac10b-58cc-4372-a567-0e02b2c3d479 # PPE lakehouse GUID
        PROD: 9b2e5f4c-8d3a-4f1b-9c3e-2d5b6e4a7f8c # PROD lakehouse GUID
    8f5c0cec-a8ea-48cd-9da4-871dc2642f4c: # workspace ID to be replaced
        PPE: d4e5f6a7-b8c9-4d1e-9f2a-3b4c5d6e7f8a # PPE workspace ID
        PROD: a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d # PROD workspace ID
```

### notebook-content.py

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
# META       "default_lakehouse_name": "Hello_World_LH",
# META       "default_lakehouse_workspace_id": "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c"
# META     },
# META     "environment": {
# META       "environmentId": "a277ea4a-e87f-8537-4ce0-39db11d4aade",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

print("Hello World")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```
