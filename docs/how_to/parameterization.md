# Parameterization

To handle environment-specific values committed to git, use a `parameter.yml` file. This file supports programmatically changing values based on the `environment` field in the `FabricWorkspace` class. If the environment value is not found in the `parameter.yml` file, any dependent replacements will be skipped.

Raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml) for additional parameterization capabilities.

## find_replace

For generic find-and-replace operations. This will replace every instance of a specified string in every file. Specify the `find` value as the key and the `replace` value for each environment. See the [Example](example.md) page for a complete yaml file and an example use case below.

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

## Example Use Case

When deploying the `Example` notebook from a feature workspace to PPE or PROD environments, the attached `Example_LH` lakehouse id needs to be updated. This update ensures the notebook points to the correct lakehouse in the respective environments.

In the `notebook-content.py` file, the referenced lakehouse guid `123e4567-e89b-12d3-a456-426614174000` must be replaced with the corresponding guid for `Example_LH` lakehouse in the target environment. This replacement is managed by the library, which takes the `find_replace` input in `Parameter.yml` and finds every instance of the guid string within the repository files and replaces it with the guid string for the deployed environment.

Note: In this example, the lakehouse workspace id in the notebook file is also replaced using `Parameter.yml`.

<p style="font-size: 1.25em; font-weight: 500; color: #012826">Parameters.yml file</p>

```yaml
find_replace:
    "123e4567-e89b-12d3-a456-426614174000": # lakehouse guid to be replaced
        PPE: "f47ac10b-58cc-4372-a567-0e02b2c3d479" # PPE lakehouse guid
        PROD: "9b2e5f4c-8d3a-4f1b-9c3e-2d5b6e4a7f8c" # PROD lakehouse guid
    "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c": # workspace id to be replaced
        PPE: "d4e5f6a7-b8c9-4d1e-9f2a-3b4c5d6e7f8a" # PPE workspace id
        PROD: "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d" # PROD workspace id
```

<p style="font-size: 1.25em; font-weight: 500; color: #012826">notebook-content.py file</p>

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

print("Example notebook")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
