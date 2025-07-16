# Parameterization Advanced

## Advanced Features

<span class="md-h4-nonanchor">Find Value Regex</span>

-   In the `find_replace` parameter, the `find_value` can be set to a regex pattern instead of a literal string to find a value in the files to replace.
-   When a match is found, the `find_value` is assigned to the matched string and can be used to replace all occurrences of that value in the file.
-   How to use this feature:
    -   Set the `find_value` to a **valid regex pattern** wrapped in quotes.
    -   Include the optional field `is_regex` and set it to the value `"true"` (see Optional Fields -> is_regex).
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
    - find_value: \#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
      replace_value:
          PPE:
          PROD:
      # Optional field: Set to "true" to treat find_value as a regex pattern
      is_regex: "true" # "<true|True>"
      item_type: "Notebook" # filter on notebook files
      item_name: ["Hello World", "Goodbye World"] # filter on specific notebook files
```

<span class="md-h4-nonanchor">Dynamic Replacement</span>

-   In the `find_replace` parameter, the `replace_value` field supports variables that reference workspace or deployed item metadata:
-   **`$workspace.id`:** Replace value is the workspace ID of the target environment.
-   **`$items.type.name.attribute`:** Replace value is an attribute of a deployed item.
-   **Format:** Item type and name are **case-sensitive**. Enter the item name exactly as it appears, including spaces. For example: `$items.Notebook.Hello World.id`
-   **Supported attributes:** `id` (item ID) and `sqlendpoint`. Attributes should be lowercase.
-   **`id`:** Supported for any item type in scope.
-   **`sqlendpoint`:** Supported for Lakehouse and Warehouse items.
-   **Important:** If the specified item type or name does not exist in the deployed workspace, or if an invalid attribute is provided, or if the attribute value does not exist, the deployment will fail.
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

-   In the `find_replace` parameter, if the `enable_environment_variable_replacement` feature flag is set, pipeline/environment variables will be used to replace the values in the parameter.yml file with the corresponding values from the variables dictionary, see example below:
    **Only Environment Variable beginning with '$ENV:' will be used as replacement values.**

```yaml
find_replace:
    # Lakehouse GUID
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
      replace_value:
          PPE: "$ENV:ppe_lakehouse"
          PROD: "$ENV:prod_lakehouse"
```

<span class="md-h4-nonanchor">File Filters</span>

-   Supported for `find_replace`, `key_value_replace`, and `spark_pool`\* parameters.
-   File filters are optional and can be used to specify the files where replacement is intended to occur.
-   The available filters include: `item_type`, `item_name`, and `file_path` (see info for each field under Optional Fields).
-   If at least one filter value does not match, the replacement will be skipped for that file.
-   If none of the optional filter fields or values are provided, the value found in _any_ repository file is subject to replacement.
-   Input values are **case sensitive**.
-   Input values must be **string** or **array** (enables one or many values to filter on).
-   YAML supports array inputs using bracket ( **[ ]** ) or dash ( **-** ) notation.
-   \*Only `item_name` filter is supported in `spark_pool` parameter.

<span class="md-h4-nonanchor">find_replace</span>

```yaml
find_replace:
    # Required fields: value must be a string
    - find_value: <find-this-value>
      replace_value:
          <environment-1-key>: <replace-with-this-value>
          <environment-2-key>: <replace-with-this-value>
      # Optional fields
      # Filter values must be a string or array of strings
      item_type: <item-type-filter-value>
      item_name: <item-name-filter-value>
      file_path: <file-path-filter-value>
```

<span class="md-h4-nonanchor">key_value_replace</span>

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

## Optional Fields

-   General parameterization functionality is unaffected when optional fields are omitted or left empty.
-   Filter fields and `is_regex` fields are not mutually exclusive.
-   String inputs should be wrapped in quotes (make sure to properly escape characters, such as **\\** for `file_path` inputs).

<span class="md-h4-nonanchor">is_regex</span>

-   Only applicable to the `find_replace` parameter.
-   Function: include `is_regex` field when setting the `find_value` to a valid regex pattern.
-   When `is_regex` is set to the **string** value: `"true"` or `"True"` (case-insensitive), regex pattern matching is enabled and the `find_value` will be interpreted as a regex pattern. Otherwise, it's interpreted as a literal string.

<span class="md-h4-nonanchor">item_type</span>

-   Item types must be valid; see valid [types](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item?tabs=HTTP#itemtype).

<span class="md-h4-nonanchor">item_name</span>

-   Item names must match the exact names of items in the `repository_directory`.

<span class="md-h4-nonanchor">file_path</span>

-   `file_path` accepts three types of paths within the _repository directory_ boundary:
    -   **Absolute paths:** Full path starting from the drive root.
    -   **Relative paths:** Paths relative to the _repository directory_.
    -   **Wildcard paths:** Paths containing glob patterns.
-   When using wildcard paths:
    -   Common patterns include `*` (matches any characters in a filename), `**` (matches any directory depth).
    -   All matched files must exist within the _repository directory_.
    -   **Examples:** `**/notebook-content.py` matches all notebook files in the repository, `Sample Pipelines/*.json` matches json files in the Sample Pipelines folder.

## Parameter File Validation

Validation of the `parameter.yml` file is a built-in feature of fabric-cicd, managed by the `Parameter` class. Validation is utilized in the following scenarios:

**Debuggability:** Users can debug and validate their parameter file to ensure it meets the acceptable structure and input value requirements before running a deployment. Simply run the `debug_parameterization.py` script located in the `devtools` directory.

**Deployment:** At the start of a deployment, an automated validation checks the validity of the `parameter.yml` file, if it is present. This step ensures that valid parameters are loaded, allowing deployment to run smoothly with correctly applied parameterized configurations. If the parameter file is invalid, the deployment will not proceed.
