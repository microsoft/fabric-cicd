# Item Types

## Activators

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   **Initial deployment** may not reflect streaming data immediately.
-   **Reflex** is the item name in source control. Source control may not support all activators/reflexes, as not all sources are compatible.

## Apache Airflow Jobs

-   **Parameterization:**
    -   The referenced items in DAG files will always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   **Connections** are not source controlled and must be created manually.
-   See known CI/CD limitations [here](https://learn.microsoft.com/en-us/fabric/data-factory/cicd-apache-airflow-jobs#known-limitations).

## API for GraphQL

-   **Parameterization:**
    -   The source will always point to the source in the original workspace unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   It is recommended to use the supported variables in `find_replace` for dynamic replacement of the source workspace and item IDs.
    -   If you are using a connection and expect it to change between different environments, then it needs to be parameterized in the `parameter.yml` file.
-   When using the **Saved Credential** method to connect to data sources, developers must have access to the Saved Credential information in order to successfully deploy GraphQL item.
-   Changes made to the original API query are not source controlled. You will need to manually update the query in the GraphQL item's query editor within the target workspace.
-   Only user authentication is currently supported for GraphQL items that source data from the SQL Analytics Endpoint.

## Azure Data Factory

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   Deploys a **mounted** Azure Data Factory to a Fabric Workspace.
-   Before deployment, ensure the Azure Data Factory resource exists with proper permissions.

## Copy Jobs

-   **Parameterization:**
    -   Connections will always point to the original data source unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   **Initial deployment** requires manual configuration of the connection after deployment.

## Dataflows

-   **Parameterization:**
    -   Source/destination items (e.g., Dataflow, Lakehouse, Warehouse) will always reference the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   The recommended approach for re-pointing source/destination items that exist in the _same_ workspace as the Dataflow is to use **`replace_value` variables** in the `find_replace` parameter along with a `find_value` regex (literal string works too). For more guidance, see [Parameterization -> Dataflows](parameterization.md#dataflows).
    -   **Important** for Dataflows that reference another Dataflow in the _same_ workspace, ONLY parameterize the referenced **dataflowId** in the `mashup.pq` file (workspaceId re-pointing is handled automatically in this approach) using the following `replace_value`: `$items.Dataflow.<The Source Dataflow Name>.id`. This ensures proper dependency resolution and ordered publishing.
-   **Initial deployment** will require a manual publish and refresh of the dataflow. After re-pointing dataflows during deployment, temporary errors may appear but should resolve after refreshing and allowing time for processing (especially when a dataflow sources from another dataflow in the target workspace).
-   `fabric ci-cd` automatically manages ordered deployment of dataflows that source from other dataflows in the same workspace.
-   **Connections** are not source controlled and require manual creation.
-   If you use connections that differ between environments, parameterize them in the `parameter.yml` file.

## Data Agents

-   **Parameterization:**
    -   Data source items (e.g., Lakehouse, Warehouse, Semantic Model) will always reference the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.

## Data Pipelines

-   **Parameterization:**
    -   Activities connected to items that exist in a different workspace will always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Activities connected to items within the same workspace are automatically re-pointed to the new item in the target workspace. **Note: Activities referencing items that don't use _logical ID_ and _default workspace ID_ (such as Refresh Dataflow and Refresh Semantic Model) require parameterization.**
-   **Connections** are not source controlled and must be created manually.
-   If you are using connections and expect them to change between different environments, then those need to be parameterized in the `parameter.yml` file.
-   The **executing identity** of the deployment must have access to the connections, or the deployment will fail.

## Environments

-   **Parameterization:**
    -   Environments attached to custom spark pools attach to the default starter pool unless parameterized in the `spark_pools` section of the `parameter.yml` file.
    -   The `find_replace` section in the `parameter.yml` file is not applied to Environments.
-   **Resources** are not source controlled and will not be deployed.
-   Environments with libraries will have **high initial publish times** (sometimes 20+ minutes).

## Eventhouses

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   The `exclude_path` variable is required when deploying an **Eventhouse** that is attached to a **KQL Database** (common scenario).
-   There may be significant _differences_ in the streaming data between the source eventhouse and the deployed eventhouse.
-   **Unpublish** is disabled by default, enable with feature flag `enable_eventhouse_unpublish`.

## Eventstreams

-   **Parameterization:**
    -   Destinations connected to items that exist in a different workspace will always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Destinations connected to items within the same workspace are re-pointed to the new item in the target workspace.
-   **Initial deployment** requires waiting for the table to populate in the destination lakehouse if a lakehouse destination is present in the eventstream.

## KQL Databases

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   In Fabric, a KQL database is not a standalone item. However, during deployment, it is treated as such. Its source control files are located within a `.children` folder under the directory of the attached eventhouse.
-   Data in KQL database tables is not source controlled and may not consistently appear in the database UI after deployment. Some tables may be empty post-deployment.

## KQL Querysets

-   **Parameterization:**
    -   KQL querysets attached to KQL databases always point to the original KQL database unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   The **cluster/query URI** of the KQL database must be present in the KQL queryset JSON for rebinding. If the KQL queryset is attached to a KQL database within the same workspace, the cluster URI value is empty and needs to be re-added. `fabric ci-cd` handles this automatically.
-   KQL querysets can still exist after the KQL database source has been deleted. However, errors will reflect in the KQL queryset.

## Lakehouses

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   **Shortcut** publish is disabled by default (for now), enable with feature flag `enable_shortcut_publish`.
-   **Schemas are not deployed** unless the schema has a shortcut present.
-   **Unpublish** is disabled by default, enable with feature flag `enable_lakehouse_unpublish`.

## Mirrored Databases

-   **Parameterization:**
    -   Connections will always point to the original source database unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   **Initial deployment** for Azure SQL Database or Azure SQL Managed Instance requires manual granting of System Assigned Managed Identity (SAMI) Read and Write permission to the mirrored database for replication to be successful after deployment. ref -> ([Prerequisites](https://learn.microsoft.com/en-us/fabric/database/mirrored-database/mirrored-database-rest-api#create-mirrored-database))
-   **Unpublish** - a warning is shown for any default Semantic Models created by the Mirror Database. This is a current limitation of the Fabric API and can be ignored.

## ML Experiments

-   **Only the ML Shell is created.** The create API does not support the creation an machine learning experiment with definition.

## Notebooks

-   **Parameterization:**
    -   Notebooks attached to lakehouses always point to the original lakehouse unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   **Resources** are not source controlled and will not be deployed.

## Real-Time Dashboard

-   **Parameterization:**
    -   Real-Time Dashboard attached to KQL databases always point to the original KQL database unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   The **cluster/query URI** of the KQL database(s) used in the dashboard must be present in the Real-Time Dashboard JSON for rebinding. If the Real-Time Dashboard is attached to a KQL database within the same workspace, the cluster URI value is empty and needs to be re-added. `fabric ci-cd` handles this automatically.

## Reports

-   **Parameterization:**
    -   Reports connected to Semantic Models outside of the same workspace always point to the original Semantic Model unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Reports connected to Semantic Models within the same workspace are re-pointed to the new item in the target workspace.

## Semantic Models

-   **Parameterization:**
    -   Semantic Models connected to sources outside of the same workspace always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Semantic Models connected to sources within the same workspace may or may not be re-pointed; it is best to test this before taking a dependency. Use the `find_replace` section of the `parameter.yml` file as needed.
-   **Initial deployment** requires manual configuration of the connection after deployment.

## SQL Databases

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   **SQL Database content is not deployed.** Only the item shell is deployed. The SQL database schema (DDL) must be deployed separately using a DACPAC or other tools such as dbt.
-   **Unpublish** is disabled by default, enable with feature flag `enable_sqldatabase_unpublish`.

## Variable Libraries

-   **Parameterization:**
    -   The active value set of the variable library is defined by the `environment` field passed into the `FabricWorkspace` object. If no `environment` is specified, the active Value Set will not be changed.
-   **Changing Value Sets:**
    -   Variable Libraries do not support programmatically changing the name of value set which is active
    -   After the initial deployment, if an active set is renamed, or removed, the deployment will fail
    -   Manual intervention will be required to make the necessary changes in the Fabric UI and then restart the deployment

## Warehouses

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   **Warehouse content is not deployed.** Only the item shell is deployed. Warehouse DDL must be deployed separately using a DACPAC or other tools such as dbt.
-   **Case insensitive collation is supported** custom collation must be manually edited in the `.platform` file creation payload. See [How to: Create a warehouse with case-insensitive (CI) collation](https://learn.microsoft.com/en-us/fabric/data-warehouse/collation) for more details.
-   **Unpublish** is disabled by default, enable with feature flag `enable_warehouse_unpublish`.
