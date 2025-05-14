# Item Types

## Data Pipelines

-   **Parameterization:**
    -   Activities connected to items that exist in a different workspace will always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Activities connected to items within the same workspace are re-pointed to the new item in the target workspace.
-   **Connections** are not source controlled and must be created manually.
-   If you are using connections and expect them to change between different environments, then those need to be parameterized in the parameter.yml file.
-   The **executing identity** of the deployment must have access to the connections, or the deployment will fail.

## Environments

-   **Parameterization:**
    -   Environments attached to custom spark pools attach to the default starter pool unless parameterized in the `spark_pools` section of the `parameter.yml` file.
    -   The `find_replace` section in the `parameter.yml` file is not applied to Environments.
-   **Resources** are not source controlled and will not be deployed.
-   Environments with libraries will have **high initial publish times** (sometimes 20+ minutes).

## Lakehouses

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   **Shortcut** publish is disabled by default (for now), enable with feature flag `enable_shortcut_publish`.
-   **Schemas are not deployed** unless the schema has a shortcut present.
-   **Unpublish** is disabled by default, enable with feature flag `enable_lakehouse_unpublish`.

## Warehouses

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   **Warehouse content is not deployed** deployment is of the empty item only, warehouse DDL must be deployed separately via dacpac or other tools such as dbt.
-   **Case insensitive collation is supported** custom collation must be manually edited in the `.platform` file creation payload. See [How to: Create a warehouse with case-insensitive (CI) collation
    ](https://learn.microsoft.com/en-us/fabric/data-warehouse/collation) for more details.
-   **Unpublish** is disabled by default, enable with feature flag `enable_warehouse_unpublish`.

## Mirrored Database

-   **Parameterization:**
    -   Connections will always point to the original source database unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   **Initial deployment** for Azure SQL Database or Azure SQL Managed Instance requires manual granting of System Assigned Managed Identity (SAMI) Read and Write permission to the mirrored database for replication to be successful after deployment. ref -> ([Prerequisites](https://learn.microsoft.com/en-us/fabric/database/mirrored-database/mirrored-database-rest-api#create-mirrored-database))
-   **Unpublish** - a warning is shown for any default Semantic Models created by the Mirror Database. This is a current limitation of the Fabric API and can be ignored.

## Notebooks

-   **Parameterization:**
    -   Notebooks attached to lakehouses always point to the original lakehouse unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   **Resources** are not source controlled and will not be deployed.

## Reports

-   **Parameterization:**
    -   Reports connected to Semantic Models outside of the same workspace always point to the original Semantic Model unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Reports connected to Semantic Models within the same workspace are re-pointed to the new item in the target workspace.

## Semantic Models

-   **Parameterization:**
    -   Semantic Models connected to sources outside of the same workspace always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Semantic Models connected to sources within the same workspace may or may not be re-pointed; it is best to test this before taking a dependency. Use the `find_replace` section of the `parameter.yml` file as needed.
-   **Initial deployment** requires manual configuration of the connection after deployment.

## Variable Libraries

-   **Parameterization:**
    -   The active value set of the variable library is defined by the `environment` field passed into the `FabricWorkspace` object. If no `environment` is specified, the active Value Set will not be changed.
-   **Changing Value Sets:**
    -   Variable Libraries do not support programmatically changing the name of value set which is active
    -   After the initial deployment, if an active set is renamed, or removed, the deployment will fail
    -   Manual intervention will be required to make the necessary changes in the Fabric UI and then restart the deployment

## Copy Job

-   **Parameterization:**
    -   Connections will always point to the original data source unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   **Initial deployment** requires manual configuration of the connection after deployment.

## Activator

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   **Initial deployment** may not reflect streaming data immediately.
-   **Reflex** is the item name in source control. Source control may not support all activators/reflexes, as not all sources are compatible.

## Eventhouse

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   The `exclude_path` variable is required when deploying an **Eventhouse** that is attached to a **KQL Database** (common scenario).
-   There may be significant _differences_ in the streaming data between the source eventhouse and the deployed eventhouse.

## Eventstream

-   **Parameterization:**
    -   Destinations connected to items that exist in a different workspace will always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Destinations connected to items within the same workspace are re-pointed to the new item in the target workspace.
-   **Initial deployment** requires waiting for the table to populate in the destination lakehouse if a lakehouse destination is present in the eventstream.

## KQL Database

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   In Fabric, a KQL database is not a standalone item. However, during deployment, it is treated as such. Its source control files are located within a `.children` folder under the directory of the attached eventhouse.
-   Data in KQL database tables is not source controlled and may not consistently appear in the database UI after deployment. Some tables may be empty post-deployment.

## KQL Queryset

-   **Parameterization:**
    -   KQL querysets attached to KQL databases always point to the original KQL database unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   The **cluster/query URI** of the KQL database must be present in the KQL queryset JSON for rebinding. If the KQL queryset is attached to a KQL database within the same workspace, the cluster URI value is empty and needs to be re-added. `fabric ci-cd` handles this automatically.
-   KQL querysets can still exist after the KQL database source has been deleted. However, errors will reflect in the KQL queryset.
