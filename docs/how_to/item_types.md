# Item Types

## Data Pipelines

-   **Parameterization:**
    -   Activities connected to items that exist in a different workspace will always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Activities connected to items within the same workspace are re-pointed to the new item in the target workspace.
-   **Connections** are not source controlled and must be created manually.
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
