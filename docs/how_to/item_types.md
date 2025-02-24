# Item Types

## Supported Item Types

<!--BEGIN-SUPPORTED-ITEM-TYPES-->
<!--END-SUPPORTED-ITEM-TYPES-->

## Guidance and Known Limitations

### Data Pipelines

-   **Parameterization:**
    -   Activities connected to items outside of the same workspace always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Activities connected to items within the same workspace are re-pointed to the new item in the target workspace.
-   **Connections:** Not source controlled and must be created manually.
-   **Deployment Identity:** The identity running the deployment must have access to the connections, or the deployment will fail.

### Environments

-   **Parameterization:**
    -   Environments attached to custom spark pools attach to the default starter pool unless parameterized in the `spark_pools` section of the `parameter.yml` file.
    -   The `find_replace` section in the `parameter.yml` file is not applied to Environments.
-   **Resources:** Not source controlled and will not be deployed.
-   **Publish Time:** Environments with libraries may have high initial publish times (sometimes 20+ minutes).

### Lakehouses

-   **Parameterization:**
    -   The `find_replace` section in the `parameter.yml` file is not applied.
-   **Shortcuts:** Not deployed with Lakehouses.

### Notebooks

-   **Parameterization:**
    -   Notebooks attached to lakehouses always point to the original lakehouse unless parameterized in the `find_replace` section of the `parameter.yml` file.
-   **Resources:** Not source controlled and will not be deployed.

### Reports

-   **Parameterization:**
    -   Reports connected to Semantic Models outside of the same workspace always point to the original Semantic Model unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Reports connected to Semantic Models within the same workspace are re-pointed to the new item in the target workspace.

### Semantic Models

-   **Parameterization:**
    -   Semantic Models connected to sources outside of the same workspace always point to the original item unless parameterized in the `find_replace` section of the `parameter.yml` file.
    -   Semantic Models connected to sources within the same workspace may or may not be re-pointed; it is best to test this before taking a dependency. Use the `find_replace` section of the `parameter.yml` file as needed.
-   **Initial Deployment:** Requires manual configuration of the connection after deployment.
