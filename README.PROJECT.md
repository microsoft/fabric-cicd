# Fabric CI/CD Deployment Toolkit

This repository contains the deployment helper used to provision, deploy, and secure Microsoft Fabric workspaces for DEV, TEST, and PROD.

It is meant to be used from a local machine with Azure CLI sign-in and access to the Fabric tenant.

## What this toolkit does

The toolkit drives a full Fabric deployment from typed Python config in `fabriccicd_inputs/` and the CLI in `fabriccicd.py`.

It can:

- create or look up a Fabric workspace
- generate local item folders for the target environment
- deploy lakehouses, Spark environments, Spark Job Definitions, and pipelines
- seed demo tables and files
- apply workspace and lakehouse permissions
- create OneLake / ADLS shortcuts
- apply pipeline schedules

## What you need before starting

- Python 3.9 or later
- Azure CLI installed
- `az login` completed in the same shell session
- access to the Fabric tenant
- permission to create or use the target workspace
- a Fabric ADLS Gen2 connection for the shortcut, created manually in MSIT Fabric UI

## First run from scratch

You do **not** create a `fabric/` folder manually.

When you run deploy, the script creates it automatically.

### First-time flow

```powershell
python fabriccicd.py create DEV
python fabriccicd.py deploy DEV
```

### What happens on first run

1. `create DEV` resolves or creates the DEV workspace and stores the workspace ID.
2. `deploy DEV` generates the local environment folders automatically.
3. `deploy DEV` creates the `fabric/` folder if it does not exist.
4. The script creates the environment subfolder for the target environment, for example:
    - `fabric/dev/`
    - `fabric/test/`
    - `fabric/prod/`
5. The script then publishes the workspace content into Fabric.

If you only deploy DEV, only `fabric/dev/` is created first. TEST and PROD subfolders are created when those environments are deployed.

## Alias usage in this project

There are two different aliases used by design.

### 1. Developer workspace alias

This is the alias used for personal workspaces.

Set it in PowerShell before running create or deploy:

```powershell
$env:FABRICCICD_USER = "vj"
```

With this set:

- DEV workspace name becomes something like `fabric-cicd-vj-dev`
- TEST workspace name becomes something like `fabric-cicd-vj-test`
- PROD workspace name becomes something like `fabric-cicd-vj-prod`
- the workspace ID is stored in `fabriccicd_inputs/.local/`

If `FABRICCICD_USER` is not set, the shared workspace naming is used.

### 2. OneLake alias for Spark executable paths

The Spark Job Definition uses an alias-style path in config:

```python
onelake://Lakehouse1/Files/JobArtifacts/my_job.jar
```

During deploy, the script resolves this alias to the current workspace-specific ABFSS path.

This avoids hardcoding old workspace GUIDs in Spark executable paths.

## Commands to know

### Create workspace

```powershell
python fabriccicd.py create DEV
```

What it does:

- creates the workspace if it does not exist
- reuses the workspace if it already exists
- stores the workspace ID for later deploys
- assigns the configured workspace roles

### Deploy workspace

```powershell
python fabriccicd.py deploy DEV
```

What it does:

1. Generates the environment folders.
2. Ensures lakehouses exist.
3. Prepares the Spark Job Definition jar artifact.
4. Publishes the items.
5. Seeds demo data.
6. Applies permissions.
7. Creates shortcuts.
8. Applies schedules.

### Other useful commands

```powershell
python fabriccicd.py generate DEV
python fabriccicd.py all DEV
python fabriccicd.py delete-workspace DEV
```

## What deploy does step by step

### 1. Generate artifacts

`deploy` first generates the local item folders for the environment.

It writes the generated items under the sibling `fabric/` directory, not inside the toolkit root.

### 2. Create lakehouses

The script ensures the declared lakehouses exist in the workspace.

If a lakehouse already exists, it is reused.

### 3. Prepare Spark Job Definition artifacts

For Scala or Java Spark job definitions, the script:

- rebuilds the JAR if needed
- uploads the JAR into OneLake
- rewrites the executable path to the current workspace-specific path

### 4. Publish all items

The script publishes all items defined in the config into the target Fabric workspace.

### 5. Seed demo data

The deployment also runs the seeding notebooks so the workspace has baseline data.

### 6. Apply permissions

The script applies:

- workspace role assignments
- lakehouse data access roles
- table-level access
- file-level access

### 7. Create shortcuts

The script creates configured shortcuts, including ADLS Gen2 shortcuts.

### 8. Apply schedules

The script creates or updates pipeline schedules and applies the enabled state from config.

## Current DEV shortcut setup in MSIT

The current working DEV shortcut is on Lakehouse2.

### Shortcut details

- Shortcut name: `adls_raw_events`
- Lakehouse path: `Files`
- Target type: `AdlsGen2`
- ADLS location: `https://sdndevusw3.dfs.core.windows.net`
- ADLS subpath: `/raw`
- Connection ID: `f4f4053b-95fe-46ca-8493-7cbac2e3cfde`

### Manual connection creation in MSIT Fabric UI

The ADLS Gen2 shortcut connection must be created manually in Fabric.

Use these values:

- Connection name: `sdndevusw3-adls-vijaya-dev`
- Connection type: `Azure Data Lake Storage Gen2`
- Server: `https://sdndevusw3.dfs.core.windows.net`
- Full path: `raw`
- Authentication: OAuth2
- Privacy level: Organizational

### Manual steps

1. Open Fabric portal.
2. Go to **Manage connections and gateways**.
3. Click **New connection**.
4. Select **Azure Data Lake Storage Gen2**.
5. Fill in the values above.
6. Save the connection.
7. Copy the generated connection GUID.
8. Put that GUID into `fabriccicd_inputs/msit_dev.py`.
9. Run `python fabriccicd.py deploy DEV`.

### Why this is manual

The shortcut API needs an existing Fabric connection ID.

Even if the Azure storage account exists and your Azure RBAC is correct, Fabric still needs a usable connection credential for the shortcut.

## Folder behavior

If there is no `fabric/` folder yet, that is fine.

The deploy command creates it automatically.

Expected structure after deploying the three environments is:

```text
fabric/
  dev/
  test/
  prod/
```

If only DEV is deployed, you will only see `fabric/dev/` until TEST and PROD are deployed.

## What to expect in a successful DEV deploy

A successful deploy should show output similar to this:

- `Deploying to DEV... Done!`
- `Notebook run completed successfully.`
- `Create Shortcuts (DEV)`
- `Applied.` for schedules

## Quick validation checklist

1. Run `python fabriccicd.py create DEV`.
2. Run `python fabriccicd.py deploy DEV`.
3. Confirm the `fabric/dev/` folder appears automatically.
4. Confirm Lakehouse2 contains the `adls_raw_events` shortcut.
5. Confirm MyPipeline schedules are enabled.
6. Confirm seed notebooks completed successfully.

## Notes

- This flow is designed to be run from a clean local machine.
- The README documents the exact first-run flow.
- The only manual Fabric step that remains is creating the ADLS Gen2 connection for the shortcut in MSIT.

## Troubleshooting

- If Azure auth fails, run `az login`.
- If `create` says the workspace already exists, that is okay; the script reuses it.
- If shortcut creation returns 400, check the ADLS subpath is not `/`.
- If shortcut creation returns 403, recreate or re-authenticate the Fabric connection.
- If `deploy` creates no folders, confirm the script is being run from the repo root with the correct Python environment.

## Current status verified locally

The current DEV flow has been exercised locally and the deploy path is working with the current config.

You should be able to follow this README from scratch as long as:

- Azure CLI is logged in
- the Fabric workspace can be created or resolved
- the MSIT ADLS connection is created in Fabric UI
