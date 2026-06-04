# Azure DevOps CI Setup — Fabric CI/CD (DEV only, defaults)

This is a one-time setup guide. After this, every PR merged into the `dev`
branch will automatically deploy to the MSIT DEV Fabric workspace
(`fabric-cicd-dev`). PROD and TEST are **not** wired up yet — that's
intentional, we'll add them once DEV is proven stable.

## What gets deployed

The pipeline runs:

1. `python scripts/fabriccicd.py check` — scans the repo for duplicate items
2. `python scripts/fabriccicd.py deploy DEV` — runs `generate_artifacts` then
   `publish_all_items` against `fabric-cicd-dev`

Lakehouse creation and permission assignment are **not** done by the pipeline
(intentional — those are workspace-shape changes that should stay manual for
now). When you add new lakehouses or change access, run those steps locally:

```pwsh
python scripts/fabriccicd.py create-lakehouses DEV
python scripts/fabriccicd.py permissions DEV
```

## One-time setup in Azure DevOps

### 1. Create a service principal

The pipeline needs a non-interactive identity to call the Fabric REST API.

**Option A: Ask an AAD admin to create a new app registration.**

Send them this request:

> Please create an Azure AD app registration named `fabric-cicd-asimov-sp`
> in tenant `33e01921-4d64-4f8c-a055-5bdaffd5e33d`. We need a client secret
> with a 1-year expiry, and we'll add it to the Fabric workspaces as a
> Contributor ourselves.

You'll receive:

- **Tenant ID** (likely `33e01921-4d64-4f8c-a055-5bdaffd5e33d`)
- **Client ID** (the app registration's Application ID)
- **Client Secret** (value, shown once at creation)

**Option B: Reuse an existing automation SP** if your team has one. Ask
Sikana for the credentials.

### 2. Grant the SP Contributor on the Fabric workspace

The SP needs Contributor role on `fabric-cicd-dev`. Easiest way: open the
workspace in the Fabric portal -> **Manage access** -> add the SP by name
-> role **Contributor**.

Alternatively, run this locally (you'll need the SP's object ID):

```pwsh
$SP_OBJECT_ID = "<service-principal-object-id>"
$WS_ID = "34831a7c-bee0-4089-8d68-f1c0524bcb1d"  # fabric-cicd-dev
az rest --method post `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/roleAssignments" `
  --body "{`"principal`":{`"id`":`"$SP_OBJECT_ID`",`"type`":`"ServicePrincipal`"},`"role`":`"Contributor`"}"
```

### 3. Create the ADO variable group

1. Go to https://dev.azure.com/msazure/One/_library
2. Click **+ Variable group**
3. **Name**: `fabric-cicd-secrets` (must match exactly — referenced in
   `azure-pipelines.yml`)
4. Add three variables:
    - `AZURE_TENANT_ID` -> the tenant GUID
    - `AZURE_CLIENT_ID` -> the SP's Application ID
    - `AZURE_CLIENT_SECRET` -> the SP's client secret (click the lock icon to
      mark it secret)
5. **Save**.

### 4. Create the pipeline

1. Go to https://dev.azure.com/msazure/One/_build
2. Click **New pipeline**.
3. Select **Azure Repos Git** -> `Asimov-vNext-Deployment`.
4. **Existing Azure Pipelines YAML file** -> branch `dev` -> path
   `/azure-pipelines.yml`.
5. **Save** (do NOT click Run yet).
6. Click **... -> Manage security** (or **Edit -> Permissions**) and grant
   the pipeline **Authorize use of** the `fabric-cicd-secrets` variable
   group. First run will prompt for this if you skip.

### 5. Test it

Option A: Push a no-op commit to `dev` (e.g. a comment in
`scripts/fabriccicd_inputs/msit_dev.py`) and watch the pipeline run.

Option B: Click **Run pipeline** -> branch `dev` -> **Run**.

Expected first run: ~3-5 min. You'll see:

- `Use Python 3.11` -> green
- `Install Python dependencies` -> green
- `Repo duplicate-item scan` -> green
- `Deploy items to MSIT DEV` -> green (publishes items)

## What's NOT in the pipeline (by design)

| Action                        | Why excluded                                                  |
| ----------------------------- | ------------------------------------------------------------- |
| `create` (workspace creation) | One-time, manual — workspace IDs are already set in env files |
| `clean`                       | Risk of deleting lakehouses (we hit this issue today)         |
| `create-lakehouses`           | Workspace-shape change; safer to do manually with review      |
| `permissions`                 | Wipes existing data-access roles if env file is empty         |
| `connect`                     | Already done on DEV; should only run once                     |
| TEST / PROD deploys           | Will be added once DEV pipeline is stable                     |

## Next steps after DEV is proven stable

1. Add a `Deploy_TEST` stage that triggers on `main` branch merges.
2. Add a `Deploy_PROD` stage with **manual approval gate** that follows TEST.
3. Add a separate PR validation pipeline (runs `check` and lint on every PR,
   no deploy).

## Troubleshooting

**Pipeline fails at `Repo duplicate-item scan`:**
Two items in the repo share a `(displayName, type)`. Run locally:
`python scripts/fabriccicd.py check` -- the output will list the duplicates.

**Pipeline fails at deploy with 401/403:**
SP either doesn't exist (check variable group values), or doesn't have
Contributor on `fabric-cicd-dev` (re-check step 2 above).

**Pipeline fails at deploy with `'X' is not available yet`:**
Fabric recycle-bin issue (we hit this with `clean` today). Wait 10 minutes
and re-run the pipeline. Avoid running `clean` in the pipeline.

**Pipeline doesn't trigger on push to dev:**
Check the `paths:` filter in `azure-pipelines.yml`. By default it only
triggers on changes to `fabric/`, `scripts/fabriccicd.py`,
`scripts/fabriccicd_inputs/`, or the pipeline itself. Other file changes
won't fire the pipeline (this is intentional to avoid spurious deploys).
