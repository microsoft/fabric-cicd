"""
Unified Fabric CI/CD script.

Reads its configuration from the ``fabriccicd_inputs`` package (typed Python
input files). Edit the per-env files in that package, then run:

    python fabriccicd.py <command> [ENV] [--realm]

Commands:
    create [ENV]            - Create the workspace for ENV (or all if omitted)
    connect [ENV]           - Connect ENV workspace to Azure DevOps Git (default DEV)
    commit [ENV]            - Commit ENV workspace changes to Git (default DEV)
    deploy [ENV]            - Deploy items to ENV workspace (default DEV)
    generate [ENV]          - Generate SJD/Env/Pipeline folders for ENV (default DEV)
    create-lakehouses [ENV] - Create lakehouses defined for ENV (default DEV)
    permissions [ENV]       - Assign lakehouse data permissions for ENV (default DEV)
    all [ENV]               - Create -> connect -> create-lakehouses -> permissions
                              -> deploy (default DEV)

Options:
    --realm                 - Use realm (dailyapi) topology
"""

import json
import os
import re
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

import requests
import yaml
from azure.identity import AzureCliCredential

from fabriccicd_inputs import (
    DataPermission,
    Pipeline,
    SparkEnvironment,
    SparkJobDefinition,
    WorkspaceEnvironment,
    get_workspace,
)

ENV_TARGETS = ("DEV", "TEST", "PROD")
INPUTS_DIR = Path(__file__).parent / "fabriccicd_inputs"


def load_config(env: str, realm_mode: bool) -> dict:
    """Build a runtime config dict from the per-env input file.

    All settings (api base, realm id, repo path, source control, tenant) come
    from the ``WorkspaceEnvironment`` for ``env``. There is no shared/common
    module - every value is declared explicitly per env.
    """
    ws = get_workspace(env, realm_mode)
    sc = ws.source_control
    return {
        "env": env.upper(),
        "realm_mode": ws.realm_mode,
        "realm_id": ws.realm_id,
        "base_url": ws.api_base,
        "repo_path": ws.repo_path,
        "workspace_prefix": ws.workspace_prefix,
        "workspace": ws,
        "git_config": {
            "gitProviderType": sc.provider.value,
            "organizationName": sc.organization,
            "projectName": sc.project,
            "repositoryName": sc.repository,
            "branchName": sc.branch,
            "directoryName": sc.directory,
        },
        "tenant_id": ws.metadata.tenant_id,
    }


def log(message: str, indent: bool = False) -> None:
    """Print a formatted log line."""
    prefix = "  -> " if indent else ">> "
    print(f"{prefix}{message}")


def get_headers(cfg: dict) -> dict:
    """Acquire an Azure CLI access token and build request headers."""
    log("Signing in with Azure CLI credentials...")
    cred = AzureCliCredential()
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    log("Authenticated successfully.", indent=True)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if cfg["realm_mode"]:
        headers["x-ms-fabric-realm-id"] = cfg["realm_id"]
    return headers


def workspace_display_name(cfg: dict, env: str) -> str:
    """Build the Fabric display name for an env.

    Uses ``workspace_name`` override if set on the env, otherwise resolves to
    ``<workspace_prefix>-<env>``.
    """
    ws = get_workspace_env(cfg, env)
    return ws.resolved_workspace_name()


def get_workspace_env(cfg: dict, env: str) -> WorkspaceEnvironment:
    """Resolve an env key to its WorkspaceEnvironment.

    If ``env`` matches the cfg's env, returns the cached workspace. Otherwise
    re-resolves from the inputs package using the cfg's realm_mode.
    """
    key = env.upper()
    if cfg.get("env") == key:
        return cfg["workspace"]
    return get_workspace(env, cfg["realm_mode"])


def require_workspace_id(cfg: dict, env: str) -> str:
    """Return the workspace ID for env, failing with a helpful message if empty."""
    ws = get_workspace_env(cfg, env)
    if not ws.workspace_id:
        name = workspace_display_name(cfg, env)
        msg = (
            f"Workspace ID for {env} is empty. Run 'python fabriccicd.py create {env}' "
            f"first (will create or look up '{name}' and persist the GUID)."
        )
        raise RuntimeError(msg)
    return ws.workspace_id


def poll_long_running_operation(cfg: dict, headers: dict, response: requests.Response) -> requests.Response | None:
    """Poll the Fabric LRO Location/operations endpoint until terminal status."""
    operation_url = response.headers.get("Location")
    if not operation_url:
        operation_id = response.headers.get("x-ms-operation-id")
        if operation_id:
            operation_url = f"{cfg['base_url']}/operations/{operation_id}"
    if not operation_url:
        return None

    retry_after = int(response.headers.get("Retry-After", 30))
    while True:
        time.sleep(retry_after)
        poll_resp = requests.get(operation_url, headers=headers)
        if poll_resp.status_code != 200:
            log(f"Polling error: {poll_resp.status_code} - {poll_resp.text}", indent=True)
            return poll_resp
        result = poll_resp.json()
        status = result.get("status", "")
        log(f"Status: {status}", indent=True)
        if status in ("Succeeded", "Failed", "Cancelled"):
            if status != "Succeeded":
                err = result.get("error") or {}
                err_msg = err.get("message") or result.get("errorMessage") or ""
                err_code = err.get("errorCode") or err.get("code") or ""
                if err_msg or err_code:
                    log(f"Error: [{err_code}] {err_msg}", indent=True)
                # Also try /operations/{id}/result for sync ops that return body there.
                try:
                    result_url = operation_url.rstrip("/") + "/result"
                    rresp = requests.get(result_url, headers=headers)
                    if rresp.status_code == 200 and rresp.text:
                        log(f"Operation result: {rresp.text[:1500]}", indent=True)
                except (requests.RequestException, ValueError):
                    pass
            return poll_resp


def _persist_workspace_id(cfg: dict, env: str, workspace_id: str) -> None:
    """Patch ``workspace_id="..."`` in the per-env input file in place."""
    mode = "realm" if cfg["realm_mode"] else "msit"
    target_file = INPUTS_DIR / f"{mode}_{env.lower()}.py"
    if not target_file.exists():
        log(f"WARNING: Cannot persist ID, file missing: {target_file}", indent=True)
        return

    content = target_file.read_text(encoding="utf-8")
    new_content, count = re.subn(
        r'(workspace_id\s*=\s*)"[^"]*"',
        f'\\1"{workspace_id}"',
        content,
        count=1,
    )
    if count == 0:
        log(f"WARNING: Could not find workspace_id line in {target_file}", indent=True)
        return

    target_file.write_text(new_content, encoding="utf-8")
    # Update in-memory copy if it's the env we currently have cached.
    if cfg.get("env") == env.upper():
        cfg["workspace"].workspace_id = workspace_id
    log(f"Persisted {env} workspace ID -> {target_file.name}", indent=True)


def create_workspaces(cfg: dict, headers: dict, envs: list) -> None:
    """Create or look up the listed envs' workspaces and assign roles."""
    mode_label = "Realm " if cfg["realm_mode"] else ""
    print("\n" + "=" * 60)
    print(f"  Create {mode_label}Workspaces: {', '.join(envs)}")
    print("=" * 60 + "\n")

    for env in envs:
        ws = get_workspace_env(cfg, env)
        workspace_name = workspace_display_name(cfg, env)
        capacity_id = ws.capacity.capacity_id

        resp = requests.post(
            f"{cfg['base_url']}/workspaces",
            headers=headers,
            json={"displayName": workspace_name, "capacityId": capacity_id},
        )

        workspace_id = ""
        if resp.status_code == 201:
            workspace_id = resp.json().get("id", "")
            print(f"{env}: Workspace created. ID: {workspace_id}")
        elif resp.status_code == 409:
            print(f"{env}: 409 (already exists). Looking up by name...")
            lookup = requests.get(
                f"{cfg['base_url']}/workspaces?$filter=displayName eq '{workspace_name}'",
                headers=headers,
            )
            matches = [w for w in lookup.json().get("value", []) if w.get("displayName") == workspace_name]
            if matches:
                workspace_id = matches[0]["id"]
                print(f"{env}: Existing workspace ID: {workspace_id}")
            else:
                print(f"{env}: 409 but no exact match. Skipping.")
                continue
        else:
            print(f"{env}: Failed: {resp.status_code} - {resp.text}")
            continue

        _persist_workspace_id(cfg, env, workspace_id)

        for identity in ws.access_control:
            role_resp = requests.post(
                f"{cfg['base_url']}/workspaces/{workspace_id}/roleAssignments",
                headers=headers,
                json={
                    "principal": {"id": identity.object_id, "type": identity.kind.value},
                    "role": identity.workspace_role.value,
                },
            )
            if role_resp.status_code in (200, 201):
                log(
                    f"Assigned {identity.workspace_role.value} to {identity.kind.value} '{identity.display_name}'",
                    indent=True,
                )
            elif role_resp.status_code == 409:
                log(f"Role already assigned for '{identity.display_name}' - skipping.", indent=True)
            else:
                log(
                    f"Role assignment failed for '{identity.display_name}': {role_resp.status_code} - {role_resp.text}",
                    indent=True,
                )


def connect_git(cfg: dict, headers: dict, env: str = "DEV") -> bool:
    """Connect the env workspace to its Git repo and sync."""
    mode_label = "Realm " if cfg["realm_mode"] else ""
    ws_id = require_workspace_id(cfg, env)
    ws_name = workspace_display_name(cfg, env)
    if cfg["realm_mode"]:
        ws_name += " (realm)"

    print("\n" + "=" * 60)
    print(f"  Connect {mode_label}{env} Workspace to Azure DevOps Git")
    print("=" * 60 + "\n")

    log(f"Step 1/3: Connecting workspace '{ws_name}' to Git repo...")
    log(
        f"Repo: {cfg['git_config']['organizationName']}/{cfg['git_config']['projectName']}/"
        f"{cfg['git_config']['repositoryName']}",
        indent=True,
    )
    log(f"Branch: {cfg['git_config']['branchName']}  |  Directory: /{cfg['git_config']['directoryName']}", indent=True)

    url = f"{cfg['base_url']}/workspaces/{ws_id}/git/connect"
    resp = requests.post(url, headers=headers, json={"gitProviderDetails": cfg["git_config"]})

    if resp.status_code == 200:
        log("Connected successfully!", indent=True)
    else:
        error = resp.json() if resp.text else {}
        if error.get("errorCode") == "WorkspaceAlreadyConnectedToGit":
            log("Already connected to Git - skipping.", indent=True)
        else:
            log(f"Failed to connect: {error.get('message', resp.text)}", indent=True)
            return False
    print()

    log("Step 2/3: Initializing Git connection...")
    url = f"{cfg['base_url']}/workspaces/{ws_id}/git/initializeConnection"
    resp = requests.post(url, headers=headers, json={"initializationStrategy": "PreferRemote"})

    action = "None"
    result: dict = {}
    if resp.status_code == 200:
        result = resp.json()
        action = result.get("requiredAction", "None")
        log(f"Initialized. Next action: {action}", indent=True)
    elif resp.status_code == 202:
        log("Initialization in progress, waiting...", indent=True)
        poll_long_running_operation(cfg, headers, resp)
    else:
        error = resp.json() if resp.text else {}
        if error.get("errorCode") == "WorkspaceGitConnectionAlreadyInitialized":
            log("Already initialized - skipping.", indent=True)
        else:
            log(f"Failed to initialize: {resp.text}", indent=True)
            return False
    print()

    if action == "UpdateFromGit":
        log("Step 3/3: Syncing workspace from Git (pulling latest)...")
        url = f"{cfg['base_url']}/workspaces/{ws_id}/git/updateFromGit"
        body = {
            "remoteCommitHash": result.get("remoteCommitHash", ""),
            "conflictResolution": {"conflictResolutionType": "Workspace", "conflictResolutionPolicy": "PreferRemote"},
            "options": {"allowOverrideItems": True},
        }
        if result.get("workspaceHead"):
            body["workspaceHead"] = result["workspaceHead"]

        resp = requests.post(url, headers=headers, json=body)
        if resp.status_code == 200:
            log("Workspace synced from Git!", indent=True)
        elif resp.status_code == 202:
            log("Sync in progress, waiting...", indent=True)
            poll_long_running_operation(cfg, headers, resp)
        else:
            log(f"Sync failed: {resp.text}", indent=True)
    elif action == "CommitToGit":
        log("Step 3/3: Workspace has local changes that need to be committed to Git.", indent=True)
        log("Run a commit-to-git operation to push workspace changes.", indent=True)
    else:
        log("Step 3/3: No sync needed - workspace and Git are already in sync.", indent=True)

    print("\n" + "=" * 60)
    print(f"  {mode_label}{env} workspace is connected to Git.")
    print("=" * 60 + "\n")
    return True


def delete_workspace(cfg: dict, headers: dict, env: str) -> bool:
    """Delete the workspace for ENV and clear the persisted workspace_id.

    Escape hatch when the workspace is in a corrupted state (items that won't
    delete, stuck git connections, etc.).
    """
    ws_id = require_workspace_id(cfg, env)
    print("\n" + "=" * 60)
    print(f"  Delete {env} workspace: {ws_id}")
    print("=" * 60 + "\n")

    resp = requests.delete(f"{cfg['base_url']}/workspaces/{ws_id}", headers=headers)
    if resp.status_code in (200, 204):
        log("Workspace deleted.")
        _persist_workspace_id(cfg, env, "")
        return True
    log(f"FAILED: {resp.status_code} - {resp.text}")
    return False


def sync_from_git(cfg: dict, headers: dict, env: str = "DEV") -> bool:
    """Force ``updateFromGit`` with PreferRemote, regardless of init state.

    Use after ``clean`` (or whenever the workspace contains stale items) to make
    the workspace match the repo.
    """
    ws_id = require_workspace_id(cfg, env)
    print("\n" + "=" * 60)
    print(f"  Force sync {env} workspace from Git (PreferRemote)")
    print("=" * 60 + "\n")

    log("Getting Git status...")
    status_resp = requests.get(f"{cfg['base_url']}/workspaces/{ws_id}/git/status", headers=headers)
    if status_resp.status_code != 200:
        log(f"Failed: {status_resp.status_code} - {status_resp.text}", indent=True)
        return False
    status = status_resp.json()
    remote_hash = status.get("remoteCommitHash", "")
    body = {
        "remoteCommitHash": remote_hash,
        "conflictResolution": {"conflictResolutionType": "Workspace", "conflictResolutionPolicy": "PreferRemote"},
        "options": {"allowOverrideItems": True},
    }
    # Use existing workspaceHead, or fall back to remoteCommitHash so Fabric
    # aligns metadata against the known commit instead of bootstrapping.
    body["workspaceHead"] = status.get("workspaceHead") or remote_hash

    log(f"Calling updateFromGit (remoteCommitHash={body['remoteCommitHash'][:8]})...")
    resp = requests.post(f"{cfg['base_url']}/workspaces/{ws_id}/git/updateFromGit", headers=headers, json=body)
    if resp.status_code == 200:
        log("Sync complete.", indent=True)
    elif resp.status_code == 202:
        log("Sync in progress, polling...", indent=True)
        poll_long_running_operation(cfg, headers, resp)
    else:
        log(f"Sync failed: {resp.status_code} - {resp.text}", indent=True)
        return False

    print("\n" + "=" * 60)
    print(f"  {env} workspace synced from Git.")
    print("=" * 60 + "\n")
    return True


def commit_to_git(cfg: dict, headers: dict, env: str = "DEV") -> bool:
    """Commit workspace changes to Git so deploy can pick them up."""
    ws_id = require_workspace_id(cfg, env)
    mode_label = "Realm " if cfg["realm_mode"] else ""

    print("\n" + "=" * 60)
    print(f"  Commit {mode_label}{env} Workspace to Git")
    print("=" * 60 + "\n")

    log("Getting Git status...")
    url = f"{cfg['base_url']}/workspaces/{ws_id}/git/status"
    resp = requests.get(url, headers=headers)

    if resp.status_code != 200:
        log(f"Failed to get Git status: {resp.status_code} - {resp.text}", indent=True)
        return False

    status = resp.json()
    workspace_head = status.get("workspaceHead", "")
    changes = status.get("changes", [])

    if not changes:
        log("No uncommitted changes.", indent=True)
        return True

    log(f"Found {len(changes)} change(s) to commit.", indent=True)
    for change in changes:
        log(
            f"  {change.get('itemType', '')} '{change.get('displayName', '')}' - {change.get('changeType', '')}",
            indent=True,
        )
    print()

    log("Committing changes to Git...")
    url = f"{cfg['base_url']}/workspaces/{ws_id}/git/commitToGit"
    body = {"mode": "All", "workspaceHead": workspace_head, "comment": "Commit from fabriccicd.py"}

    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code == 200:
        log("Committed successfully!", indent=True)
    elif resp.status_code == 202:
        log("Commit in progress, waiting...", indent=True)
        poll_long_running_operation(cfg, headers, resp)
    else:
        log(f"Commit failed: {resp.status_code} - {resp.text}", indent=True)
        return False

    print("\n" + "=" * 60)
    print(f"  {mode_label}{env} workspace committed to Git.")
    print("=" * 60 + "\n")
    return True


# ---------------------------------------------------------------------------
# Artifact generation: turn typed inputs into the Fabric Git folder layout.
# ---------------------------------------------------------------------------

PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
)


def _stable_logical_id(env_target: str, item_type: str, name: str) -> str:
    """Deterministic per-(env, type, name) logicalId so re-generation is idempotent."""
    seed = f"{env_target}|{item_type}|{name}".lower()
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, seed))


def _write_platform_file(folder: Path, item_type: str, display_name: str, description: str = "") -> None:
    """Write the ``.platform`` metadata file. Preserves the existing logicalId if one is already present."""
    folder.mkdir(parents=True, exist_ok=True)
    platform_path = folder / ".platform"

    existing_logical_id = ""
    if platform_path.exists():
        try:
            existing = json.loads(platform_path.read_text(encoding="utf-8"))
            existing_logical_id = existing.get("config", {}).get("logicalId", "")
        except (OSError, json.JSONDecodeError):
            existing_logical_id = ""

    logical_id = existing_logical_id or _stable_logical_id(folder.parent.name, item_type, display_name)

    payload = {
        "$schema": PLATFORM_SCHEMA,
        "metadata": {
            "type": item_type,
            "displayName": display_name,
            "description": description or item_type,
        },
        "config": {"version": "2.0", "logicalId": logical_id},
    }
    platform_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _activity_to_json(activity) -> dict:  # noqa: ANN001
    """Convert a PipelineActivity dataclass into Fabric's pipeline-content JSON shape."""
    return {
        "name": activity.name,
        "type": activity.type,
        "typeProperties": activity.type_properties,
        "dependsOn": activity.depends_on,
        "policy": activity.policy,
        "userProperties": activity.user_properties,
    }


def _generate_spark_job_definition(repo_root: Path, sjd: SparkJobDefinition) -> None:
    """Generate ``<name>.SparkJobDefinition/`` with .platform + SparkJobDefinitionV1.json.

    The driver only emits the metadata; ``Main/`` and ``Libs/`` source files must
    already exist in the folder (committed by the user).
    """
    folder = repo_root / f"{sjd.name}.SparkJobDefinition"
    _write_platform_file(folder, "SparkJobDefinition", sjd.name, "Spark job definition")

    definition = {
        "executableFile": sjd.executable_file,
        # default/additional lakehouse + environment IDs are resolved at deploy
        # time via fabric-cicd's parameter.yml replacement (recommended) or can
        # be patched in here directly. We leave them empty so deploys against
        # different envs map them via parameter file.
        "defaultLakehouseArtifactId": "",
        "mainClass": sjd.main_class,
        "additionalLakehouseIds": [],
        "retryPolicy": sjd.retry_policy,
        "commandLineArguments": sjd.command_line_arguments,
        "additionalLibraryUris": list(sjd.additional_library_uris),
        "language": sjd.language.value,
        "environmentArtifactId": "",
    }
    (folder / "SparkJobDefinitionV1.json").write_text(json.dumps(definition, indent=4), encoding="utf-8")
    log(f"Generated SJD '{sjd.name}' at {folder}", indent=True)


def _generate_spark_environment(repo_root: Path, env_item: SparkEnvironment) -> None:
    """Generate ``<name>.Environment/`` with .platform, Setting/Sparkcompute.yml, Libraries/."""
    folder = repo_root / f"{env_item.name}.Environment"
    _write_platform_file(folder, "Environment", env_item.name, env_item.description or "Environment")

    # Spark compute settings.
    setting_dir = folder / "Setting"
    setting_dir.mkdir(parents=True, exist_ok=True)
    pool = env_item.pool
    compute_doc: dict = {
        "enable_native_execution_engine": False,
        "runtime_version": env_item.runtime_version,
    }
    if pool.node_family:
        compute_doc["node_family"] = pool.node_family
    if pool.node_size:
        compute_doc["node_size"] = pool.node_size
    if pool.auto_scale_enabled:
        compute_doc["dynamic_executor_allocation"] = {
            "enabled": pool.dynamic_executor_allocation,
            "min_executors": pool.min_node_count,
            "max_executors": pool.max_node_count,
        }
    if env_item.spark_properties:
        compute_doc["spark_conf"] = dict(env_item.spark_properties)
    (setting_dir / "Sparkcompute.yml").write_text(yaml.safe_dump(compute_doc, sort_keys=False), encoding="utf-8")

    # Libraries.
    libs_dir = folder / "Libraries"
    libs_dir.mkdir(parents=True, exist_ok=True)
    pip_libs = [lib.file_name for lib in env_item.libraries if lib.library_type.lower() == "pypi"]
    if pip_libs:
        public_dir = libs_dir / "PublicLibraries"
        public_dir.mkdir(parents=True, exist_ok=True)
        env_yml = {"dependencies": [{"pip": pip_libs}]}
        (public_dir / "environment.yml").write_text(yaml.safe_dump(env_yml, sort_keys=False), encoding="utf-8")
    custom_libs = [lib for lib in env_item.libraries if lib.library_type.lower() != "pypi"]
    if custom_libs:
        custom_dir = libs_dir / "CustomLibraries"
        custom_dir.mkdir(parents=True, exist_ok=True)
        # The actual .whl/.jar files must be committed under CustomLibraries/ already.
    log(f"Generated Spark Environment '{env_item.name}' at {folder}", indent=True)


def _generate_pipeline(repo_root: Path, pipeline: Pipeline) -> None:
    """Generate ``<name>.DataPipeline/`` with .platform + pipeline-content.json."""
    folder = repo_root / f"{pipeline.name}.DataPipeline"
    _write_platform_file(folder, "DataPipeline", pipeline.name, pipeline.description)

    content: dict = {
        "properties": {
            "activities": [_activity_to_json(a) for a in pipeline.activities],
        }
    }
    if pipeline.parameters:
        content["properties"]["parameters"] = pipeline.parameters
    if pipeline.variables:
        content["properties"]["variables"] = pipeline.variables
    if pipeline.annotations:
        content["properties"]["annotations"] = list(pipeline.annotations)

    (folder / "pipeline-content.json").write_text(json.dumps(content, indent=2), encoding="utf-8")
    log(f"Generated Pipeline '{pipeline.name}' at {folder}", indent=True)


def generate_artifacts(cfg: dict, env: str) -> None:
    """Write SJD / Spark Environment / Pipeline folders from the env input into the repo path."""
    ws = get_workspace_env(cfg, env)
    if not (ws.spark_job_definitions or ws.spark_environments or ws.pipelines):
        log(f"No SJDs / Spark envs / Pipelines declared for {env} - skipping artifact generation.")
        return

    repo_root = Path(cfg["repo_path"])
    if not repo_root.exists():
        log(f"ERROR: repo_path does not exist: {repo_root}")
        return

    print("\n" + "=" * 60)
    print(f"  Generate artifacts for {env} -> {repo_root}")
    print("=" * 60 + "\n")

    # Spark environments first so SJDs can reference them by name on disk.
    for spark_env in ws.spark_environments:
        _generate_spark_environment(repo_root, spark_env)
    for sjd in ws.spark_job_definitions:
        _generate_spark_job_definition(repo_root, sjd)
    for pipeline in ws.pipelines:
        _generate_pipeline(repo_root, pipeline)

    print("\n" + "=" * 60)
    print("  Artifact generation complete.")
    print("=" * 60 + "\n")


def deploy(cfg: dict, env: str) -> None:
    """Deploy items to the env workspace via fabric-cicd."""
    if cfg["realm_mode"]:
        os.environ["FABRIC_API_ROOT_URL"] = "https://dailyapi.powerbi.com"
        os.environ["DEFAULT_API_ROOT_URL"] = "https://dailyapi.powerbi.com"
        os.environ["FABRIC_REALM_ID"] = cfg["realm_id"]

    # Materialize SJD / Environment / Pipeline folders from the env input
    # before handing off to fabric-cicd's repo-driven publisher.
    generate_artifacts(cfg, env)

    from fabric_cicd import FabricWorkspace, publish_all_items

    mode_label = " (realm)" if cfg["realm_mode"] else ""
    ws_id = require_workspace_id(cfg, env)
    ws_env = get_workspace_env(cfg, env)

    print("\n" + "=" * 60)
    print(f"  Deploy to {env}{mode_label}")
    print("=" * 60 + "\n")

    creds = AzureCliCredential()
    workspace = FabricWorkspace(
        workspace_id=ws_id,
        environment=env,
        repository_directory=cfg["repo_path"],
        item_type_in_scope=ws_env.item_types_in_scope,
        token_credential=creds,
    )
    print(f"Deploying to {env}{mode_label}...")
    publish_all_items(workspace)
    print("Done!")


def create_lakehouses(cfg: dict, headers: dict, env: str = "DEV") -> None:
    """Create lakehouses defined for the given env's workspace."""
    print("\n" + "=" * 60)
    print(f"  Create Lakehouses ({env})")
    print("=" * 60 + "\n")

    ws_id = require_workspace_id(cfg, env)
    ws_env = get_workspace_env(cfg, env)

    if not ws_env.lakehouses:
        log(f"No lakehouses defined for {env} in inputs.")
        return

    for lh in ws_env.lakehouses:
        log(f"Creating lakehouse '{lh.name}'...")
        resp = requests.post(
            f"{cfg['base_url']}/workspaces/{ws_id}/items",
            headers=headers,
            json={"displayName": lh.name, "type": "Lakehouse"},
        )

        if resp.status_code == 201:
            lakehouse_id = resp.json().get("id", "")
            log(f"Created! ID: {lakehouse_id}", indent=True)
        elif resp.status_code == 202:
            log("Creation in progress, waiting...", indent=True)
            poll_long_running_operation(cfg, headers, resp)
        elif resp.status_code == 409:
            log("Already exists - skipping.", indent=True)
        else:
            log(f"Failed: {resp.status_code} - {resp.text}", indent=True)

    print("\n" + "=" * 60)
    print("  Lakehouse creation complete.")
    print("=" * 60 + "\n")


def _resolve_lakehouse_ids(cfg: dict, headers: dict, workspace_id: str) -> dict:
    """Get all lakehouses in the workspace and return a name->id map."""
    url = f"{cfg['base_url']}/workspaces/{workspace_id}/items?type=Lakehouse"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        log(f"Failed to list lakehouses: {resp.status_code} - {resp.text}")
        return {}
    items = resp.json().get("value", [])
    return {item["displayName"]: item["id"] for item in items}


def _resolve_user_info(email: str) -> tuple[str, str]:
    """Resolve a user's email to their Microsoft Entra Object ID and Tenant ID via Graph."""
    cred = AzureCliCredential()
    graph_token = cred.get_token("https://graph.microsoft.com/.default").token
    graph_headers = {"Authorization": f"Bearer {graph_token}", "Content-Type": "application/json"}

    resp = requests.get(f"https://graph.microsoft.com/v1.0/users/{email}", headers=graph_headers)
    user_data: dict = {}
    if resp.status_code != 200:
        search_headers = {**graph_headers, "ConsistencyLevel": "eventual"}
        resp = requests.get(
            f"https://graph.microsoft.com/v1.0/users?$filter=mail eq '{email}'&$select=id",
            headers=search_headers,
        )
        if resp.status_code == 200 and resp.json().get("value"):
            user_data = resp.json()["value"][0]
        else:
            log(f"WARNING: Could not resolve user {email}: not found", indent=True)
            return "", ""
    else:
        user_data = resp.json()

    object_id = user_data.get("id", "")

    org_resp = requests.get("https://graph.microsoft.com/v1.0/organization", headers=graph_headers)
    tenant_id = ""
    if org_resp.status_code == 200:
        orgs = org_resp.json().get("value", [])
        if orgs:
            tenant_id = orgs[0].get("id", "")

    return object_id, tenant_id


def _sanitize_role_name(name: str) -> str:
    """Role name must start with a letter and contain only letters and numbers."""
    sanitized = re.sub(r"[^a-zA-Z0-9]", "", name)
    if sanitized and not sanitized[0].isalpha():
        sanitized = "Role" + sanitized
    return sanitized or "DefaultRole"


def _build_access_role(lh_name: str, entry, default_tenant_id: str) -> dict:  # noqa: ANN001
    """Build a Fabric data access role from a DataAccessEntry."""
    email = entry.email
    display_name = entry.display_name or (email.split("@")[0] if email else "User")
    object_id = entry.object_id
    actions = [entry.permission.value]

    tenant_id = default_tenant_id
    if not object_id and email:
        resolved_id, resolved_tenant = _resolve_user_info(email)
        object_id = resolved_id
        if resolved_tenant:
            tenant_id = resolved_tenant

    role_name = _sanitize_role_name(f"{lh_name}{display_name}")

    role_def = {
        "name": role_name,
        "decisionRules": [
            {
                "effect": "Permit",
                "permission": [
                    {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                    {"attributeName": "Action", "attributeValueIncludedIn": actions},
                ],
            }
        ],
        "members": {},
    }

    if object_id:
        member = {"objectId": object_id, "objectType": "User"}
        if tenant_id:
            member["tenantId"] = tenant_id
        role_def["members"]["microsoftEntraMembers"] = [member]
    else:
        log(f"WARNING: No objectId for {email} - role will have no members", indent=True)

    return role_def


def _build_table_access_role(lh_name: str, entry, default_tenant_id: str) -> dict:  # noqa: ANN001
    """Build a Fabric custom data access role scoped to specific tables.

    Used for table-level permissions on a lakehouse (TableAccessEntry).
    """
    email = entry.email
    display_name = entry.display_name or (email.split("@")[0] if email else "User")
    object_id = entry.object_id

    tenant_id = default_tenant_id
    if not object_id and email:
        resolved_id, resolved_tenant = _resolve_user_info(email)
        object_id = resolved_id
        if resolved_tenant:
            tenant_id = resolved_tenant

    table_paths = [f"/Tables/{t}" for t in entry.tables]
    table_label = "_".join(entry.tables)
    role_name = _sanitize_role_name(f"{lh_name}{display_name}{table_label}")

    role_def = {
        "name": role_name,
        "decisionRules": [
            {
                "effect": "Permit",
                "permission": [
                    {"attributeName": "Path", "attributeValueIncludedIn": table_paths},
                    {"attributeName": "Action", "attributeValueIncludedIn": [entry.permission.value]},
                ],
            }
        ],
        "members": {},
    }

    if object_id:
        member = {"objectId": object_id, "objectType": "User"}
        if tenant_id:
            member["tenantId"] = tenant_id
        role_def["members"]["microsoftEntraMembers"] = [member]
    else:
        log(f"WARNING: No objectId for {email} - table role will have no members", indent=True)

    return role_def


def _build_file_access_role(lh_name: str, entry, default_tenant_id: str) -> dict:  # noqa: ANN001
    """Build a Fabric custom data access role scoped to specific files/folders under Files/.

    Used for file-level permissions on a lakehouse (FileAccessEntry).
    """
    email = entry.email
    display_name = entry.display_name or (email.split("@")[0] if email else "User")
    object_id = entry.object_id

    tenant_id = default_tenant_id
    if not object_id and email:
        resolved_id, resolved_tenant = _resolve_user_info(email)
        object_id = resolved_id
        if resolved_tenant:
            tenant_id = resolved_tenant

    # Normalize each path: must start with '/' (e.g. '/Files/raw/customers.csv').
    file_paths = [p if p.startswith("/") else f"/{p}" for p in entry.paths]
    path_label = "_".join(p.rsplit("/", 1)[-1] for p in entry.paths)
    role_name = _sanitize_role_name(f"{lh_name}{display_name}files{path_label}")

    role_def = {
        "name": role_name,
        "decisionRules": [
            {
                "effect": "Permit",
                "permission": [
                    {"attributeName": "Path", "attributeValueIncludedIn": file_paths},
                    {"attributeName": "Action", "attributeValueIncludedIn": [entry.permission.value]},
                ],
            }
        ],
        "members": {},
    }

    if object_id:
        member = {"objectId": object_id, "objectType": "User"}
        if tenant_id:
            member["tenantId"] = tenant_id
        role_def["members"]["microsoftEntraMembers"] = [member]
    else:
        log(f"WARNING: No objectId for {email} - file role will have no members", indent=True)

    return role_def


def _grant_workspace_contributor(cfg: dict, headers: dict, ws_id: str, email: str, display_name: str) -> None:
    """Add a user as workspace Contributor (idempotent). Required for Write access on lakehouses."""
    object_id, tenant_id = _resolve_user_info(email)
    if not object_id:
        log(f"WARNING: Could not resolve {email} for workspace Contributor grant - skipping.", indent=True)
        return
    body = {"principal": {"id": object_id, "type": "User"}, "role": "Contributor"}
    resp = requests.post(
        f"{cfg['base_url']}/workspaces/{ws_id}/roleAssignments",
        headers=headers,
        json=body,
    )
    if resp.status_code in (200, 201):
        log(f"  Granted workspace Contributor to {display_name or email}", indent=True)
    elif resp.status_code == 409:
        log(f"  Workspace Contributor already assigned for {display_name or email}", indent=True)
    else:
        log(
            f"  Workspace Contributor grant failed for {display_name or email}: {resp.status_code} - {resp.text}",
            indent=True,
        )


def assign_permissions(cfg: dict, headers: dict, env: str = "DEV") -> None:
    """Assign exclusive lakehouse permissions for the given env from inputs."""
    print("\n" + "=" * 60)
    print(f"  Assign Lakehouse Permissions ({env})")
    print("=" * 60 + "\n")

    ws_id = require_workspace_id(cfg, env)
    ws_env = get_workspace_env(cfg, env)
    lakehouses = ws_env.lakehouses

    if not lakehouses:
        log("No lakehouses defined in inputs.")
        return

    log("Resolving lakehouse names to IDs...")
    lakehouse_map = _resolve_lakehouse_ids(cfg, headers, ws_id)
    if not lakehouse_map:
        log("No lakehouses found in workspace.")
        return
    log(f"Found {len(lakehouse_map)} lakehouse(s): {', '.join(lakehouse_map.keys())}", indent=True)
    print()

    granted_contributors: set[str] = set()

    for lh in lakehouses:
        if lh.name not in lakehouse_map:
            log(f"WARNING: Lakehouse '{lh.name}' not found in workspace - skipping.")
            continue

        lakehouse_id = lakehouse_map[lh.name]
        log(f"Setting permissions for '{lh.name}' (ID: {lakehouse_id})...")

        roles = [_build_access_role(lh.name, entry, cfg["tenant_id"]) for entry in lh.access_list]
        for entry in lh.access_list:
            log(f"  Granting {entry.permission.value} to {entry.display_name or entry.email}", indent=True)

        for entry in lh.table_access:
            roles.append(_build_table_access_role(lh.name, entry, cfg["tenant_id"]))
            tables_str = ", ".join(entry.tables)
            log(
                f"  Granting {entry.permission.value} on tables [{tables_str}] to {entry.display_name or entry.email}",
                indent=True,
            )

        for entry in lh.file_access:
            roles.append(_build_file_access_role(lh.name, entry, cfg["tenant_id"]))
            paths_str = ", ".join(entry.paths)
            log(
                f"  Granting {entry.permission.value} on files [{paths_str}] to {entry.display_name or entry.email}",
                indent=True,
            )

        # OneLake dataAccessRoles enforce Read scoping only. Write access requires
        # a workspace role; grant Contributor to any user declared as ReadWrite.
        rw_users: dict[str, str] = {}
        for entry in list(lh.access_list) + list(lh.table_access) + list(lh.file_access):
            if entry.permission == DataPermission.ReadWrite and entry.email:
                rw_users.setdefault(entry.email.lower(), entry.display_name or entry.email)
        for email, display_name in rw_users.items():
            if email in granted_contributors:
                continue
            _grant_workspace_contributor(cfg, headers, ws_id, email, display_name)
            granted_contributors.add(email)

        url = f"{cfg['base_url']}/workspaces/{ws_id}/items/{lakehouse_id}/dataAccessRoles"
        resp = requests.put(url, headers=headers, json={"value": roles})

        if resp.status_code in (200, 201):
            log(f"Permissions applied for '{lh.name}'.", indent=True)
        elif resp.status_code == 202:
            log("Operation in progress, waiting...", indent=True)
            poll_long_running_operation(cfg, headers, resp)
        else:
            log(f"Failed: {resp.status_code} - {resp.text}", indent=True)
        print()

    print("=" * 60)
    print("  Lakehouse permissions assignment complete.")
    print("=" * 60 + "\n")


def _type_endpoint(item_type: str) -> str:
    """Return the type-specific Fabric REST path segment for an item type, or empty string."""
    mapping = {
        "Lakehouse": "lakehouses",
        "Notebook": "notebooks",
        "SparkJobDefinition": "sparkJobDefinitions",
        "DataPipeline": "dataPipelines",
        "Eventhouse": "eventhouses",
        "KQLDatabase": "kqlDatabases",
        "Eventstream": "eventstreams",
        "Environment": "environments",
        "Warehouse": "warehouses",
        "MLModel": "mlModels",
        "MLExperiment": "mlExperiments",
        "SemanticModel": "semanticModels",
        "Report": "reports",
        "CopyJob": "copyJobs",
        "Reflex": "reflexes",
        "GraphQLApi": "GraphQLApis",
        "Dataflow": "dataflows",
        "SQLDatabase": "sqlDatabases",
        "MountedDataFactory": "mountedDataFactories",
    }
    return mapping.get(item_type, "")


def _list_repo_items(cfg: dict) -> set[tuple[str, str]]:
    """Return the set of (displayName_lower, type_lower) declared in the repo."""
    repo_path = Path(cfg["repo_path"])
    items: set[tuple[str, str]] = set()
    if not repo_path.exists():
        return items
    for pf in repo_path.rglob(".platform"):
        try:
            data = json.loads(pf.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        meta = data.get("metadata", {})
        name = meta.get("displayName", "")
        item_type = meta.get("type", "")
        if name and item_type:
            items.add((name.lower(), item_type.lower()))
    return items


def clean_workspace(cfg: dict, headers: dict, env: str) -> bool:
    """Delete workspace items that collide with repo items but are unlinked from Git.

    Fabric rejects ``updateFromGit`` when the workspace already contains an item
    with the same ``(displayName, type)`` as one in the repo but with no
    ``logicalId`` binding. This removes those orphans so the sync can proceed.
    """
    ws_id = require_workspace_id(cfg, env)
    print("\n" + "=" * 60)
    print(f"  Clean orphan items in {env} workspace")
    print("=" * 60 + "\n")

    repo_items = _list_repo_items(cfg)
    log(f"Repo declares {len(repo_items)} item(s).")

    resp = requests.get(f"{cfg['base_url']}/workspaces/{ws_id}/items", headers=headers)
    if resp.status_code != 200:
        log(f"ERROR: Failed to list workspace items: {resp.status_code} - {resp.text}")
        return False

    ws_items = resp.json().get("value", [])
    log(f"Workspace contains {len(ws_items)} item(s).")

    deleted = 0
    for item in ws_items:
        name = item.get("displayName", "")
        item_type = item.get("type", "")
        item_id = item.get("id", "")
        key = (name.lower(), item_type.lower())
        if key not in repo_items:
            continue
        log(f"Deleting workspace item colliding with repo: '{name}' ({item_type}) [{item_id}]")
        del_resp = requests.delete(f"{cfg['base_url']}/workspaces/{ws_id}/items/{item_id}", headers=headers)
        if del_resp.status_code not in (200, 204):
            # Fall back to type-specific endpoint (e.g. /lakehouses/{id}).
            type_path = _type_endpoint(item_type)
            if type_path:
                log(f"Generic delete failed ({del_resp.status_code}); retrying via /{type_path}...", indent=True)
                del_resp = requests.delete(
                    f"{cfg['base_url']}/workspaces/{ws_id}/{type_path}/{item_id}", headers=headers
                )
        if del_resp.status_code in (200, 204):
            deleted += 1
            log("Deleted.", indent=True)
        else:
            log(f"FAILED: {del_resp.status_code} - {del_resp.text}", indent=True)

    print("\n" + "=" * 60)
    print(f"  Clean complete. Deleted {deleted} orphan item(s).")
    print("=" * 60 + "\n")
    return True


def check_repo(cfg: dict) -> bool:
    """Scan the local repo for duplicate (displayName, type) Fabric items.

    Returns True if no duplicates were found, False otherwise. This catches the
    Fabric Git error: "We can't complete this action because multiple items
    have the same name."
    """
    repo_path = Path(cfg["repo_path"])
    print("\n" + "=" * 60)
    print(f"  Check repo for duplicate items: {repo_path}")
    print("=" * 60 + "\n")

    if not repo_path.exists():
        log(f"ERROR: Repo path does not exist: {repo_path}")
        return False

    grouped: dict[tuple[str, str], list[Path]] = defaultdict(list)
    by_logical_id: dict[str, list[Path]] = defaultdict(list)
    platform_files = list(repo_path.rglob(".platform"))
    log(f"Scanned {len(platform_files)} .platform file(s).")

    for pf in platform_files:
        try:
            data = json.loads(pf.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log(f"WARNING: Could not parse {pf}: {exc}", indent=True)
            continue
        meta = data.get("metadata", {})
        display_name = meta.get("displayName", "")
        item_type = meta.get("type", "")
        logical_id = data.get("config", {}).get("logicalId", "")
        if display_name and item_type:
            # Normalize casing for case-insensitive collision detection.
            grouped[(display_name.lower(), item_type.lower())].append(pf.parent)
        if logical_id:
            by_logical_id[logical_id].append(pf.parent)

    name_dups = {k: v for k, v in grouped.items() if len(v) > 1}
    id_dups = {k: v for k, v in by_logical_id.items() if len(v) > 1}

    if not name_dups and not id_dups:
        log("No duplicate (displayName, type) pairs or logicalIds found.", indent=True)
        print("\n" + "=" * 60)
        print("  Repo check passed.")
        print("=" * 60 + "\n")
        return True

    if name_dups:
        log(f"Found {len(name_dups)} duplicate (displayName, type) group(s):")
        for (name, item_type), folders in name_dups.items():
            log(f"  '{name}' ({item_type}) appears in:", indent=True)
            for folder in folders:
                log(f"    - {folder}", indent=True)

    if id_dups:
        log(f"Found {len(id_dups)} duplicate logicalId group(s):")
        for logical_id, folders in id_dups.items():
            log(f"  logicalId={logical_id} appears in:", indent=True)
            for folder in folders:
                log(f"    - {folder}", indent=True)

    print("\n" + "=" * 60)
    print("  Repo check FAILED. Fix duplicates before running connect/deploy.")
    print("=" * 60 + "\n")
    return False


def main() -> None:
    """CLI entry point."""
    usage = (
        "Usage:\n"
        "  python fabriccicd.py <command> [ENV] [--realm]\n\n"
        "Commands:\n"
        "  check                   - Scan repo for duplicate (displayName, type) items\n"
        "  clean [ENV]             - Delete unlinked workspace items colliding with repo (default DEV)\n"
        "  delete-workspace [ENV]  - Delete the ENV workspace entirely (escape hatch)\n"
        "  create [ENV]            - Create workspace(s). If ENV omitted, all envs.\n"
        "  connect [ENV]           - Connect ENV workspace to Git (default DEV)\n"
        "  sync [ENV]              - Force pull from Git with PreferRemote (default DEV)\n"
        "  commit [ENV]            - Commit ENV workspace to Git (default DEV)\n"
        "  deploy [ENV]            - Deploy items to ENV (default DEV)\n"
        "  generate [ENV]          - Generate SJD/Env/Pipeline folders for ENV (default DEV)\n"
        "  create-lakehouses [ENV] - Create lakehouses for ENV (default DEV)\n"
        "  permissions [ENV]       - Assign lakehouse permissions for ENV (default DEV)\n"
        "  all [ENV]               - Full pipeline: check -> create -> clean -> deploy ->\n"
        "                            lakehouses -> permissions (+ git connect for DEV)\n\n"
        "Options:\n"
        "  --realm                 - Use realm (dailyapi) topology\n"
    )

    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)

    realm_mode = "--realm" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--realm"]

    command = args[0].lower()
    env_arg = args[1].upper() if len(args) > 1 else ""
    env = env_arg or "DEV"

    cfg = load_config(env, realm_mode)
    mode_label = " [REALM]" if realm_mode else ""
    print(f">> Mode:{mode_label} | Environment: {env}")

    if command == "check":
        ok = check_repo(cfg)
        sys.exit(0 if ok else 1)
    elif command == "clean":
        headers = get_headers(cfg)
        clean_workspace(cfg, headers, env)
    elif command == "delete-workspace":
        headers = get_headers(cfg)
        delete_workspace(cfg, headers, env)
    elif command == "create":
        envs = [env_arg] if env_arg else list(ENV_TARGETS)
        for e in envs:
            ecfg = load_config(e, realm_mode)
            eheaders = get_headers(ecfg)
            create_workspaces(ecfg, eheaders, [e])
    elif command == "connect":
        headers = get_headers(cfg)
        connect_git(cfg, headers, env)
    elif command == "sync":
        headers = get_headers(cfg)
        sync_from_git(cfg, headers, env)
    elif command == "commit":
        headers = get_headers(cfg)
        commit_to_git(cfg, headers, env)
    elif command == "deploy":
        deploy(cfg, env)
    elif command == "generate":
        generate_artifacts(cfg, env)
    elif command == "create-lakehouses":
        headers = get_headers(cfg)
        create_lakehouses(cfg, headers, env)
    elif command == "permissions":
        headers = get_headers(cfg)
        assign_permissions(cfg, headers, env)
    elif command == "all":
        if not check_repo(cfg):
            log("Aborting 'all' due to repo check failures.")
            sys.exit(1)
        headers = get_headers(cfg)
        create_workspaces(cfg, headers, [env])
        clean_workspace(cfg, headers, env)
        deploy(cfg, env)
        create_lakehouses(cfg, headers, env)
        assign_permissions(cfg, headers, env)
        if env == "DEV":
            connect_git(cfg, headers, env)
        else:
            log(f"Skipping git connect for {env} (only DEV is git-connected).")
    else:
        print(f"Unknown command: {command}\n")
        print(usage)
        sys.exit(1)


if __name__ == "__main__":
    main()
