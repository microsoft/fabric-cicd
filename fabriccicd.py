"""
Unified Fabric CI/CD script.

Reads its configuration from the ``fabriccicd_inputs`` package (typed Python
input files). Edit the per-env files in that package, then run:

    python fabriccicd.py <command> [ENV] [--realm]

Commands:
    check                   - Scan repo for duplicate (displayName, type) items
    create [ENV]            - Create or look up the ENV workspace and assign roles
    delete-workspace [ENV]  - Hard-delete the ENV workspace (escape hatch)
    generate [ENV]          - Generate SJD/Env/Pipeline folders for ENV (default DEV)
    deploy [ENV]            - Generate artifacts, create lakehouses, publish all
                              items, and assign data permissions (default DEV)
    all [ENV]               - check -> create -> deploy (default DEV)

Multi-developer model:
    Set ``FABRICCICD_USER`` to your alias to target a personal DEV workspace
    (``<workspace_prefix>-<user>-dev``). The workspace ID is persisted to a
    sidecar under ``fabriccicd_inputs/.local/`` so it does not collide with
    shared input files.

Options:
    --realm                 - Use realm (dailyapi) topology
"""

import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from pathlib import Path

import requests
import yaml
from azure.identity import AzureCliCredential, ClientSecretCredential

from fabriccicd_inputs import (
    DataPermission,
    LakehouseDefinition,
    Pipeline,
    Schedule,
    SparkEnvironment,
    SparkJobDefinition,
    WorkspaceEnvironment,
    _sanitize_user,
    get_workspace,
)

ENV_TARGETS = ("DEV", "TEST", "PROD")
INPUTS_DIR = Path(__file__).parent / "fabriccicd_inputs"


def get_credential() -> AzureCliCredential | ClientSecretCredential:
    """Return a token credential suitable for the current environment.

    - In CI (when ``AZURE_TENANT_ID`` / ``AZURE_CLIENT_ID`` / ``AZURE_CLIENT_SECRET``
      are all set), returns a ``ClientSecretCredential`` for the service principal.
    - Otherwise, falls back to ``AzureCliCredential`` for local development
      (requires ``az login`` first).
    """
    tenant = os.getenv("AZURE_TENANT_ID")
    client = os.getenv("AZURE_CLIENT_ID")
    secret = os.getenv("AZURE_CLIENT_SECRET")
    if tenant and client and secret:
        return ClientSecretCredential(tenant, client, secret)
    return AzureCliCredential()


def load_config(env: str, realm_mode: bool) -> dict:
    """Build a runtime config dict from the per-env input file.

    All settings (api base, realm id, repo path, source control, tenant) come
    from the ``WorkspaceEnvironment`` for ``env``. There is no shared/common
    module - every value is declared explicitly per env. When ``repo_path`` is
    empty in the input file, it is auto-resolved to the per-env sibling folder
    ``fabric/<env>/`` next to this toolkit's parent directory, so DEV / TEST /
    PROD each own a separate set of generated artifacts and never overwrite
    each other on disk.
    """
    ws = get_workspace(env, realm_mode)
    repo_path = ws.repo_path or str((Path(__file__).resolve().parent.parent / "fabric" / env.lower()).resolve())
    return {
        "env": env.upper(),
        "realm_mode": ws.realm_mode,
        "realm_id": ws.realm_id,
        "base_url": ws.api_base,
        "repo_path": repo_path,
        "workspace_prefix": ws.workspace_prefix,
        "workspace": ws,
        "tenant_id": ws.metadata.tenant_id,
    }


def log(message: str, indent: bool = False) -> None:
    """Print a formatted log line."""
    prefix = "  -> " if indent else ">> "
    print(f"{prefix}{message}")


def get_headers(cfg: dict) -> dict:
    """Acquire an Azure access token and build request headers."""
    log("Acquiring Azure access token...")
    cred = get_credential()
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
            log(
                f"Polling error: {poll_resp.status_code} - {poll_resp.text}",
                indent=True,
            )
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
    """Persist a workspace GUID for later runs.

    By default, patches the ``workspace_id`` literal in the per-env input file.
    When ``FABRICCICD_USER`` is set, writes to a sidecar file under
    ``fabriccicd_inputs/.local/`` instead, so each developer's personal
    DEV / TEST / PROD IDs do not collide with the shared inputs files.
    """
    mode = "realm" if cfg["realm_mode"] else "msit"
    user = os.environ.get("FABRICCICD_USER", "")
    if user.strip():
        safe_user = _sanitize_user(user)
        sidecar_dir = INPUTS_DIR / ".local"
        sidecar_dir.mkdir(parents=True, exist_ok=True)
        sidecar = sidecar_dir / f"{mode}_{env.lower()}_{safe_user}.id"
        sidecar.write_text(workspace_id, encoding="utf-8")
        if cfg.get("env") == env.upper():
            cfg["workspace"].workspace_id = workspace_id
        log(
            f"Persisted personal {env} workspace ID -> {sidecar.relative_to(INPUTS_DIR.parent)}",
            indent=True,
        )
        return

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
                    "principal": {
                        "id": identity.object_id,
                        "type": identity.kind.value,
                    },
                    "role": identity.workspace_role.value,
                },
            )
            if role_resp.status_code in (200, 201):
                log(
                    f"Assigned {identity.workspace_role.value} to {identity.kind.value} '{identity.display_name}'",
                    indent=True,
                )
            elif role_resp.status_code == 409:
                log(
                    f"Role already assigned for '{identity.display_name}' - skipping.",
                    indent=True,
                )
            else:
                log(
                    f"Role assignment failed for '{identity.display_name}': {role_resp.status_code} - {role_resp.text}",
                    indent=True,
                )


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


# ---------------------------------------------------------------------------
# Artifact generation: turn typed inputs into the Fabric Git folder layout.
# ---------------------------------------------------------------------------

PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
)

# Markers written into every folder/file produced by ``fabriccicd.py generate`` so
# reviewers can quickly tell hand-authored Fabric items apart from generated ones.
_GENERATED_BY = "fabriccicd.py"
_YAML_AUTOGEN_HEADER = (
    f"# AUTO-GENERATED by {_GENERATED_BY} - DO NOT EDIT BY HAND.\n"
    "# Source of truth: fabriccicd_inputs/<env>.py (re-run 'fabriccicd.py generate' to refresh).\n"
)
_AUTOGEN_DESCRIPTION_SUFFIX = f" [auto-generated by {_GENERATED_BY}]"
_AUTOGEN_MARKER_FILENAME = ".fabriccicd-generated"


def _write_autogen_marker(folder: Path, item_type: str) -> None:  # noqa: ARG001
    """No-op: an in-folder marker file would be uploaded as an item-definition part and
    break publish for strict item types (e.g. DataPipeline rejects extra parts with
    'Sequence contains more than one element'). The YAML header on inputs files and the
    '[auto-generated]' suffix in .platform descriptions already convey origin.
    """
    folder.mkdir(parents=True, exist_ok=True)
    stale = folder / _AUTOGEN_MARKER_FILENAME
    if stale.exists():
        stale.unlink()


def _stable_logical_id(item_type: str, name: str) -> str:
    """Deterministic per-(type, name) logicalId so re-generation is idempotent and stable across envs."""
    seed = f"{item_type}|{name}".lower()
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

    logical_id = existing_logical_id or _stable_logical_id(item_type, display_name)

    base_description = description or item_type
    if _AUTOGEN_DESCRIPTION_SUFFIX not in base_description:
        base_description = f"{base_description}{_AUTOGEN_DESCRIPTION_SUFFIX}"

    payload = {
        "$schema": PLATFORM_SCHEMA,
        "metadata": {
            "type": item_type,
            "displayName": display_name,
            "description": base_description,
        },
        "config": {"version": "2.0", "logicalId": logical_id},
    }
    platform_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _activity_to_json(activity) -> dict:  # noqa: ANN001
    """Convert a PipelineActivity dataclass into Fabric's pipeline-content JSON shape.

    Empty optional fields (``dependsOn``, ``policy``, ``userProperties``) are omitted
    because Fabric's deserializer rejects empty objects/lists for some activity types
    (e.g. ``Wait`` fails with 'Sequence contains more than one element' when ``policy``
    is ``{}``).
    """
    payload: dict = {
        "name": activity.name,
        "type": activity.type,
        "typeProperties": activity.type_properties,
    }
    if activity.depends_on:
        payload["dependsOn"] = activity.depends_on
    if activity.policy:
        payload["policy"] = activity.policy
    if activity.user_properties:
        payload["userProperties"] = activity.user_properties
    return payload


def _is_remote_uri(path_or_uri: str) -> bool:
    """Return True when a value looks like a remote URI (e.g. abfss://...)."""
    return bool(re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", path_or_uri))


def _generate_spark_job_definition(repo_root: Path, sjd: SparkJobDefinition) -> None:
    """Generate ``<name>.SparkJobDefinition/`` with .platform + SparkJobDefinitionV1.json.

    Stub ``Main/<executable_file>`` and ``Libs/<lib>`` files are created when missing
    so a fresh deploy succeeds without any user-supplied source. Existing files are
    preserved.
    """
    folder = repo_root / f"{sjd.name}.SparkJobDefinition"
    _write_platform_file(folder, "SparkJobDefinition", sjd.name, "Spark job definition")
    _write_autogen_marker(folder, "SparkJobDefinition")

    # Fabric API accepts "Scala/Java" as a combined value; keep input enum
    # backwards-compatible by normalizing legacy "Scala"/"Java" values.
    language_value = sjd.language.value
    if language_value in {"Scala", "Java"}:
        language_value = "Scala/Java"

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
        "language": language_value,
        "environmentArtifactId": "",
    }
    (folder / "SparkJobDefinitionV1.json").write_text(json.dumps(definition, indent=4), encoding="utf-8")

    def _prune_autogen_main_stubs(main_dir: Path, keep: Path | None = None) -> None:
        if not main_dir.exists():
            return
        for candidate in main_dir.glob("*"):
            if not candidate.is_file() or (keep is not None and candidate == keep):
                continue
            try:
                preview = candidate.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            if preview.startswith("# Auto-generated stub for SJD") or preview.startswith(
                "// Auto-generated stub for SJD"
            ):
                candidate.unlink()

    if sjd.executable_file and not _is_remote_uri(sjd.executable_file):
        main_dir = folder / "Main"
        main_dir.mkdir(parents=True, exist_ok=True)
        main_file = main_dir / sjd.executable_file

        # Keep only the current executable when older auto-generated stubs exist
        # (e.g. switching between main.py and my_job.jar across config changes).
        _prune_autogen_main_stubs(main_dir, keep=main_file)

        if not main_file.exists():
            ext = main_file.suffix.lower()
            if ext == ".py":
                stub = (
                    f"# Auto-generated stub for SJD '{sjd.name}'. Replace with real logic.\n"
                    "from pyspark.sql import SparkSession\n\n"
                    "def main() -> None:\n"
                    f'    spark = SparkSession.builder.appName("{sjd.name}").getOrCreate()\n'
                    f'    print("SJD {sjd.name} running")\n\n'
                    'if __name__ == "__main__":\n'
                    "    main()\n"
                )
            else:
                stub = f"// Auto-generated stub for SJD '{sjd.name}'. Replace with real logic.\n"
            main_file.write_text(stub, encoding="utf-8")
    else:
        # Remote executables should not include local Main file parts. Remove stale
        # auto-generated stubs so updateDefinition doesn't fail path validation.
        _prune_autogen_main_stubs(folder / "Main")

    if sjd.additional_library_uris:
        libs_dir = folder / "Libs"
        libs_dir.mkdir(parents=True, exist_ok=True)
        for lib_name in sjd.additional_library_uris:
            if _is_remote_uri(lib_name):
                continue
            lib_file = libs_dir / lib_name
            if not lib_file.exists():
                lib_file.parent.mkdir(parents=True, exist_ok=True)
                ext = lib_file.suffix.lower()
                if ext == ".py":
                    stub = f"# Auto-generated stub for SJD '{sjd.name}' library '{lib_name}'.\nPIPELINE_CONFIG = {{}}\n"
                else:
                    stub = f"// Auto-generated stub for SJD '{sjd.name}' library '{lib_name}'.\n"
                lib_file.write_text(stub, encoding="utf-8")

    log(f"Generated SJD '{sjd.name}' at {folder}", indent=True)


def _generate_spark_environment(repo_root: Path, env_item: SparkEnvironment) -> None:
    """Generate ``<name>.Environment/`` with .platform, Setting/Sparkcompute.yml, Libraries/."""
    folder = repo_root / f"{env_item.name}.Environment"
    _write_platform_file(folder, "Environment", env_item.name, env_item.description or "Environment")
    _write_autogen_marker(folder, "Environment")

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
    (setting_dir / "Sparkcompute.yml").write_text(
        _YAML_AUTOGEN_HEADER + yaml.safe_dump(compute_doc, sort_keys=False),
        encoding="utf-8",
    )

    # Libraries.
    libs_dir = folder / "Libraries"
    libs_dir.mkdir(parents=True, exist_ok=True)
    pip_libs = [lib.file_name for lib in env_item.libraries if lib.library_type.lower() == "pypi"]
    if pip_libs:
        public_dir = libs_dir / "PublicLibraries"
        public_dir.mkdir(parents=True, exist_ok=True)
        env_yml = {"dependencies": [{"pip": pip_libs}]}
        (public_dir / "environment.yml").write_text(
            _YAML_AUTOGEN_HEADER + yaml.safe_dump(env_yml, sort_keys=False),
            encoding="utf-8",
        )
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
    _write_autogen_marker(folder, "DataPipeline")

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


def _generate_lakehouse(repo_root: Path, lakehouse: LakehouseDefinition) -> None:
    """Generate ``<name>.Lakehouse/`` with the four files Fabric git-sync produces.

    Lakehouses are shell-only on Fabric (no definition payload). The folder exists
    purely so reviewers can see every deployed item in source control.
    """
    folder = repo_root / f"{lakehouse.name}.Lakehouse"
    _write_platform_file(folder, "Lakehouse", lakehouse.name, "Lakehouse")
    (folder / "lakehouse.metadata.json").write_text("{}", encoding="utf-8")
    (folder / "shortcuts.metadata.json").write_text("[]", encoding="utf-8")
    alm = {
        "version": "1.0.1",
        "objectTypes": [
            {
                "name": "Shortcuts",
                "state": "Enabled",
                "subObjectTypes": [
                    {"name": "Shortcuts.OneLake", "state": "Enabled"},
                    {"name": "Shortcuts.AdlsGen2", "state": "Enabled"},
                    {"name": "Shortcuts.Dataverse", "state": "Enabled"},
                    {"name": "Shortcuts.AmazonS3", "state": "Enabled"},
                    {"name": "Shortcuts.S3Compatible", "state": "Enabled"},
                    {"name": "Shortcuts.GoogleCloudStorage", "state": "Enabled"},
                    {"name": "Shortcuts.AzureBlobStorage", "state": "Enabled"},
                    {"name": "Shortcuts.OneDriveSharePoint", "state": "Enabled"},
                ],
            },
            {"name": "DataAccessRoles", "state": "Disabled"},
        ],
    }
    (folder / "alm.settings.json").write_text(json.dumps(alm, indent=2), encoding="utf-8")
    log(f"Generated Lakehouse '{lakehouse.name}' at {folder}", indent=True)


def _resolve_onelake_executable_alias(executable_file: str, ws_id: str, lakehouse_map: dict[str, str]) -> str:
    """Resolve ``onelake://<LakehouseName>/Files/...`` to an ABFSS path in the current workspace."""
    prefix = "onelake://"
    if not executable_file.startswith(prefix):
        return executable_file

    suffix = executable_file[len(prefix) :]
    lakehouse_name, sep, relative_path = suffix.partition("/")
    if not sep or not relative_path:
        msg = f"Invalid SJD executable_file '{executable_file}'. Expected onelake://<LakehouseName>/Files/..."
        raise ValueError(msg)
    if not relative_path.startswith("Files/"):
        msg = f"Invalid SJD executable_file '{executable_file}'. Path must start with 'Files/'."
        raise ValueError(msg)

    lakehouse_id = lakehouse_map.get(lakehouse_name)
    if not lakehouse_id:
        msg = f"Invalid SJD executable_file '{executable_file}'. Lakehouse '{lakehouse_name}' not found in workspace."
        raise ValueError(msg)

    return f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/{relative_path}"


def _parse_onelake_executable_alias(executable_file: str) -> tuple[str, str]:
    """Parse ``onelake://<LakehouseName>/Files/...`` into (lakehouse_name, relative_path)."""
    prefix = "onelake://"
    if not executable_file.startswith(prefix):
        msg = f"Invalid onelake executable alias '{executable_file}'."
        raise ValueError(msg)

    suffix = executable_file[len(prefix) :]
    lakehouse_name, sep, relative_path = suffix.partition("/")
    if not sep or not relative_path or not relative_path.startswith("Files/"):
        msg = f"Invalid onelake executable alias '{executable_file}'. Expected onelake://<Lakehouse>/Files/..."
        raise ValueError(msg)
    return lakehouse_name, relative_path


def _build_noop_java_jar(output_jar: Path, main_class: str, sjd_name: str) -> None:
    """Build a minimal JVM jar so SJD pipeline runs have a valid executable artifact."""
    if not main_class:
        msg = f"SJD '{sjd_name}' is missing main_class required for JVM jar build."
        raise RuntimeError(msg)

    package_name, _, class_name = main_class.rpartition(".")
    if not class_name:
        msg = f"Invalid main_class '{main_class}' for SJD '{sjd_name}'."
        raise RuntimeError(msg)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        src_root = tmp_dir / "src"
        classes_root = tmp_dir / "classes"

        pkg_dir = src_root
        if package_name:
            pkg_dir = src_root / Path(package_name.replace(".", "/"))
        pkg_dir.mkdir(parents=True, exist_ok=True)

        java_file = pkg_dir / f"{class_name}.java"
        package_decl = f"package {package_name};\n\n" if package_name else ""
        java_source = (
            f"{package_decl}"
            f"public class {class_name} {{\n"
            "    public static void main(String[] args) throws Exception {\n"
            f'        System.out.println("[{sjd_name}] Starting Spark job...");\n'
            "        // Acquire SparkSession via reflection so this class compiles without\n"
            "        // Spark on the classpath; Fabric provides Spark at runtime.\n"
            "        try {\n"
            '            Class<?> ssClass = Class.forName("org.apache.spark.sql.SparkSession");\n'
            '            Object builder = ssClass.getMethod("builder").invoke(null);\n'
            '            builder.getClass().getMethod("appName", String.class)\n'
            f'                .invoke(builder, "{sjd_name}");\n'
            '            Object spark = builder.getClass().getMethod("getOrCreate").invoke(builder);\n'
            f'            System.out.println("[{sjd_name}] SparkSession acquired.");\n'
            '            spark.getClass().getMethod("stop").invoke(spark);\n'
            "        } catch (ClassNotFoundException e) {\n"
            f'            System.out.println("[{sjd_name}] SparkSession not found on classpath (running standalone).");\n'
            "        }\n"
            f'        System.out.println("[{sjd_name}] Job completed successfully.");\n'
            "    }\n"
            "}\n"
        )
        java_file.write_text(java_source, encoding="utf-8")

        classes_root.mkdir(parents=True, exist_ok=True)

        javac = subprocess.run(
            ["javac", "--release", "11", "-d", str(classes_root), str(java_file)],
            check=False,
            capture_output=True,
            text=True,
        )
        if javac.returncode != 0:
            msg = f"Failed to compile fallback jar for SJD '{sjd_name}': {javac.stderr.strip() or javac.stdout.strip()}"
            raise RuntimeError(msg)

        output_jar.parent.mkdir(parents=True, exist_ok=True)
        jar = subprocess.run(
            ["jar", "--create", "--file", str(output_jar), "--main-class", main_class, "-C", str(classes_root), "."],
            check=False,
            capture_output=True,
            text=True,
        )
        if jar.returncode != 0:
            msg = f"Failed to package fallback jar for SJD '{sjd_name}': {jar.stderr.strip() or jar.stdout.strip()}"
            raise RuntimeError(msg)


def _ensure_local_jar_artifact(sjd: SparkJobDefinition) -> Path:
    """Ensure local jar artifact exists; build a fallback jar when absent."""
    if sjd.local_executable_artifact:
        artifact_path = (Path(__file__).parent / sjd.local_executable_artifact).resolve()
    else:
        artifact_path = (Path(__file__).parent / "deploy_artifacts" / f"{sjd.name}.jar").resolve()

    is_managed_artifact = "deploy_artifacts" in artifact_path.parts

    if artifact_path.exists() and not is_managed_artifact:
        return artifact_path

    if artifact_path.exists() and is_managed_artifact:
        log(f"Rebuilding managed jar for '{sjd.name}' at {artifact_path}", indent=True)
    else:
        log(f"Local jar not found for '{sjd.name}'. Building fallback jar at {artifact_path}", indent=True)
    _build_noop_java_jar(artifact_path, sjd.main_class, sjd.name)
    return artifact_path


def _onelake_storage_headers(cfg: dict) -> dict[str, str]:
    """Build headers for OneLake DFS upload APIs."""
    cred = get_credential()
    token = cred.get_token("https://storage.azure.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "x-ms-version": "2021-12-02",
    }
    if cfg["realm_mode"]:
        headers["x-ms-fabric-realm-id"] = cfg["realm_id"]
    return headers


def _upload_file_to_onelake(
    cfg: dict,
    ws_id: str,
    lakehouse_id: str,
    relative_path: str,
    local_file: Path,
) -> None:
    """Upload a local file to OneLake using DFS create/append/flush APIs."""
    base_url = f"https://onelake.dfs.fabric.microsoft.com/{ws_id}/{lakehouse_id}"
    storage_headers = _onelake_storage_headers(cfg)

    directory = str(Path(relative_path).parent).replace("\\", "/")
    if directory and directory != ".":
        parts = directory.split("/")
        prefix = []
        for part in parts:
            prefix.append(part)
            dir_path = "/".join(prefix)
            r = requests.put(
                f"{base_url}/{dir_path}?resource=directory",
                headers=storage_headers,
                timeout=60,
            )
            if r.status_code not in (200, 201, 409):
                msg = f"Failed creating OneLake directory '{dir_path}': {r.status_code} - {r.text}"
                raise RuntimeError(msg)

    r = requests.put(
        f"{base_url}/{relative_path}?resource=file",
        headers=storage_headers,
        timeout=60,
    )
    if r.status_code not in (200, 201):
        msg = f"Failed creating OneLake file '{relative_path}': {r.status_code} - {r.text}"
        raise RuntimeError(msg)

    data = local_file.read_bytes()
    append_headers = {**storage_headers, "Content-Type": "application/octet-stream"}
    r = requests.patch(
        f"{base_url}/{relative_path}?action=append&position=0",
        headers=append_headers,
        data=data,
        timeout=120,
    )
    if r.status_code not in (200, 202):
        msg = f"Failed appending OneLake file '{relative_path}': {r.status_code} - {r.text}"
        raise RuntimeError(msg)

    r = requests.patch(
        f"{base_url}/{relative_path}?action=flush&position={len(data)}",
        headers=storage_headers,
        timeout=60,
    )
    if r.status_code not in (200, 201):
        msg = f"Failed flushing OneLake file '{relative_path}': {r.status_code} - {r.text}"
        raise RuntimeError(msg)


def _upload_sjd_local_artifacts(cfg: dict, headers: dict[str, str], env: str) -> None:
    """Upload local SJD executable jars to current workspace for onelake aliases."""
    ws_env = get_workspace_env(cfg, env)
    if not ws_env.spark_job_definitions:
        return

    ws_id = require_workspace_id(cfg, env)
    lakehouse_map = _resolve_lakehouse_ids(cfg, headers, ws_id)

    for sjd in ws_env.spark_job_definitions:
        if not sjd.executable_file.startswith("onelake://"):
            continue
        if sjd.language.value not in {"Scala", "Java", "Scala/Java"}:
            continue

        lakehouse_name, relative_path = _parse_onelake_executable_alias(sjd.executable_file)
        lakehouse_id = lakehouse_map.get(lakehouse_name)
        if not lakehouse_id:
            msg = f"Cannot upload SJD artifact for '{sjd.name}': lakehouse '{lakehouse_name}' not found."
            raise RuntimeError(msg)

        local_artifact = _ensure_local_jar_artifact(sjd)
        _upload_file_to_onelake(cfg, ws_id, lakehouse_id, relative_path, local_artifact)
        log(f"Uploaded SJD artifact for '{sjd.name}' to OneLake path {relative_path}", indent=True)


def _rewrite_sjd_executable_paths(cfg: dict, headers: dict[str, str], env: str) -> None:
    """Rewrite SJD executable aliases to current workspace ABFSS paths before publish."""
    ws_env = get_workspace_env(cfg, env)
    if not ws_env.spark_job_definitions:
        return

    ws_id = require_workspace_id(cfg, env)
    lakehouse_map = _resolve_lakehouse_ids(cfg, headers, ws_id)
    repo_root = Path(cfg["repo_path"])

    for sjd in ws_env.spark_job_definitions:
        if not sjd.executable_file.startswith("onelake://"):
            continue

        resolved = _resolve_onelake_executable_alias(sjd.executable_file, ws_id, lakehouse_map)
        definition_path = repo_root / f"{sjd.name}.SparkJobDefinition" / "SparkJobDefinitionV1.json"
        if not definition_path.exists():
            log(f"WARNING: SJD definition file not found for '{sjd.name}' - skipping alias rewrite.")
            continue

        definition = json.loads(definition_path.read_text(encoding="utf-8"))
        definition["executableFile"] = resolved
        definition_path.write_text(json.dumps(definition, indent=4), encoding="utf-8")
        log(f"Resolved SJD '{sjd.name}' executable to current workspace path.", indent=True)


def generate_artifacts(cfg: dict, env: str) -> None:
    """Write Lakehouse / SJD / Spark Environment / Pipeline folders from the env input into the repo path."""
    ws = get_workspace_env(cfg, env)
    if not (ws.lakehouses or ws.spark_job_definitions or ws.spark_environments or ws.pipelines):
        log(f"No Lakehouses / SJDs / Spark envs / Pipelines declared for {env} - skipping artifact generation.")
        return

    repo_root = Path(cfg["repo_path"])
    repo_root.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 60)
    print(f"  Generate artifacts for {env} -> {repo_root}")
    print("=" * 60 + "\n")

    for lakehouse in ws.lakehouses:
        _generate_lakehouse(repo_root, lakehouse)
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
    """Deploy everything for ENV from the local repo to the workspace.

    Sequence:
      1. Generate SJD / Spark Env / Pipeline / Lakehouse folders under fabric/<env>/.
      2. Create any lakehouses declared in the inputs (idempotent).
      3. Publish all items via ``fabric_cicd.publish_all_items``.
      4. Seed demo tables and files into Lakehouse1/Lakehouse2.
      5. Apply OneLake data access roles + workspace Contributor for RW users.
      6. Create OneLake shortcuts declared on lakehouses.
      7. Apply job schedules declared on pipelines.
    """
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
    headers = get_headers(cfg)

    # Step 1: provision lakehouses first so items that bind to them publish cleanly.
    create_lakehouses(cfg, headers, env)

    # Upload JVM SJD artifacts to OneLake in the current workspace.
    _upload_sjd_local_artifacts(cfg, headers, env)

    # Resolve any workspace-local SJD executable aliases after lakehouse provisioning.
    _rewrite_sjd_executable_paths(cfg, headers, env)

    print("\n" + "=" * 60)
    print(f"  Deploy to {env}{mode_label}")
    print("=" * 60 + "\n")

    creds = get_credential()
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

    # Step 4: seed demo tables + files into Lakehouse1/Lakehouse2 so every env
    # (DEV/TEST/PROD) ends up with the same baseline data. Done BEFORE permissions
    # so data-access roles bind to objects that already exist.
    print("\n" + "=" * 60)
    print(f"  Seed demo data ({env})")
    print("=" * 60 + "\n")
    try:
        import seed_files
        import seed_tables

        seed_tables.run(env, realm_mode=cfg["realm_mode"])
        seed_files.run(env, realm_mode=cfg["realm_mode"])
    except Exception as exc:  # noqa: BLE001
        log(f"WARNING: seed step failed ({exc}); workspace items deployed but tables/files not seeded.")

    # Step 5: apply OneLake data access roles + workspace Contributor for RW users.
    # Permissions run last so role bindings reference tables/files that now exist.
    assign_permissions(cfg, headers, env)

    # Step 6: create OneLake shortcuts declared on lakehouses (idempotent).
    create_shortcuts(cfg, headers, env)

    # Step 7: create / update job schedules declared on pipelines (idempotent).
    apply_schedules(cfg, headers, env)


def apply_schedules(cfg: dict, headers: dict, env: str = "DEV") -> None:
    """Create or update job schedules declared on pipeline schedule fields.

    Uses ``POST /workspaces/{ws}/items/{itemId}/jobSchedules``. Idempotent:
    matching schedules are updated in place; otherwise they are created.
    """
    ws_env = get_workspace_env(cfg, env)
    scheduled = [p for p in ws_env.pipelines if p.schedule is not None or getattr(p, "schedules", [])]
    if not scheduled:
        return

    print("\n" + "=" * 60)
    print(f"  Apply Schedules ({env})")
    print("=" * 60 + "\n")

    ws_id = require_workspace_id(cfg, env)

    # Look up pipeline IDs by display name.
    resp = requests.get(f"{cfg['base_url']}/workspaces/{ws_id}/dataPipelines", headers=headers)
    if resp.status_code != 200:
        log(f"WARNING: cannot list pipelines ({resp.status_code}); skipping schedules.")
        return
    pipeline_ids = {p["displayName"]: p["id"] for p in resp.json().get("value", [])}

    for pipeline in scheduled:
        pid = pipeline_ids.get(pipeline.name)
        if not pid:
            log(f"WARNING: Pipeline '{pipeline.name}' not found; skipping schedule.")
            continue

        schedule_defs = list(getattr(pipeline, "schedules", []) or [])
        if pipeline.schedule is not None:
            schedule_defs.insert(0, pipeline.schedule)

        for sched in schedule_defs:
            configuration = _build_schedule_configuration(sched, pipeline.name)
            if configuration is None:
                continue

            payload = {"enabled": sched.enabled, "configuration": configuration}
            url = f"{cfg['base_url']}/workspaces/{ws_id}/items/{pid}/jobs/{sched.job_type}/schedules"

            existing = requests.get(url, headers=headers)
            schedule_id = None
            if existing.status_code == 200:
                for existing_sched in existing.json().get("value", []):
                    if _schedule_matches(existing_sched, sched, configuration):
                        schedule_id = existing_sched.get("id")
                        break

            if schedule_id:
                log(f"Updating schedule for '{pipeline.name}' ({sched.type})...")
                r = requests.patch(f"{url}/{schedule_id}", headers=headers, json=payload)
            else:
                log(f"Creating schedule for '{pipeline.name}' ({sched.type})...")
                r = requests.post(url, headers=headers, json=payload)

            if r.status_code in (200, 201):
                log("  -> Applied.", indent=True)
            else:
                log(f"  -> Failed ({r.status_code}): {r.text}", indent=True)


def _build_schedule_configuration(sched: Schedule, pipeline_name: str) -> dict | None:
    if sched.type == "Cron":
        return {
            "type": "Cron",
            "interval": sched.interval,
            "startDateTime": sched.start,
            "endDateTime": sched.end,
            "localTimeZoneId": sched.timezone,
        }
    if sched.type == "Daily":
        return {
            "type": "Daily",
            "times": sched.times,
            "startDateTime": sched.start,
            "endDateTime": sched.end,
            "localTimeZoneId": sched.timezone,
        }
    if sched.type == "Weekly":
        return {
            "type": "Weekly",
            "weekdays": sched.weekdays,
            "times": sched.times,
            "startDateTime": sched.start,
            "endDateTime": sched.end,
            "localTimeZoneId": sched.timezone,
        }
    log(f"WARNING: Unsupported schedule type '{sched.type}' on '{pipeline_name}'.")
    return None


def _schedule_matches(existing_sched: dict, desired_sched: Schedule, desired_config: dict) -> bool:
    existing_config = existing_sched.get("configuration", {})
    if existing_config.get("type") != desired_sched.type:
        return False

    if desired_sched.type == "Cron":
        return existing_config.get("interval") == desired_config.get("interval")

    if desired_sched.type == "Daily":
        return sorted(existing_config.get("times", [])) == sorted(desired_config.get("times", []))

    if desired_sched.type == "Weekly":
        return sorted(existing_config.get("times", [])) == sorted(desired_config.get("times", [])) and sorted(
            existing_config.get("weekdays", [])
        ) == sorted(desired_config.get("weekdays", []))

    return False


def create_shortcuts(cfg: dict, headers: dict, env: str = "DEV") -> None:
    """Create OneLake shortcuts declared on LakehouseDefinition.shortcuts.

    Idempotent: each shortcut is recreated only if it does not already exist.
    """
    ws_env = get_workspace_env(cfg, env)
    if not any(lh.shortcuts for lh in ws_env.lakehouses):
        return

    print("\n" + "=" * 60)
    print(f"  Create Shortcuts ({env})")
    print("=" * 60 + "\n")

    ws_id = require_workspace_id(cfg, env)
    lh_ids = _resolve_lakehouse_ids(cfg, headers, ws_id)

    for lh in ws_env.lakehouses:
        if not lh.shortcuts:
            continue
        lh_id = lh_ids.get(lh.name)
        if not lh_id:
            log(f"WARNING: Lakehouse '{lh.name}' not found in workspace; skipping shortcuts.")
            continue

        for sc in lh.shortcuts:
            if not sc.enabled:
                log(f"Skipping disabled shortcut '{lh.name}/{sc.path}/{sc.name}'.")
                continue

            target: dict = {}
            st = sc.source_type
            if st == "OneLake":
                target["oneLake"] = {
                    "workspaceId": sc.source_workspace_id,
                    "itemId": sc.source_item_id,
                    "path": sc.source_path,
                }
            elif st == "AdlsGen2":
                target["adlsGen2"] = {
                    "location": sc.source_location,
                    "subpath": sc.source_subpath,
                    "connectionId": sc.connection_id,
                }
            elif st == "S3":
                target["amazonS3"] = {
                    "location": sc.source_location,
                    "subpath": sc.source_subpath,
                    "connectionId": sc.connection_id,
                }
            else:
                log(f"WARNING: Unsupported shortcut source_type '{st}' on '{lh.name}/{sc.name}'.")
                continue

            payload = {"name": sc.name, "path": sc.path, "target": target}
            log(f"Creating shortcut '{lh.name}/{sc.path}/{sc.name}' -> {st}...")
            resp = requests.post(
                f"{cfg['base_url']}/workspaces/{ws_id}/items/{lh_id}/shortcuts",
                headers=headers,
                json=payload,
            )
            if resp.status_code in (200, 201):
                log("  -> Created.", indent=True)
            elif resp.status_code == 409:
                log("  -> Already exists; skipping.", indent=True)
            else:
                log(
                    f"  -> Failed ({resp.status_code}): {resp.text}",
                    indent=True,
                )


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
    try:
        cred = get_credential()
        graph_token = cred.get_token("https://graph.microsoft.com/.default").token
        graph_headers = {
            "Authorization": f"Bearer {graph_token}",
            "Content-Type": "application/json",
        }

        resp = requests.get(
            f"https://graph.microsoft.com/v1.0/users/{email}",
            headers=graph_headers,
            timeout=30,
        )
        user_data: dict = {}
        if resp.status_code != 200:
            search_headers = {**graph_headers, "ConsistencyLevel": "eventual"}
            resp = requests.get(
                f"https://graph.microsoft.com/v1.0/users?$filter=mail eq '{email}'&$select=id",
                headers=search_headers,
                timeout=30,
            )
            if resp.status_code == 200 and resp.json().get("value"):
                user_data = resp.json()["value"][0]
            else:
                log(f"WARNING: Could not resolve user {email}: not found", indent=True)
                return "", ""
        else:
            user_data = resp.json()

        object_id = user_data.get("id", "")

        org_resp = requests.get(
            "https://graph.microsoft.com/v1.0/organization",
            headers=graph_headers,
            timeout=30,
        )
        tenant_id = ""
        if org_resp.status_code == 200:
            orgs = org_resp.json().get("value", [])
            if orgs:
                tenant_id = orgs[0].get("id", "")

        return object_id, tenant_id
    except BaseException as exc:  # noqa: BLE001
        log(f"WARNING: Could not resolve user {email}: {exc}", indent=True)
        return "", ""


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
                    {
                        "attributeName": "Action",
                        "attributeValueIncludedIn": [entry.permission.value],
                    },
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
        log(
            f"WARNING: No objectId for {email} - table role will have no members",
            indent=True,
        )

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
                    {
                        "attributeName": "Action",
                        "attributeValueIncludedIn": [entry.permission.value],
                    },
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
        log(
            f"WARNING: No objectId for {email} - file role will have no members",
            indent=True,
        )

    return role_def


def _grant_workspace_contributor(cfg: dict, headers: dict, ws_id: str, email: str, display_name: str) -> None:
    """Add a user as workspace Contributor (idempotent). Required for Write access on lakehouses."""
    try:
        object_id, tenant_id = _resolve_user_info(email)
    except BaseException as exc:  # noqa: BLE001
        log(
            f"WARNING: Could not resolve {email} for workspace Contributor grant: {exc}",
            indent=True,
        )
        return
    if not object_id:
        log(
            f"WARNING: Could not resolve {email} for workspace Contributor grant - skipping.",
            indent=True,
        )
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
        log(
            f"  Workspace Contributor already assigned for {display_name or email}",
            indent=True,
        )
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
    log(
        f"Found {len(lakehouse_map)} lakehouse(s): {', '.join(lakehouse_map.keys())}",
        indent=True,
    )
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
            log(
                f"  Granting {entry.permission.value} to {entry.display_name or entry.email}",
                indent=True,
            )

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
        try:
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
        except BaseException as exc:  # noqa: BLE001
            log(f"WARNING: Failed to apply permissions for '{lh.name}': {exc}", indent=True)
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
                log(
                    f"Generic delete failed ({del_resp.status_code}); retrying via /{type_path}...",
                    indent=True,
                )
                del_resp = requests.delete(
                    f"{cfg['base_url']}/workspaces/{ws_id}/{type_path}/{item_id}",
                    headers=headers,
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
        "  create [ENV]            - Create workspace(s) and assign roles. If ENV omitted, all envs.\n"
        "  delete-workspace [ENV]  - Delete the ENV workspace entirely (escape hatch)\n"
        "  clean [ENV]             - Delete unlinked workspace items colliding with repo (escape hatch)\n"
        "  generate [ENV]          - Generate SJD/Env/Pipeline folders for ENV (default DEV)\n"
        "  deploy [ENV]            - Generate artifacts -> create lakehouses -> publish all\n"
        "                            items -> apply data permissions -> seed demo tables/files\n"
        "                            (default DEV)\n"
        "  all [ENV]               - check -> create -> deploy (default DEV)\n\n"
        "Environment variables:\n"
        "  FABRICCICD_USER         - When set, targets a personal workspace per env\n"
        "                            (``<workspace_prefix>-<user>-<env>``). Personal IDs are\n"
        "                            stored under ``fabriccicd_inputs/.local/``.\n\n"
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
    elif command == "deploy":
        deploy(cfg, env)
    elif command == "generate":
        generate_artifacts(cfg, env)
    elif command == "all":
        if not check_repo(cfg):
            log("Aborting 'all' due to repo check failures.")
            sys.exit(1)
        headers = get_headers(cfg)
        create_workspaces(cfg, headers, [env])
        deploy(cfg, env)
    else:
        print(f"Unknown command: {command}\n")
        print(usage)
        sys.exit(1)


if __name__ == "__main__":
    main()
