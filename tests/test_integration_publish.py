# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Integration test for publish operations using mock Fabric API server."""

import gzip
import importlib
import os
import shutil
from pathlib import Path
from urllib.parse import urlparse

import pytest
from fixtures.credentials import DummyTokenCredential
from fixtures.mock_fabric_server import MOCK_SERVER_PORT, MockFabricServer

import fabric_cicd
import fabric_cicd._common._validate_env_vars as validate_env_vars
import fabric_cicd.constants


@pytest.fixture
def allow_localhost_http_for_integration(monkeypatch: pytest.MonkeyPatch):
    """
    Test-only override: allow http://localhost for mocked integration servers.
    """
    real_validate = validate_env_vars.validate_env_var_api_url

    def _validate_api_url_test(env_var_name: str, default_value: str) -> str:
        value = os.environ.get(env_var_name, default_value)
        parsed = urlparse(value)
        host = (parsed.hostname or "").lower()

        if parsed.scheme == "http" and host in {"localhost", "127.0.0.1", "::1"}:
            return value.rstrip("/")

        return real_validate(env_var_name, default_value)

    monkeypatch.setattr(validate_env_vars, "validate_env_var_api_url", _validate_api_url_test)
    return


@pytest.fixture
def mock_fabric_api_server(allow_localhost_http_for_integration):  # noqa: ARG001
    """
    Start mock Fabric API server for the test.

    Yields the server and sets environment variables for API URLs.
    """
    tests_dir = Path(__file__).parent
    trace_file_gz = tests_dir / "fixtures" / MockFabricServer.HTTP_TRACE_FILE
    trace_file = trace_file_gz.with_suffix("")

    if not trace_file_gz.exists():
        pytest.skip(
            "http_trace.json.gz not found - run devtools/debug_trace_deployment.py first to generate trace data"
        )

    if trace_file.exists():
        trace_file.unlink()
    with gzip.open(trace_file_gz, "rb") as f_in, trace_file.open("wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    server = MockFabricServer(trace_file, port=MOCK_SERVER_PORT)

    original_default_api = os.environ.get("DEFAULT_API_ROOT_URL")
    original_fabric_api = os.environ.get("FABRIC_API_ROOT_URL")
    original_retry_delay = os.environ.get("FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS")

    os.environ["DEFAULT_API_ROOT_URL"] = f"http://127.0.0.1:{MOCK_SERVER_PORT}"
    os.environ["FABRIC_API_ROOT_URL"] = f"http://127.0.0.1:{MOCK_SERVER_PORT}"
    os.environ["FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS"] = "0"

    # reload only after env is set and override fixture is active
    importlib.reload(fabric_cicd.constants)

    server.start()

    yield server

    server.stop()

    if original_default_api is not None:
        os.environ["DEFAULT_API_ROOT_URL"] = original_default_api
    else:
        os.environ.pop("DEFAULT_API_ROOT_URL", None)

    if original_fabric_api is not None:
        os.environ["FABRIC_API_ROOT_URL"] = original_fabric_api
    else:
        os.environ.pop("FABRIC_API_ROOT_URL", None)

    if original_retry_delay is not None:
        os.environ["FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS"] = original_retry_delay
    else:
        os.environ.pop("FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS", None)

    importlib.reload(fabric_cicd.constants)


def test_publish_all_items_integration(mock_fabric_api_server):  # noqa: ARG001
    """Test full publish_all_items workflow using mocked API responses."""
    workspace_id = "00000000-0000-0000-0000-000000000000"
    environment_key = "PPE"

    root_directory = Path(__file__).resolve().parent.parent
    artifacts_folder = root_directory / "sample" / "workspace"

    item_types_to_deploy = [
        "Dataflow",
        "DataPipeline",
        "Environment",
        "Eventhouse",
        "Eventstream",
        "KQLDatabase",
        "KQLQueryset",
        "Lakehouse",
        "MirroredDatabase",
        "MLExperiment",
        "Notebook",
        "Reflex",
        "Report",
        "SemanticModel",
        "SparkJobDefinition",
        "SQLDatabase",
        "VariableLibrary",
        "Warehouse",
    ]

    token_credential = DummyTokenCredential()

    for flag in ["enable_shortcut_publish", "continue_on_shortcut_failure"]:
        fabric_cicd.append_feature_flag(flag)

    target_workspace = fabric_cicd.FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment_key,
        repository_directory=str(artifacts_folder),
        item_type_in_scope=item_types_to_deploy,
        token_credential=token_credential,
    )

    fabric_cicd.publish_all_items(target_workspace)

    assert True, "Publish completed successfully"
