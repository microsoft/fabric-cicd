# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Integration test for publish operations using mock Fabric API server."""

import importlib
import os
from pathlib import Path

import pytest
from credentials import StaticTokenCredential
from fixtures.mock_fabric_server import MOCK_SERVER_PORT, MockFabricServer

import fabric_cicd
import fabric_cicd.constants


@pytest.fixture
def mock_fabric_api_server():
    """
    Start mock Fabric API server for the test.

    Yields the server and sets environment variables for API URLs.
    """
    tests_dir = Path(__file__).parent
    trace_file = tests_dir / "fixtures" / "http_trace.csv"

    if not trace_file.exists():
        pytest.skip("http_trace.csv not found - run debug_publish_all.py first to generate trace data")

    server = MockFabricServer(trace_file, port=MOCK_SERVER_PORT)

    original_default_api = os.environ.get("DEFAULT_API_ROOT_URL")
    original_fabric_api = os.environ.get("FABRIC_API_ROOT_URL")

    os.environ["DEFAULT_API_ROOT_URL"] = f"http://127.0.0.1:{MOCK_SERVER_PORT}"
    os.environ["FABRIC_API_ROOT_URL"] = f"http://127.0.0.1:{MOCK_SERVER_PORT}"

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

    importlib.reload(fabric_cicd.constants)


def test_publish_all_items_integration(mock_fabric_api_server):  # noqa: ARG001
    """Test full publish_all_items workflow using mocked API responses."""
    workspace_id = "b4e2b127-32f1-49e7-a3aa-a87dba44990c"
    environment_key = "PPE"

    root_directory = Path(__file__).resolve().parent.parent
    artifacts_folder = root_directory / "sample" / "workspace"

    item_types_to_deploy = [
        "Dataflow",
        "DataPipeline",
        "Eventhouse",
        "KQLDatabase",
        "KQLQueryset",
        "Lakehouse",
        "Notebook",
        "Reflex",
        "Report",
        "SemanticModel",
        "SparkJobDefinition",
        "VariableLibrary",
    ]

    token_credential = StaticTokenCredential()

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
