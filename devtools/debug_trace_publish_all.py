#!/usr/bin/env python

"""Captures route traces from a live Fabric workspace deployment into a JSON trace file."""

import csv
import os
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

from azure.identity import DefaultAzureCredential

import fabric_cicd


def main():
    """Capture HTTP trace while publishing all items to Fabric workspace."""

    os.environ["FABRIC_CICD_HTTP_TRACE_ENABLED"] = "1"
    os.environ["FABRIC_CICD_HTTP_TRACE_FILE"] = str(root_directory / "http_trace.json")

    workspace_id = os.environ.get("FABRIC_WORKSPACE_ID")
    if not workspace_id:
        msg = "FABRIC_WORKSPACE_ID environment variable must be set"
        raise ValueError(msg)

    environment_key = "PPE"

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
    token_credential = DefaultAzureCredential()
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

    print("Publish completed successfully")


if __name__ == "__main__":
    main()
