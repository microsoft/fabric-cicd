# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Captures route traces from a live Fabric workspace deployment into a JSON GZIP trace file.

import gzip
import os
import shutil
import sys
from pathlib import Path

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

    environment = "PPE"

    repository_directory = root_directory / "sample" / "workspace"
    item_type_in_scope = [
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
    for flag in ["enable_shortcut_publish", "continue_on_shortcut_failure"]:
        fabric_cicd.append_feature_flag(flag)
    target_workspace = fabric_cicd.FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=str(repository_directory),
        item_type_in_scope=item_type_in_scope,
    )
    fabric_cicd.publish_all_items(target_workspace)

    print("Publish completed successfully")

    # The raw JSON trace file is very large; GZIP compress it generate a compact version
    # that can be used by tests. The raw trace file is still left in place for 
    # debugging purposes.
    #
    trace_file = root_directory / "http_trace.json"
    compressed_file = root_directory / "http_trace.json.gz"
    with trace_file.open("rb") as f_in, gzip.open(compressed_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    print(f"Compressed trace file to {compressed_file}")


if __name__ == "__main__":
    main()
