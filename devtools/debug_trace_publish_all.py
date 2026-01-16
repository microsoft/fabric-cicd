#!/usr/bin/env python

"""Captures route traces from a real Fabric workspace deployment into a CSV file."""

import csv
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

csv.field_size_limit(sys.maxsize)

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

from azure.identity import DefaultAzureCredential

import fabric_cicd
from fabric_cicd._common._http_tracer import HTTPRequest


def get_route_key(request_b64: str) -> str:
    """Extract route key from base64 encoded request."""
    try:
        request = HTTPRequest.from_b64(request_b64)
        parsed_url = urlparse(request.url)
        route = parsed_url.path
        if parsed_url.query:
            route += f"?{parsed_url.query}"
        return f"{request.method} {route}"
    except Exception:
        return ""


def merge_trace_files():
    """Merge new trace file into fixtures, skipping duplicate routes."""
    new_trace_file = root_directory / "http_trace.csv"
    fixture_trace_file = root_directory / "tests" / "fixtures" / "http_trace.csv"

    if not new_trace_file.exists():
        print("No new trace file generated")
        return

    existing_routes = set()
    if fixture_trace_file.exists():
        with fixture_trace_file.open("r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                route_key = get_route_key(row["request_b64"])
                if route_key:
                    existing_routes.add(route_key)

    new_rows = []
    with new_trace_file.open("r") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            route_key = get_route_key(row["request_b64"])
            if route_key and route_key not in existing_routes:
                new_rows.append(row)
                existing_routes.add(route_key)

    if new_rows:
        with fixture_trace_file.open("a") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            for row in new_rows:
                writer.writerow(row)
        print(f"Added {len(new_rows)} new routes to fixture trace file")
    else:
        print("No new routes to add")

    new_trace_file.unlink()

    # Deduplicate: keep only first and last entry per route
    from collections import defaultdict

    route_rows = defaultdict(list)
    with fixture_trace_file.open("r") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            route_key = get_route_key(row["request_b64"])
            if route_key:
                route_rows[route_key].append(row)

    deduplicated_rows = []
    for _route_key, rows in route_rows.items():
        if len(rows) == 1:
            deduplicated_rows.append(rows[0])
        elif len(rows) >= 2:
            deduplicated_rows.append(rows[0])
            if rows[0] != rows[-1]:
                deduplicated_rows.append(rows[-1])

    with fixture_trace_file.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in deduplicated_rows:
            writer.writerow(row)

    print("Deduplicated trace file: kept max 2 entries per route")


def main():
    """Capture HTTP trace while publishing all items to Fabric workspace."""

    os.environ["FABRIC_CICD_HTTP_TRACE_ENABLED"] = "1"
    os.environ["FABRIC_CICD_HTTP_TRACE_FILE"] = str(root_directory / "http_trace.csv")
    workspace_id = "b4e2b127-32f1-49e7-a3aa-a87dba44990c"
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
        "Notebook",
        "Reflex",
        "Report",
        "SemanticModel",
        "SparkJobDefinition",
        "VariableLibrary",
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
    merge_trace_files()


if __name__ == "__main__":
    main()
