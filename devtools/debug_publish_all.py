#!/usr/bin/env python

import sys
from pathlib import Path

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

import fabric_cicd
from azure.identity import DefaultAzureCredential


def main():
    """Main debug function - mimics the user's API call pattern"""

    workspace_id = "b4e2b127-32f1-49e7-a3aa-a87dba44990c"
    environment_key = "PPE"
    
    artifacts_folder = root_directory / "sample" / "workspace"
    item_types_to_deploy = [
        "Lakehouse",
        "VariableLibrary",
        "Dataflow",
        "DataPipeline",
        "Notebook",
        "Environment",
        "SemanticModel",
        "Report",
        "Eventhouse",
        "KQLDatabase",
        "KQLQueryset",
        "Reflex",
        "Eventstream",
        "SparkJobDefinition",
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
    
    print(f"Publish completed successfully")


if __name__ == "__main__":
    main()
