# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug locally against the github repo

import sys
from pathlib import Path
import os

from azure.identity import ClientSecretCredential


root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

from fabric_cicd import (
    FabricWorkspace,
    append_feature_flag,
    change_log_level,
    publish_all_items,
    unpublish_all_orphan_items,
)

# Import feature_flag here to avoid circular import
from fabric_cicd import feature_flag

# Uncomment to enable debug
# change_log_level()

# Uncomment to add feature flag
# append_feature_flag("disable_executing_identity")
append_feature_flag("enable_deployment_variables")
os.environ["$ENV:SQL_CON_PPE_VAR"] = "104e7ff8-70c7-4e84-a6ad-e4e1f4ecd1b5"
os.environ["$ENV:LAKEHOUSE_PPE_VAR"] = "104e7ff8-70c7-4e84-a6ad-e4e1f4ecd1b5"
os.environ["$ENV:LAKEHOUSE_WORKSPACE_PPE_VAR"] = "f0e3fa10-e2d7-4ce6-a508-4e6e16ba3a27"

# The defined environment values should match the names found in the parameter.yml file
workspace_id = "f0e3fa10-e2d7-4ce6-a508-4e6e16ba3a27"
environment = "PPE"

# In this example, our workspace content sits within the root/sample/workspace directory
repository_directory = str(root_directory / "sample" / "workspace")

# Explicitly define which of the item types we want to deploy
# item_type_in_scope = ["DataPipeline", "Notebook", "Environment", "SemanticModel", "Report"]
item_type_in_scope = ["Notebook"]

# Uncomment to use SPN auth
# client_id = "your-client-id"
# client_secret = "your-client-secret"
# tenant_id = "your-tenant-id"
# token_credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=environment,
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    # Override base url in rare cases where it's different
    base_api_url="https://api.fabric.microsoft.com/",
    # Uncomment to use SPN auth
    # token_credential=token_credential,
)

# Uncomment to publish
# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Uncomment to unpublish
# Unpublish all items defined in scope not found in repository
# unpublish_all_orphan_items(target_workspace, item_name_exclude_regex=r"^DEBUG.*")
