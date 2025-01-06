"""
Example of optional parameters for FabricWorkspace and publish functions.
"""

from fabric_cicd import FabricWorkspace, enable_debug_log, publish_all_items, unpublish_all_orphan_items

# Sample values for FabricWorkspace parameters
workspace_id = "your-workspace-id"
environment = "your-environment"
repository_directory = "your-repository-directory"
item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]
base_api_url = "https://msitapi.fabric.microsoft.com/"
token_credential = TokenCredential

# Optional: Print all API calls to log file
enable_debug_log()

# Initialize the FabricWorkspace object with the required and optional parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=environment,
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    # Optional: Override base URL in rare cases where it's different
    base_api_url=base_api_url,
    # Optional: Override token credential to use a different authentication
    token_credential=token_credential,
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(
    target_workspace,
    # Optional: Exclude item names matching the regex pattern
    item_name_exclude_regex=r"^DEBUG.*",
)
