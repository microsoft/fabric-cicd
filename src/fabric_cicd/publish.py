from fabric_cicd._common._custom_print import print_header
import fabric_cicd._items as items
import json
import base64
import re

"""
Functions to deploy Fabric workspace items.
"""


def publish_all_items(fabric_workspace_obj):
    """
    Publishes all items defined in item_type_in_scope list.
    """
    if "Environment" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Environments")
        items._publish_environments(fabric_workspace_obj)
    if "Notebook" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Notebooks")
        items._publish_notebooks(fabric_workspace_obj)
    if "DataPipeline" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing DataPipelines")
        items._publish_datapipelines(fabric_workspace_obj)


def unpublish_all_orphan_items(fabric_workspace_obj, item_name_exclude_regex):
    """
    Unpublishes all orphaned items not present in the repository except for those matching the exclude regex.

    :param item_name_exclude_regex: Regex pattern to exclude specific items from being unpublished.
    """
    regex_pattern = re.compile(item_name_exclude_regex)

    fabric_workspace_obj.refresh_deployed_items()
    print_header("Unpublishing Orphaned Items")

    # Order of unpublishing to handle dependencies cleanly
    # TODO need to expand this to be more dynamic
    unpublish_order = [
        x
        for x in ["DataPipeline", "Notebook", "Environment"]
        if x in fabric_workspace_obj.item_type_in_scope
    ]

    for item_type in unpublish_order:
        deployed_names = set(
            fabric_workspace_obj.deployed_items.get(item_type, {}).keys()
        )
        repository_names = set(
            fabric_workspace_obj.repository_items.get(item_type, {}).keys()
        )

        to_delete_set = deployed_names - repository_names
        to_delete_list = [
            name for name in to_delete_set if not regex_pattern.match(name)
        ]

        if item_type == "DataPipeline":
            # need to first define order of delete
            unsorted_pipeline_dict = {}

            for item_name in to_delete_list:
                # Get deployed item definition
                # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition
                item_guid = fabric_workspace_obj.deployed_items[item_type][item_name][
                    "guid"
                ]
                response = fabric_workspace_obj.endpoint.invoke(
                    method="POST",
                    url=f"{fabric_workspace_obj.base_api_url}/items/{item_guid}/getDefinition",
                )

                for part in response["body"]["definition"]["parts"]:
                    if part["path"] == "pipeline-content.json":
                        # Decode Base64 string to dictionary
                        decoded_bytes = base64.b64decode(part["payload"])
                        decoded_string = decoded_bytes.decode("utf-8")
                        unsorted_pipeline_dict[item_name] = json.loads(decoded_string)

            # Determine order to delete w/o dependencies
            to_delete_list = items._sort_datapipelines(
                fabric_workspace_obj, unsorted_pipeline_dict, "Deployed"
            )

        for item_name in to_delete_list:
            fabric_workspace_obj.unpublish_item(
                item_name=item_name, item_type=item_type
            )
