from deployfabric.FabricEndpoint import FabricEndpoint, CustomPrint
import os, json, base64, re, yaml
from collections import defaultdict, deque

class FabricWorkspace:
    """
    A class to manage and publish workspace items to the Fabric API.
    """
    def __init__(self, workspace_id, environment, repository_directory, item_type_in_scope, base_api_url="https://api.fabric.microsoft.com/", debug_output=False):
        """
        Initializes the FabricWorkspace instance.

        :param workspace_id: The ID of the workspace to interact with.
        :param environment: The environment to be used for parameterization.
        :param repository_directory: Directory path where repository items are located.
        :param item_type_in_scope: Item types that should be deployed for given workspace.
        :param base_api_url: Base URL for the Fabric API. Defaults to the Fabric API endpoint.
        :param debug_output: If True, enables debug output for API requests.
        """
        self.endpoint = FabricEndpoint(debug_output=debug_output)
        self.workspace_id = workspace_id
        self.environment = environment
        self.repository_directory = repository_directory
        self.item_type_in_scope = item_type_in_scope
        self.base_api_url = f"{base_api_url}/v1/workspaces/{workspace_id}"
        self.debug_output = debug_output

        self.refresh_deployed_items()
        self.refresh_repository_items()

    def refresh_repository_items(self):
        """
        Refreshes the repository_items dictionary by scanning the repository directory.
        """

        self.repository_items = {}

        for directory in os.scandir(self.repository_directory):
            if directory.is_dir():
                item_metadata_path = os.path.join(directory.path, ".platform")

                # Attempt to read metadata file
                try:
                    with open(item_metadata_path, 'r') as file:
                        item_metadata = json.load(file)
                except FileNotFoundError:
                    raise ValueError(f"{item_metadata_path} path does not exist in the specified repository.")
                except json.JSONDecodeError:
                    raise ValueError(f"Error decoding JSON in {item_metadata_path}.")

                # Ensure required metadata fields are present
                if 'type' not in item_metadata['metadata'] or 'displayName' not in item_metadata['metadata']:
                    raise ValueError(f"displayName & type are required in {item_metadata_path}")

                item_type = item_metadata['metadata']['type']
                item_description = item_metadata['metadata'].get('description', '')
                item_name = item_metadata['metadata']['displayName']
                item_logical_id = item_metadata['config']['logicalId']

                # Get the GUID if the item is already deployed
                item_guid = self.deployed_items.get(item_type, {}).get(item_name, {}).get("guid", "")

                if item_type not in self.repository_items:
                    self.repository_items[item_type] = {}

                # Add the item to the repository_items dictionary
                self.repository_items[item_type][item_name] = {
                    "description": item_description,
                    "path": directory.path,
                    "guid": item_guid,
                    "logical_id": item_logical_id
                }

        # load parameters if file is present
        parameter_file_path = os.path.join(self.repository_directory, "parameter.yml")
        self.environment_parameter = {}

        if os.path.isfile(parameter_file_path):
            print(parameter_file_path)
            with open(parameter_file_path, 'r') as yaml_file:
                self.environment_parameter = yaml.safe_load(yaml_file)

    def refresh_deployed_items(self):
        """
        Refreshes the deployed_items dictionary by querying the Fabric workspace items API.
        """
        # Get all items in workspace
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item
        response = self.endpoint.invoke(method="GET", url=f"{self.base_api_url}/items")

        self.deployed_items = {}

        for item in response["body"]["value"]:
            item_type = item["type"]
            item_description = item["description"]
            item_name = item["displayName"]
            item_guid = item["id"]

            # Add an empty dictionary if the item type hasn't been added yet
            if item_type not in self.deployed_items:
                self.deployed_items[item_type] = {}

            # Add item details to the deployed_items dictionary
            self.deployed_items[item_type][item_name] = {
                "description": item_description,
                "guid": item_guid
            }

    def publish_item(self, item_name, item_type, excluded_files={".platform"}, full_publish=True):
        """
        Publishes or updates an item in the Fabric Workspace.

        :param item_name: Name of the item to publish.
        :param item_type: Type of the item (e.g., Notebook, Environment).
        :param excluded_files: Set of file names to exclude from the publish process.
        :param full_publish: If True, publishes the full item with its content. If False, only publishes metadata (for items like Environments).
        """
        item_path = self.repository_items[item_type][item_name]["path"]
        item_guid = self.repository_items[item_type][item_name]["guid"]
        item_description = self.repository_items[item_type][item_name]["description"]

        metadata_body = {
            'displayName': item_name,
            'type': item_type,
            'description': item_description
        }

        if full_publish:
            item_payload = []
            for root, _, files in os.walk(item_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, item_path)

                    if file not in excluded_files:
                        with open(full_path, 'r', encoding='utf-8') as f:
                            raw_file = f.read()

                        # Replace feature branch workspace IDs with target workspace IDs in data pipeline activities.
                        if item_type == 'DataPipeline':
                            raw_file = self.replace_activity_workspace_ids(raw_file, "Repository")
                        
                        # Replace default workspace id with target workspace id
                        # TODO Remove this once bug is resolved in API
                        if item_type == 'Notebook':                           
                            default_workspace_string = '"workspaceId": "00000000-0000-0000-0000-000000000000"'
                            target_workspace_string = f'"workspaceId": "{self.workspace_id}"'
                            raw_file = raw_file.replace(default_workspace_string, target_workspace_string)
                        
                        # Replace logical IDs with deployed GUIDs.
                        replaced_raw_file = self.replace_logical_ids(raw_file)
                        replaced_raw_file = self.replace_parameters(replaced_raw_file)

                        byte_file = replaced_raw_file.encode('utf-8')
                        payload = base64.b64encode(byte_file).decode('utf-8')

                        item_payload.append({
                            'path': relative_path,
                            'payload': payload,
                            'payloadType': 'InlineBase64'
                        })

            definition_body = {
                'definition': {
                    'parts': item_payload
                }
            }
            combined_body = {**metadata_body, **definition_body}
        else:
            combined_body = metadata_body

        CustomPrint.timestamp(f"Publishing {item_type} '{item_name}'")

        if not item_guid:
            # Create a new item if it does not exist
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item
            item_create_response = self.endpoint.invoke(method="POST", url=f"{self.base_api_url}/items", body=combined_body)
            item_guid = item_create_response["body"]["id"]
            self.repository_items[item_type][item_name]["guid"] = item_guid
        else:
            if full_publish:
                # Update the item's definition if full publish is required
                # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item-definition
                self.endpoint.invoke(method="POST", url=f"{self.base_api_url}/items/{item_guid}/updateDefinition", body=definition_body)

            # Remove the 'type' key as it's not supported in the update-item API
            metadata_body.pop('type', None)

            # Update the item's metadata
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item
            self.endpoint.invoke(method="PATCH", url=f"{self.base_api_url}/items/{item_guid}", body=metadata_body)

        CustomPrint.sub_line("Published")

    def replace_logical_ids(self, raw_file):
        """
        Replaces logical IDs with deployed GUIDs in the raw file content.

        :param raw_file: The raw file content where logical IDs need to be replaced.
        :return: The raw file content with logical IDs replaced by GUIDs.
        """
        for items in self.repository_items.values():
            for item_dict in items.values():
                logical_id = item_dict["logical_id"]
                item_guid = item_dict["guid"]

                if logical_id in raw_file:
                    if item_guid == "":
                        raise Exception("Cannot replace logical ID as referenced item is not yet deployed.")
                    else:
                        raw_file = raw_file.replace(logical_id, item_guid)

        return raw_file

    def replace_parameters(self, raw_file):
        """
        Replaces values found in parameter file with the chosen environment value.

        :param raw_file: The raw file content where parameter values need to be replaced.
        """
        if "find_replace" in self.environment_parameter:
            for key, parameter_dict in self.environment_parameter["find_replace"].items():
                if key in raw_file:
                    # if environment not found in dict
                    if self.environment in parameter_dict:
                        # replace any found references with specified environment value
                        raw_file = raw_file.replace(key, parameter_dict[self.environment])

        return raw_file
    
    def replace_activity_workspace_ids(self, raw_file, lookup_type):
        """
        Replaces feature branch workspace ID referenced in data pipeline activities with target workspace ID in the raw file content.
    
        :param raw_file: The raw file content where workspace IDs need to be replaced.
        :return: The raw file content with feature branch workspace IDs replaced by target workspace IDs.
        """
        # Create a dictionary from the raw_file
        item_content_dict = json.loads(raw_file)

        def find_and_replace_activity_workspace_ids(input_object):
            """
            Recursively scans through JSON to find and replace feature branch workspace IDs in nested and non-nested activities where workspaceId 
            property exists (e.g. Trident Notebook). Note: the function can be modified to process other pipeline activities where workspaceId exists.
            
            :param input_object: Object can be a dictionary or list present in the input JSON.
            """
            # Check if the current object is a dictionary
            if isinstance(input_object, dict):
                target_workspace_id = self.workspace_id

                # Iterate through the activities and search for TridentNotebook activities
                for key, value in input_object.items():
                    if key == "type" and value == "TridentNotebook":
                        # Convert the notebook ID to its name
                        item_type = "Notebook"
                        referenced_id = input_object["typeProperties"]["notebookId"]
                        referenced_name = self.convert_id_to_name(item_type=item_type, generic_id=referenced_id, lookup_type=lookup_type)
                        # Replace workspace ID with target workspace ID if the referenced notebook exists in the repository
                        if referenced_name: 
                            input_object["typeProperties"]["workspaceId"] = target_workspace_id 
                    
                    # Recursively search in the value
                    else:
                        find_and_replace_activity_workspace_ids(value)
            
            # Check if the current object is a list
            elif isinstance(input_object, list):
                # Recursively search in each item
                for item in input_object:
                    find_and_replace_activity_workspace_ids(item)

        # Start the recursive search and replace from the root of the JSON data
        find_and_replace_activity_workspace_ids(item_content_dict)

        # Convert the updated dict back to a JSON string
        raw_file = json.dumps(item_content_dict, indent=2)

        return raw_file

    def publish_all_items(self):
        """
        Publishes all items defined in item_type_in_scope list.
        """

        if "Environment" in self.item_type_in_scope:
            self.publish_environments()
        if "Notebook" in self.item_type_in_scope:
            self.publish_notebooks()
        if "DataPipeline" in self.item_type_in_scope:
            self.publish_datapipelines()

    def publish_notebooks(self):
        """
        Publishes all notebook items from the repository.
        """
        item_type = "Notebook"
        CustomPrint.header(f"Publishing {item_type}s")
        for item_name in self.repository_items.get(item_type, {}):
            self.publish_item(item_name=item_name, item_type=item_type)

    def publish_environments(self):
        """
        Publishes all environment items from the repository.

        Environments can only deploy the shell; compute and spark configurations are published separately.
        """
        item_type = "Environment"
        CustomPrint.header(f"Publishing {item_type}s")
        for item_name in self.repository_items.get(item_type, {}):
            # Only deploy the shell for environments
            self.publish_item(item_name=item_name, item_type=item_type, full_publish=False)
            self.publish_environment_compute(item_name=item_name)

    def publish_environment_compute(self, item_name):
        """
        Publishes compute settings for a given environment item.

        This process involves two steps:
        1. Updating the compute settings.
        2. Publishing the updated settings.

        :param item_name: Name of the environment item whose compute settings are to be published.
        """
        item_type = "Environment"
        item_path = self.repository_items[item_type][item_name]["path"]
        item_guid = self.repository_items[item_type][item_name]["guid"]

        # Read compute settings from YAML file
        with open(os.path.join(item_path, "Setting", "Sparkcompute.yml"), 'r+', encoding='utf-8') as f:
            yaml_body = yaml.safe_load(f)
            
            # Update instance pool settings if present
            if 'instance_pool_id' in yaml_body:
                pool_id = yaml_body['instance_pool_id']
                
                if "spark_pool" in self.environment_parameter:
                    parameter_dict = self.environment_parameter["spark_pool"]
                    if pool_id in parameter_dict:
                        # replace any found references with specified environment value
                         yaml_body['instancePool'] = parameter_dict[pool_id]
                         del yaml_body['instance_pool_id']    
    
            yaml_body = self.convert_environment_compute_to_camel(yaml_body)

        # Update compute settings
        # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-compute/update-staging-settings
        self.endpoint.invoke(method="PATCH", url=f"{self.base_api_url}/environments/{item_guid}/staging/sparkcompute", body=yaml_body)
        CustomPrint.sub_line("Updating Spark Settings")

        # Publish updated settings
        # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/publish-environment
        self.endpoint.invoke(method="POST", url=f"{self.base_api_url}/environments/{item_guid}/staging/publish")
        CustomPrint.sub_line("Published Spark Settings")

    def publish_datapipelines(self):
        """
        Publishes all data pipeline items from the repository in the correct order based on their dependencies.
        """
        item_type = "DataPipeline"
        CustomPrint.header(f"Publishing {item_type}s")

        # Get all data pipelines from the repository
        pipelines = self.repository_items.get(item_type, {})

        unsorted_pipeline_dict = {}

        # Construct unsorted_pipeline_dict with dict of pipeline
        unsorted_pipeline_dict = {}
        for item_name, item_details in pipelines.items():
            with open(os.path.join(item_details["path"], "pipeline-content.json"), 'r', encoding='utf-8') as f:
                raw_file = f.read()
            item_content_dict = json.loads(raw_file)

            unsorted_pipeline_dict[item_name] = item_content_dict

        publish_order = self.sort_datapipelines(unsorted_pipeline_dict, "Repository")

        # Publish
        for item_name in publish_order:
            self.publish_item(item_name=item_name, item_type=item_type)

    def sort_datapipelines(self, unsorted_pipeline_dict, lookup_type):
        """
        Output sorted list that datapipelines should be published or unpublished with based on item dependencies.

        :param item_content_dict: Dict representation of the pipeline-content file.
        :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository)
        """

        # Step 1: Create a graph to manage dependencies
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        unpublish_items = []

        # Step 2: Build the graph and count the in-degrees
        for item_name, item_content_dict in unsorted_pipeline_dict.items():
            # In an unpublish case, keep track of items to get unpublished
            if (lookup_type == "Deployed"): unpublish_items.append(item_name)

            referenced_pipelines = self.find_referenced_datapipelines(item_content_dict=item_content_dict, lookup_type=lookup_type)

            for referenced_name in referenced_pipelines:
                graph[referenced_name].append(item_name)
                in_degree[item_name] += 1
            # Ensure every item has an entry in the in-degree map
            if item_name not in in_degree:
                in_degree[item_name] = 0

        # In an unpublish case, adjust in_degree to include entire dependency chain for each pipeline
        if (lookup_type == "Deployed"):
            for item_name in graph:
                if item_name not in in_degree:
                    in_degree[item_name] = 0
                for neighbor in graph[item_name]:
                    if neighbor not in in_degree:
                        in_degree[neighbor] += 1

        # Step 3: Perform a topological sort to determine the correct publish order
        zero_in_degree_queue = deque([item_name for item_name in in_degree if in_degree[item_name] == 0])
        sorted_items = []

        while zero_in_degree_queue:
            item_name = zero_in_degree_queue.popleft()
            sorted_items.append(item_name)

            for neighbor in graph[item_name]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    zero_in_degree_queue.append(neighbor)

        if len(sorted_items) != len(in_degree):
            raise ValueError("There is a cycle in the graph. Cannot determine a valid publish order.")

        # Remove items not present in unpublish list and invert order for deployed sort
        if(lookup_type == "Deployed"):
            sorted_items = [item_name for item_name in sorted_items if item_name in unpublish_items]
            sorted_items = sorted_items[::-1]
        
        return sorted_items

    def find_referenced_datapipelines(self, item_content_dict, lookup_type):
        """
        Scan through item path and find pipeline references (including nested pipeline activities).

        :param item_content_dict: Dict representation of the pipeline-content file.
        :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
        :return: a list of referenced pipeline names.
        """
        item_type = "DataPipeline"
        reference_list = []  

        def find_execute_pipeline_activities(input_object):
            """
            Recursively scans through JSON to find all pipeline references.

            :param input_object: Object can be a dict or list present in the input JSON.
            """
            # Check if the current object is a dict
            if isinstance(input_object, dict):  
                for key, value in input_object.items():
                    referenced_id = None
                    
                    # Check for legacy and new pipeline activities
                    if key == "type" and value == "ExecutePipeline":
                        referenced_id = input_object["typeProperties"]["pipeline"]["referenceName"]
                    elif key == "type" and value == "InvokePipeline":
                        referenced_id = input_object["typeProperties"]["pipelineId"]
                    
                    # Add found pipeline reference to list
                    if referenced_id is not None:
                        referenced_name = self.convert_id_to_name(item_type=item_type, generic_id=referenced_id, lookup_type=lookup_type)
                        if referenced_name:
                            reference_list.append(referenced_name)

                    # Recursively search in the value
                    else:
                        find_execute_pipeline_activities(value)  
            
            # Check if the current object is a list
            elif isinstance(input_object, list):
                # Recursively search in each item  
                for item in input_object:
                    find_execute_pipeline_activities(item)  

        # Start the recursive search from the root of the JSON data
        find_execute_pipeline_activities(item_content_dict)
    
        return reference_list

    def convert_id_to_name(self, item_type, generic_id, lookup_type):
        """
        For a given item_type and id, returns the item name.  Special handling for both deployed and repository items

        :param item_type: Type of the item (e.g., Notebook, Environment).
        :param generic_id: Logical id or item guid of the item based on lookup_type.
        :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository)
        """

        lookup_dict = self.repository_items if lookup_type == "Repository" else self.deployed_items
        lookup_key = "logical_id" if lookup_type == "Repository" else "guid"

        for item_name, item_details in lookup_dict[item_type].items():
            if item_details.get(lookup_key) == generic_id:
                return item_name
        #if not found
        return None

    def unpublish_all_orphan_items(self, item_name_exclude_regex):
        """
        Unpublishes all orphaned items not present in the repository except for those matching the exclude regex.

        :param item_name_exclude_regex: Regex pattern to exclude specific items from being unpublished.
        """
        regex_pattern = re.compile(item_name_exclude_regex)

        self.refresh_deployed_items()
        CustomPrint.header("Unpublishing Orphaned Items")

        # Order of unpublishing to handle dependencies cleanly
        # TODO need to expand this to be more dynamic
        unpublish_order = [x for x in ["DataPipeline", "Notebook", "Environment"] if x in self.item_type_in_scope]

        for item_type in unpublish_order:
            deployed_names = set(self.deployed_items.get(item_type, {}).keys())
            repository_names = set(self.repository_items.get(item_type, {}).keys())

            to_delete_set = deployed_names - repository_names
            to_delete_list = [name for name in to_delete_set if not regex_pattern.match(name)]

            if item_type == "DataPipeline":
                # need to first define order of delete
                unsorted_pipeline_dict = {}

                for item_name in to_delete_list:
                    # Get deployed item definition
                    # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition
                    item_guid = self.deployed_items[item_type][item_name]["guid"]
                    response = self.endpoint.invoke(method="POST", url=f"{self.base_api_url}/items/{item_guid}/getDefinition")

                    for part in response["body"]["definition"]["parts"]:
                        if (part["path"] == "pipeline-content.json"):
                            # Decode Base64 string to dictionary
                            decoded_bytes = base64.b64decode(part["payload"])
                            decoded_string = decoded_bytes.decode('utf-8')
                            unsorted_pipeline_dict[item_name] = json.loads(decoded_string)

                # Determine order to delete w/o dependencies
                to_delete_list = self.sort_datapipelines(unsorted_pipeline_dict, "Deployed")

            for item_name in to_delete_list:
                self.unpublish_item(item_name=item_name, item_type=item_type)

    def unpublish_item(self, item_name, item_type):
        """
        Unpublishes an item from the Fabric workspace.

        :param item_name: Name of the item to unpublish.
        :param item_type: Type of the item (e.g., Notebook, Environment).
        """
        item_guid = self.deployed_items[item_type][item_name]["guid"]

        CustomPrint.timestamp(f"Unpublishing {item_type} '{item_name}'")

        # Delete the item from the workspace
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/delete-item
        self.endpoint.invoke(method="DELETE", url=f"{self.base_api_url}/items/{item_guid}")
        CustomPrint.sub_line("Unpublished")
        
    def convert_environment_compute_to_camel(self, input_dict):
        """
        Converts dictionary keys stored in snake_case to camelCase, except for 'spark_conf'.

        :param input_dict: Dictionary with snake_case keys.
        """
        new_input_dict = {}

        for key, value in input_dict.items():
            if key == 'spark_conf':
                new_key = 'sparkProperties'
            else:
                # Convert the key to camelCase
                key_components = key.split('_')
                # Capitalize the first letter of each component except the first one
                new_key = key_components[0] + ''.join(x.title() for x in key_components[1:])

            # Recursively update dictionary values if they are dictionaries
            if isinstance(value, dict):
                value = self.convert_environment_compute_to_camel(value)

            # Add the new key-value pair to the new dictionary
            new_input_dict[new_key] = value

        return new_input_dict