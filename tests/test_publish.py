# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for publish.py module."""

import logging
import re
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd import constants
from fabric_cicd._common._item import Item
from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import publish_all_items, unpublish_all_orphan_items


@pytest.fixture
def mock_fabric_workspace():
    """Create a mock FabricWorkspace object."""
    mock_workspace = MagicMock(spec=FabricWorkspace)
    mock_workspace.item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]
    mock_workspace.deployed_items = {}
    mock_workspace.repository_items = {}
    mock_workspace.endpoint = MagicMock()
    mock_workspace.base_api_url = "https://api.powerbi.com/v1.0/myorg/workspaces/test-workspace-id"
    return mock_workspace


@pytest.fixture
def mock_validate_fabric_workspace_obj():
    """Mock the validate_fabric_workspace_obj function."""
    with patch("fabric_cicd.publish.validate_fabric_workspace_obj") as mock_validate:
        mock_validate.side_effect = lambda obj: obj
        yield mock_validate


@pytest.fixture
def mock_items_module():
    """Mock all the items module functions."""
    with patch("fabric_cicd.publish.items") as mock_items:
        yield mock_items


@pytest.fixture
def mock_print_header():
    """Mock the print_header function."""
    with patch("fabric_cicd.publish.print_header") as mock_header:
        yield mock_header


@pytest.fixture
def mock_check_regex():
    """Mock the check_regex function."""
    with patch("fabric_cicd.publish.check_regex") as mock_regex:
        mock_regex.return_value = re.compile("test_pattern")
        yield mock_regex


class TestPublishAllItems:
    """Test class for publish_all_items function."""
    
    def test_publish_all_items_basic_functionality(
        self, 
        mock_fabric_workspace, 
        mock_validate_fabric_workspace_obj,
        mock_items_module,
        mock_print_header
    ):
        """Test basic functionality of publish_all_items."""
        # Setup
        mock_fabric_workspace.item_type_in_scope = ["Notebook", "Environment"]
        
        # Execute
        publish_all_items(mock_fabric_workspace)
        
        # Verify validation was called
        mock_validate_fabric_workspace_obj.assert_called_once_with(mock_fabric_workspace)
        
        # Verify folder operations were called (when feature flag is not set)
        mock_fabric_workspace._refresh_deployed_folders.assert_called_once()
        mock_fabric_workspace._refresh_repository_folders.assert_called_once()
        mock_fabric_workspace._publish_folders.assert_called_once()
        
        # Verify item operations were called
        mock_fabric_workspace._refresh_deployed_items.assert_called_once()
        mock_fabric_workspace._refresh_repository_items.assert_called_once()
        
        # Verify appropriate publish functions were called
        mock_print_header.assert_any_call("Publishing Notebooks")
        mock_items_module.publish_notebooks.assert_called_once_with(mock_fabric_workspace)
        
        mock_print_header.assert_any_call("Publishing Environments")
        mock_items_module.publish_environments.assert_called_once_with(mock_fabric_workspace)
        
        # Verify environment publish state checking
        mock_print_header.assert_any_call("Checking Environment Publish State")
        mock_items_module.check_environment_publish_state.assert_called_once_with(mock_fabric_workspace)

    def test_publish_all_items_with_folder_feature_flag(
        self, 
        mock_fabric_workspace
    ):
        """Test publish_all_items when folder publish is disabled."""
        # Setup - add feature flag to disable folder publish
        original_feature_flag = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add("disable_workspace_folder_publish")
        
        try:
            # Execute
            publish_all_items(mock_fabric_workspace)
            
            # Verify folder operations were NOT called
            mock_fabric_workspace._refresh_deployed_folders.assert_not_called()
            mock_fabric_workspace._refresh_repository_folders.assert_not_called()
            mock_fabric_workspace._publish_folders.assert_not_called()
            
            # Verify item operations were still called
            # Note: _refresh_deployed_items is called twice because DataPipeline is in scope 
            # which calls it again for dependency management
            assert mock_fabric_workspace._refresh_deployed_items.call_count == 2
            mock_fabric_workspace._refresh_repository_items.assert_called_once()
        finally:
            # Cleanup
            constants.FEATURE_FLAG = original_feature_flag

    def test_publish_all_items_with_exclude_regex(
        self, 
        mock_fabric_workspace, 
        caplog
    ):
        """Test publish_all_items with item name exclude regex."""
        # Setup
        exclude_regex = ".*_test"
        
        # Execute
        with caplog.at_level(logging.WARNING):
            publish_all_items(mock_fabric_workspace, exclude_regex)
        
        # Verify warning was logged
        assert "Using item_name_exclude_regex is risky" in caplog.text
        
        # Verify regex was set on workspace
        assert mock_fabric_workspace.publish_item_name_exclude_regex == exclude_regex

    @pytest.mark.parametrize(("item_types", "expected_calls"), [
        (["VariableLibrary"], [("Publishing Variable Libraries", "publish_variablelibraries")]),
        (["Warehouse"], [("Publishing Warehouses", "publish_warehouses")]),
        (["Lakehouse"], [("Publishing Lakehouses", "publish_lakehouses")]),
        (["SQLDatabase"], [("Publishing SQL Databases", "publish_sqldatabases")]),
        (["MirroredDatabase"], [("Publishing Mirrored Databases", "publish_mirroreddatabase")]),
        (["Notebook"], [("Publishing Notebooks", "publish_notebooks")]),
        (["SemanticModel"], [("Publishing Semantic Models", "publish_semanticmodels")]),
        (["Report"], [("Publishing Reports", "publish_reports")]),
        (["CopyJob"], [("Publishing Copy Jobs", "publish_copyjobs")]),
        (["Eventhouse"], [("Publishing Eventhouses", "publish_eventhouses")]),
        (["KQLDatabase"], [("Publishing KQL Databases", "publish_kqldatabases")]),
        (["KQLQueryset"], [("Publishing KQL Querysets", "publish_kqlquerysets")]),
        (["Reflex"], [("Publishing Activators", "publish_activators")]),
        (["Eventstream"], [("Publishing Eventstreams", "publish_eventstreams")]),
        (["KQLDashboard"], [("Publishing KQL Dashboards", "publish_kqldashboard")]),
        (["Dataflow"], [("Publishing Dataflows", "publish_dataflows")]),
        (["DataPipeline"], [("Publishing Data Pipelines", "publish_datapipelines")]),
        (["Notebook", "DataPipeline"], [
            ("Publishing Notebooks", "publish_notebooks"),
            ("Publishing Data Pipelines", "publish_datapipelines")
        ]),
    ])
    def test_publish_all_items_different_item_types(
        self,
        item_types,
        expected_calls,
        mock_fabric_workspace,
        mock_items_module,
        mock_print_header
    ):
        """Test publish_all_items with different item types."""
        # Setup
        mock_fabric_workspace.item_type_in_scope = item_types
        
        # Execute
        publish_all_items(mock_fabric_workspace)
        
        # Verify expected calls were made
        for header_text, function_name in expected_calls:
            mock_print_header.assert_any_call(header_text)
            getattr(mock_items_module, function_name).assert_called_once_with(mock_fabric_workspace)

    def test_publish_all_items_environment_special_handling(
        self,
        mock_fabric_workspace,
        mock_items_module,
        mock_print_header
    ):
        """Test that Environment items get special publish state checking."""
        # Setup
        mock_fabric_workspace.item_type_in_scope = ["Environment", "Notebook"]
        
        # Execute
        publish_all_items(mock_fabric_workspace)
        
        # Verify Environment publish and check were both called
        mock_print_header.assert_any_call("Publishing Environments")
        mock_items_module.publish_environments.assert_called_once_with(mock_fabric_workspace)
        
        mock_print_header.assert_any_call("Checking Environment Publish State")
        mock_items_module.check_environment_publish_state.assert_called_once_with(mock_fabric_workspace)

    def test_publish_all_items_no_matching_item_types(
        self,
        mock_fabric_workspace,
        mock_validate_fabric_workspace_obj,
        mock_items_module
    ):
        """Test publish_all_items when no item types match."""
        # Setup
        mock_fabric_workspace.item_type_in_scope = ["NonExistentType"]
        
        # Execute
        publish_all_items(mock_fabric_workspace)
        
        # Verify basic operations were still called
        mock_validate_fabric_workspace_obj.assert_called_once()
        mock_fabric_workspace._refresh_deployed_items.assert_called_once()
        mock_fabric_workspace._refresh_repository_items.assert_called_once()
        
        # Verify no publish functions were called
        for attr_name in dir(mock_items_module):
            if attr_name.startswith('publish_') and not attr_name.endswith('_'):
                attr = getattr(mock_items_module, attr_name)
                if hasattr(attr, 'assert_not_called'):
                    attr.assert_not_called()

    def test_publish_all_items_graphql_api_with_warning(
        self,
        mock_fabric_workspace,
        mock_items_module,
        mock_print_header,
        caplog
    ):
        """Test publish_all_items with GraphQLApi item type and warning message."""
        # Setup
        mock_fabric_workspace.item_type_in_scope = ["GraphQLApi"]
        
        # Execute
        with caplog.at_level(logging.WARNING):
            publish_all_items(mock_fabric_workspace)
        
        # Verify header was printed
        mock_print_header.assert_any_call("Publishing GraphQL APIs")
        
        # Verify warning message was logged
        assert "Only user authentication is supported for GraphQL API items sourced from SQL Analytics Endpoint" in caplog.text
        
        # Verify publish function was called
        mock_items_module.publish_graphqlapis.assert_called_once_with(mock_fabric_workspace)


class TestUnpublishAllOrphanItems:
    """Test class for unpublish_all_orphan_items function."""
    
    def test_unpublish_all_orphan_items_basic_functionality(
        self,
        mock_fabric_workspace,
        mock_validate_fabric_workspace_obj,
        mock_check_regex,
        mock_print_header
    ):
        """Test basic functionality of unpublish_all_orphan_items."""
        # Setup
        mock_fabric_workspace.item_type_in_scope = ["Notebook", "DataPipeline"]
        
        # Create proper Item objects with guid attribute
        deployed_notebook = Item(type="Notebook", name="deployed_notebook", description="", guid="guid-1")
        deployed_pipeline = Item(type="DataPipeline", name="deployed_pipeline", description="", guid="guid-2")
        repo_notebook = Item(type="Notebook", name="repo_notebook", description="", guid="guid-3")
        repo_pipeline = Item(type="DataPipeline", name="repo_pipeline", description="", guid="guid-4")
        
        mock_fabric_workspace.deployed_items = {
            "Notebook": {"deployed_notebook": deployed_notebook},
            "DataPipeline": {"deployed_pipeline": deployed_pipeline}
        }
        mock_fabric_workspace.repository_items = {
            "Notebook": {"repo_notebook": repo_notebook},
            "DataPipeline": {"repo_pipeline": repo_pipeline}
        }
        
        # Mock the endpoint response for dependency management
        mock_fabric_workspace.endpoint.invoke.return_value = {
            "body": {
                "definition": {
                    "parts": [{
                        "path": "pipeline-content.json",
                        "payload": "e30="  # base64 encoded empty JSON object {}
                    }]
                }
            }
        }
        
        # Execute
        unpublish_all_orphan_items(mock_fabric_workspace, "test_regex")
        
        # Verify validation and regex check were called
        mock_validate_fabric_workspace_obj.assert_called_once_with(mock_fabric_workspace)
        mock_check_regex.assert_called_once_with("test_regex")
        
        # Verify refresh operations were called
        mock_fabric_workspace._refresh_deployed_items.assert_called()
        mock_fabric_workspace._refresh_repository_items.assert_called_once()
        
        # Verify header was printed
        mock_print_header.assert_any_call("Unpublishing Orphaned Items")
        
        # Verify unpublish operations were called for orphaned items
        mock_fabric_workspace._unpublish_item.assert_any_call(
            item_name="deployed_notebook", item_type="Notebook"
        )
        mock_fabric_workspace._unpublish_item.assert_any_call(
            item_name="deployed_pipeline", item_type="DataPipeline"
        )

    def test_unpublish_all_orphan_items_default_regex(
        self,
        mock_fabric_workspace,
        mock_check_regex
    ):
        """Test unpublish_all_orphan_items with default regex."""
        # Setup
        mock_fabric_workspace.deployed_items = {}
        mock_fabric_workspace.repository_items = {}
        
        # Execute
        unpublish_all_orphan_items(mock_fabric_workspace)
        
        # Verify default regex was used
        mock_check_regex.assert_called_once_with("^$")

    def test_unpublish_all_orphan_items_with_feature_flags(
        self,
        mock_fabric_workspace
    ):
        """Test unpublish_all_orphan_items with feature flags for restricted items."""
        # Setup
        original_feature_flag = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add("enable_lakehouse_unpublish")
        constants.FEATURE_FLAG.add("enable_warehouse_unpublish")
        
        mock_fabric_workspace.item_type_in_scope = ["Lakehouse", "Warehouse", "SQLDatabase", "Notebook"]
        
        # Create proper Item objects
        orphan_lakehouse = Item(type="Lakehouse", name="orphan_lakehouse", description="", guid="guid-1")
        orphan_warehouse = Item(type="Warehouse", name="orphan_warehouse", description="", guid="guid-2")
        orphan_sql = Item(type="SQLDatabase", name="orphan_sql", description="", guid="guid-3")
        orphan_notebook = Item(type="Notebook", name="orphan_notebook", description="", guid="guid-4")
        
        mock_fabric_workspace.deployed_items = {
            "Lakehouse": {"orphan_lakehouse": orphan_lakehouse},
            "Warehouse": {"orphan_warehouse": orphan_warehouse},
            "SQLDatabase": {"orphan_sql": orphan_sql},  # No feature flag set
            "Notebook": {"orphan_notebook": orphan_notebook}
        }
        mock_fabric_workspace.repository_items = {
            "Lakehouse": {},
            "Warehouse": {},
            "SQLDatabase": {},
            "Notebook": {}
        }
        
        try:
            # Execute
            unpublish_all_orphan_items(mock_fabric_workspace)
            
            # Verify items with enabled feature flags were unpublished
            mock_fabric_workspace._unpublish_item.assert_any_call(
                item_name="orphan_lakehouse", item_type="Lakehouse"
            )
            mock_fabric_workspace._unpublish_item.assert_any_call(
                item_name="orphan_warehouse", item_type="Warehouse"
            )
            mock_fabric_workspace._unpublish_item.assert_any_call(
                item_name="orphan_notebook", item_type="Notebook"
            )
            
            # Verify SQLDatabase was NOT unpublished (no feature flag)
            call_args_list = [call.kwargs for call in mock_fabric_workspace._unpublish_item.call_args_list]
            sql_calls = [call for call in call_args_list if call.get('item_type') == 'SQLDatabase']
            assert len(sql_calls) == 0
            
        finally:
            # Cleanup
            constants.FEATURE_FLAG = original_feature_flag

    def test_unpublish_all_orphan_items_regex_exclusion(
        self,
        mock_fabric_workspace,
        mock_check_regex
    ):
        """Test unpublish_all_orphan_items with regex exclusion."""
        # Setup
        exclude_pattern = re.compile(r".*_keep$")
        mock_check_regex.return_value = exclude_pattern
        
        mock_fabric_workspace.item_type_in_scope = ["Notebook"]
        
        # Create proper Item objects
        orphan_delete = Item(type="Notebook", name="orphan_delete", description="", guid="guid-1")
        orphan_keep = Item(type="Notebook", name="orphan_keep", description="", guid="guid-2")
        
        mock_fabric_workspace.deployed_items = {
            "Notebook": {
                "orphan_delete": orphan_delete,
                "orphan_keep": orphan_keep  # Should be excluded by regex
            }
        }
        mock_fabric_workspace.repository_items = {"Notebook": {}}
        
        # Execute
        unpublish_all_orphan_items(mock_fabric_workspace, ".*_keep$")
        
        # Verify only non-matching item was unpublished
        mock_fabric_workspace._unpublish_item.assert_called_once_with(
            item_name="orphan_delete", item_type="Notebook"
        )

    @pytest.mark.parametrize(("item_type", "has_dependencies"), [
        ("DataPipeline", True),
        ("Dataflow", True),
        ("Notebook", False),
        ("Environment", False)
    ])
    def test_unpublish_all_orphan_items_dependency_handling(
        self,
        item_type,
        has_dependencies,
        mock_fabric_workspace,
        mock_items_module
    ):
        """Test unpublish_all_orphan_items handles dependencies correctly."""
        # Setup
        mock_fabric_workspace.item_type_in_scope = [item_type]
        
        # Create proper Item object
        orphan_item = Item(type=item_type, name="orphan_item", description="", guid="guid-1")
        
        mock_fabric_workspace.deployed_items = {item_type: {"orphan_item": orphan_item}}
        mock_fabric_workspace.repository_items = {item_type: {}}
        
        if has_dependencies:
            # Mock the dependency handling functions
            mock_items_module.set_unpublish_order.return_value = ["orphan_item"]
            if item_type == "DataPipeline":
                mock_items_module.find_referenced_datapipelines = MagicMock()
            elif item_type == "Dataflow":
                mock_items_module.find_referenced_dataflows = MagicMock()
        
        with patch("fabric_cicd.publish.items", mock_items_module):
            # Execute
            unpublish_all_orphan_items(mock_fabric_workspace)
            
            # Verify dependency handling was called for items that have dependencies
            if has_dependencies:
                mock_items_module.set_unpublish_order.assert_called_once()
            
            # Verify item was unpublished
            mock_fabric_workspace._unpublish_item.assert_called_once_with(
                item_name="orphan_item", item_type=item_type
            )

    def test_unpublish_all_orphan_items_folder_cleanup(
        self,
        mock_fabric_workspace
    ):
        """Test unpublish_all_orphan_items cleans up folders."""
        # Setup
        original_feature_flag = constants.FEATURE_FLAG.copy()
        # Ensure folder publish is not disabled
        constants.FEATURE_FLAG.discard("disable_workspace_folder_publish")
        
        mock_fabric_workspace.deployed_items = {}
        mock_fabric_workspace.repository_items = {}
        
        try:
            # Execute
            unpublish_all_orphan_items(mock_fabric_workspace)
            
            # Verify folder cleanup operations were called
            assert mock_fabric_workspace._refresh_deployed_items.call_count == 2  # Once at start, once at end
            mock_fabric_workspace._refresh_deployed_folders.assert_called_once()
            mock_fabric_workspace._unpublish_folders.assert_called_once()
            
        finally:
            # Cleanup
            constants.FEATURE_FLAG = original_feature_flag

    def test_unpublish_all_orphan_items_folder_cleanup_disabled(
        self,
        mock_fabric_workspace
    ):
        """Test unpublish_all_orphan_items when folder cleanup is disabled."""
        # Setup
        original_feature_flag = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add("disable_workspace_folder_publish")
        
        mock_fabric_workspace.deployed_items = {}
        mock_fabric_workspace.repository_items = {}
        
        try:
            # Execute
            unpublish_all_orphan_items(mock_fabric_workspace)
            
            # Verify folder cleanup operations were NOT called
            mock_fabric_workspace._unpublish_folders.assert_not_called()
            
        finally:
            # Cleanup
            constants.FEATURE_FLAG = original_feature_flag

    def test_unpublish_order_is_correct(
        self,
        mock_fabric_workspace
    ):
        """Test that unpublish order follows the expected sequence."""
        # Setup all item types
        all_item_types = [
            "DataPipeline", "Dataflow", "Eventstream", "Reflex", "KQLDashboard", 
            "KQLQueryset", "KQLDatabase", "Eventhouse", "CopyJob", "Report", 
            "SemanticModel", "Notebook", "Environment", "MirroredDatabase", 
            "SQLDatabase", "Lakehouse", "Warehouse", "VariableLibrary"
        ]
        
        mock_fabric_workspace.item_type_in_scope = all_item_types
        
        # Create orphaned items for each type
        deployed_items = {}
        repository_items = {}
        for i, item_type in enumerate(all_item_types):
            orphan_item = Item(type=item_type, name=f"orphan_{item_type.lower()}", description="", guid=f"guid-{i}")
            deployed_items[item_type] = {f"orphan_{item_type.lower()}": orphan_item}
            repository_items[item_type] = {}
        
        mock_fabric_workspace.deployed_items = deployed_items
        mock_fabric_workspace.repository_items = repository_items
        
        # Enable all feature flags
        original_feature_flag = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.update([
            "enable_lakehouse_unpublish",
            "enable_sqldatabase_unpublish", 
            "enable_warehouse_unpublish"
        ])
        
        try:
            # Mock the endpoint response for dependency management
            def mock_invoke_response(method, url, **kwargs):  # noqa: ARG001
                if "getDefinition" in url:
                    # Determine the item type based on GUID
                    if "guid-0" in url:  # DataPipeline
                        payload = "e30="  # base64 encoded empty JSON object {}
                        path = "pipeline-content.json"
                    elif "guid-1" in url:  # Dataflow
                        payload = "bGV0DQogIHNvdXJjZSA9ICMidGFibGUi"  # base64 encoded sample Dataflow content
                        path = "mashup.pq"
                    else:
                        # Default to pipeline for other items that might need dependency checking
                        payload = "e30="
                        path = "pipeline-content.json"
                    
                    return {
                        "body": {
                            "definition": {
                                "parts": [{
                                    "path": path,
                                    "payload": payload
                                }]
                            }
                        }
                    }
                return {"body": {}, "header": {}}
            
            mock_fabric_workspace.endpoint.invoke.side_effect = mock_invoke_response
            
            # Execute
            unpublish_all_orphan_items(mock_fabric_workspace)
            
            # Verify items were unpublished
            assert mock_fabric_workspace._unpublish_item.call_count == len(all_item_types)
            
            # Get the order of unpublish calls
            call_args = [call[1]['item_type'] for call in mock_fabric_workspace._unpublish_item.call_args_list]
            
            # Define expected order (from the function)
            expected_order = [
                "DataPipeline", "Dataflow", "Eventstream", "Reflex", "KQLDashboard",
                "KQLQueryset", "KQLDatabase", "Eventhouse", "CopyJob", "Report",
                "SemanticModel", "Notebook", "Environment", "MirroredDatabase",
                "SQLDatabase", "Lakehouse", "Warehouse", "VariableLibrary"
            ]
            
            # Verify the order matches
            assert call_args == expected_order
            
        finally:
            # Cleanup
            constants.FEATURE_FLAG = original_feature_flag