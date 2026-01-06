# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for Report rebinding functionality with cross-workspace scenarios."""

import json

from fabric_cicd._items._report import sync_report_dataset_reference


class TestReportRebinding:
    """Test suite for Report rebinding with cross-workspace dataset references."""

    def test_sync_report_dataset_reference_with_semanticmodelid(self):
        """Test that pbiModelDatabaseName is synced when connectionString contains semanticmodelid."""
        # Arrange
        old_model_id = "11111111-1111-1111-1111-111111111111"
        new_model_id = "22222222-2222-2222-2222-222222222222"

        report_content = json.dumps(
            {
                "version": "4.0",
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/1.0.0/schema.json",
                "datasetReference": {
                    "byConnection": {
                        "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/dev__GlobalDataHouse__WS; initial catalog=Metric_Measure_Catalogue_GDH;access mode=readonly; integrated security=ClaimsToken;semanticmodelid={new_model_id}",
                        "pbiServiceModelId": None,
                        "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                        "pbiModelDatabaseName": old_model_id,  # This should be updated to match new_model_id
                        "name": "EntityDataSource",
                        "connectionType": "pbiServiceXmlaStyleLive",
                    }
                },
            },
            indent=4,
        )

        # Act
        result = sync_report_dataset_reference(report_content)
        result_json = json.loads(result)

        # Assert
        assert result_json["datasetReference"]["byConnection"]["pbiModelDatabaseName"] == new_model_id
        assert new_model_id in result_json["datasetReference"]["byConnection"]["connectionString"]

    def test_sync_report_dataset_reference_no_change_when_already_synced(self):
        """Test that content is not modified when pbiModelDatabaseName already matches semanticmodelid."""
        # Arrange
        model_id = "22222222-2222-2222-2222-222222222222"

        report_content = json.dumps(
            {
                "version": "4.0",
                "datasetReference": {
                    "byConnection": {
                        "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/dev__WS;semanticmodelid={model_id}",
                        "pbiServiceModelId": None,
                        "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                        "pbiModelDatabaseName": model_id,  # Already matches
                        "name": "EntityDataSource",
                        "connectionType": "pbiServiceXmlaStyleLive",
                    }
                },
            },
            indent=4,
        )

        # Act
        result = sync_report_dataset_reference(report_content)
        result_json = json.loads(result)

        # Assert - Should remain the same
        assert result_json["datasetReference"]["byConnection"]["pbiModelDatabaseName"] == model_id

    def test_sync_report_dataset_reference_no_connectionstring(self):
        """Test that content is unchanged when connectionString is None or missing."""
        # Arrange
        report_content = json.dumps(
            {
                "version": "4.0",
                "datasetReference": {
                    "byConnection": {
                        "connectionString": None,
                        "pbiServiceModelId": None,
                        "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                        "pbiModelDatabaseName": "11111111-1111-1111-1111-111111111111",
                        "name": "EntityDataSource",
                        "connectionType": "pbiServiceXmlaStyleLive",
                    }
                },
            },
            indent=4,
        )

        # Act
        result = sync_report_dataset_reference(report_content)

        # Assert - Should remain unchanged
        assert result == report_content

    def test_sync_report_dataset_reference_no_semanticmodelid_in_connectionstring(self):
        """Test that content is unchanged when connectionString doesn't contain semanticmodelid."""
        # Arrange
        report_content = json.dumps(
            {
                "version": "4.0",
                "datasetReference": {
                    "byConnection": {
                        "connectionString": "Data Source=powerbi://api.powerbi.com/v1.0/myorg/some__WS",
                        "pbiServiceModelId": None,
                        "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                        "pbiModelDatabaseName": "11111111-1111-1111-1111-111111111111",
                        "name": "EntityDataSource",
                        "connectionType": "pbiServiceXmlaStyleLive",
                    }
                },
            },
            indent=4,
        )

        # Act
        result = sync_report_dataset_reference(report_content)

        # Assert - Should remain unchanged
        assert result == report_content

    def test_sync_report_dataset_reference_bypath_format(self):
        """Test that byPath format is not affected by sync function."""
        # Arrange
        report_content = json.dumps(
            {"version": "4.0", "datasetReference": {"byPath": {"path": "../ABC.SemanticModel"}}}, indent=4
        )

        # Act
        result = sync_report_dataset_reference(report_content)

        # Assert - Should remain unchanged
        assert result == report_content

    def test_sync_report_dataset_reference_invalid_json(self):
        """Test that invalid JSON is handled gracefully."""
        # Arrange
        invalid_json = "{ invalid json content"

        # Act
        result = sync_report_dataset_reference(invalid_json)

        # Assert - Should return unchanged
        assert result == invalid_json

    def test_sync_report_dataset_reference_case_insensitive_semanticmodelid(self):
        """Test that semanticmodelid matching is case-insensitive."""
        # Arrange
        old_model_id = "11111111-1111-1111-1111-111111111111"
        new_model_id = "22222222-2222-2222-2222-222222222222"

        # Using uppercase SEMANTICMODELID
        report_content = json.dumps(
            {
                "version": "4.0",
                "datasetReference": {
                    "byConnection": {
                        "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/dev__WS;SEMANTICMODELID={new_model_id}",
                        "pbiServiceModelId": None,
                        "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                        "pbiModelDatabaseName": old_model_id,
                        "name": "EntityDataSource",
                        "connectionType": "pbiServiceXmlaStyleLive",
                    }
                },
            },
            indent=4,
        )

        # Act
        result = sync_report_dataset_reference(report_content)
        result_json = json.loads(result)

        # Assert
        assert result_json["datasetReference"]["byConnection"]["pbiModelDatabaseName"] == new_model_id

    def test_sync_report_dataset_reference_with_spaces_in_connectionstring(self):
        """Test that semanticmodelid is extracted correctly even with spaces around equals sign."""
        # Arrange
        old_model_id = "11111111-1111-1111-1111-111111111111"
        new_model_id = "22222222-2222-2222-2222-222222222222"

        # Connection string with spaces: "semanticmodelid = <guid>"
        report_content = json.dumps(
            {
                "version": "4.0",
                "datasetReference": {
                    "byConnection": {
                        "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/dev__WS;semanticmodelid = {new_model_id}",
                        "pbiServiceModelId": None,
                        "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                        "pbiModelDatabaseName": old_model_id,
                        "name": "EntityDataSource",
                        "connectionType": "pbiServiceXmlaStyleLive",
                    }
                },
            },
            indent=4,
        )

        # Act
        result = sync_report_dataset_reference(report_content)
        result_json = json.loads(result)

        # Assert
        assert result_json["datasetReference"]["byConnection"]["pbiModelDatabaseName"] == new_model_id
