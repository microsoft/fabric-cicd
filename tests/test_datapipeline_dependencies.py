# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Regressions for detecting DataPipeline-to-DataPipeline dependencies."""

from unittest.mock import MagicMock

import pytest

from fabric_cicd._common._exceptions import ParsingError
from fabric_cicd._items._datapipeline import find_referenced_datapipelines
from fabric_cicd._items._manage_dependencies import sort_items
from fabric_cicd.constants import ItemType

PIPELINE_A_ID = "11111111-1111-4111-8111-111111111111"
PIPELINE_B_ID = "22222222-2222-4222-8222-222222222222"
DEPLOYED_A_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
DEPLOYED_B_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


@pytest.fixture
def workspace():
    workspace = MagicMock()
    ids = {
        "Repository": {PIPELINE_A_ID: "A", PIPELINE_B_ID: "B"},
        "Deployed": {DEPLOYED_A_ID: "A", DEPLOYED_B_ID: "B"},
    }
    workspace._convert_id_to_name.side_effect = (
        lambda item_type, generic_id, lookup_type: ids[lookup_type].get(generic_id)
        if item_type == ItemType.DATA_PIPELINE.value
        else None
    )
    return workspace


def pipeline(*activities, **properties):
    return {"properties": {"activities": list(activities), **properties}}


def execute_pipeline(pipeline_id):
    return {
        "name": "Run child",
        "type": "ExecutePipeline",
        "typeProperties": {
            "pipeline": {"type": "PipelineReference", "referenceName": pipeline_id},
            "waitOnCompletion": True,
        },
    }


def test_guid_in_pipeline_parameter_is_not_a_self_dependency(workspace):
    definition = pipeline(parameters={"auditPipelineId": {"type": "String", "defaultValue": PIPELINE_A_ID}})
    assert find_referenced_datapipelines(workspace, definition, "Repository") == []
    assert sort_items(workspace, {"A": definition}, "Repository", find_referenced_datapipelines) == ["A"]


def test_unrelated_activity_guids_do_not_create_dependencies(workspace):
    definition = pipeline(
        {
            "name": "Call audit webhook",
            "type": "WebActivity",
            "typeProperties": {
                "body": PIPELINE_B_ID,
                "pipelineId": PIPELINE_A_ID,
            },
        },
        {
            "name": "Run notebook",
            "type": "TridentNotebook",
            "typeProperties": {"notebookId": PIPELINE_B_ID},
        },
    )
    assert find_referenced_datapipelines(workspace, definition, "Repository") == []


def test_unrelated_pipeline_id_does_not_create_false_cycle(workspace):
    definitions = {
        "A": pipeline({"name": "Audit", "type": "WebActivity", "typeProperties": {"body": PIPELINE_B_ID}}),
        "B": pipeline(execute_pipeline(PIPELINE_A_ID)),
    }
    assert sort_items(workspace, definitions, "Repository", find_referenced_datapipelines) == ["A", "B"]


def test_execute_pipeline_dependency_orders_publish(workspace):
    definitions = {"A": pipeline(execute_pipeline(PIPELINE_B_ID)), "B": pipeline()}
    assert find_referenced_datapipelines(workspace, definitions["A"], "Repository") == ["B"]
    assert sort_items(workspace, definitions, "Repository", find_referenced_datapipelines) == ["B", "A"]


def test_nested_pipeline_dependencies_are_discovered_without_duplicates(workspace):
    definition = pipeline(
        {
            "name": "For each file",
            "type": "ForEach",
            "typeProperties": {
                "activities": [
                    execute_pipeline(PIPELINE_B_ID),
                    {
                        "name": "Switch routing",
                        "type": "Switch",
                        "typeProperties": {
                            "cases": [{"value": "yes", "activities": [execute_pipeline(PIPELINE_B_ID)]}]
                        },
                    },
                ]
            },
        }
    )
    assert find_referenced_datapipelines(workspace, definition, "Repository") == ["B"]


def test_nested_if_and_until_pipeline_dependencies(workspace):
    definition = pipeline(
        {
            "name": "Conditional run",
            "type": "IfCondition",
            "typeProperties": {
                "ifTrueActivities": [execute_pipeline(PIPELINE_B_ID)],
                "ifFalseActivities": [
                    {
                        "name": "Wait until",
                        "type": "Until",
                        "typeProperties": {"activities": [execute_pipeline(PIPELINE_B_ID)]},
                    }
                ],
            },
        }
    )
    assert find_referenced_datapipelines(workspace, definition, "Repository") == ["B"]


def test_invoke_pipeline_dependency_is_discovered(workspace):
    definition = pipeline(
        {
            "name": "Invoke child pipeline",
            "type": "InvokePipeline",
            "typeProperties": {"pipelineId": PIPELINE_B_ID, "workspaceId": PIPELINE_A_ID},
        }
    )
    assert find_referenced_datapipelines(workspace, definition, "Repository") == ["B"]


def test_expression_wrapped_literal_ids_preserve_publish_order(workspace):
    # Fabric can wrap a fixed GUID when "Add dynamic content" is enabled.
    expression_id = {"value": PIPELINE_B_ID, "type": "Expression"}
    definitions = {
        "A": pipeline(
            execute_pipeline(expression_id),
            {
                "name": "Invoke child pipeline",
                "type": "InvokePipeline",
                "typeProperties": {"pipelineId": expression_id},
            },
        ),
        "B": pipeline(),
    }
    assert find_referenced_datapipelines(workspace, definitions["A"], "Repository") == ["B"]
    assert sort_items(workspace, definitions, "Repository", find_referenced_datapipelines) == ["B", "A"]


def test_dynamic_expression_reference_is_ignored(workspace):
    definition = pipeline(
        execute_pipeline({"value": "@pipeline().parameters.childPipelineId", "type": "Expression"}),
        {
            "type": "InvokePipeline",
            "typeProperties": {"pipelineId": {"value": "@variables('childId')", "type": "Expression"}},
        },
    )
    assert find_referenced_datapipelines(workspace, definition, "Repository") == []


@pytest.mark.parametrize(
    "definition",
    [
        {},
        {"properties": None},
        {"properties": []},
        {"properties": {"activities": None}},
        {"properties": {"activities": "not a list"}},
    ],
)
def test_missing_or_malformed_properties_do_not_crash(workspace, definition):
    assert find_referenced_datapipelines(workspace, definition, "Repository") == []


def test_unresolved_and_expression_pipeline_references_are_ignored(workspace):
    definition = pipeline(
        execute_pipeline("@pipeline().parameters.childPipelineId"),
        execute_pipeline("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
    )
    assert find_referenced_datapipelines(workspace, definition, "Repository") == []


def test_true_self_dependency_still_raises(workspace):
    with pytest.raises(ParsingError, match="cycle in the graph"):
        sort_items(
            workspace,
            {"A": pipeline(execute_pipeline(PIPELINE_A_ID))},
            "Repository",
            find_referenced_datapipelines,
        )


def test_true_cycles_still_raise(workspace):
    definitions = {
        "A": pipeline(execute_pipeline(PIPELINE_B_ID)),
        "B": pipeline(execute_pipeline(PIPELINE_A_ID)),
    }
    with pytest.raises(ParsingError, match="cycle in the graph"):
        sort_items(workspace, definitions, "Repository", find_referenced_datapipelines)


def test_deployed_dependencies_are_resolved_for_unpublish_order(workspace):
    definitions = {"A": pipeline(execute_pipeline(DEPLOYED_B_ID)), "B": pipeline()}
    assert sort_items(workspace, definitions, "Deployed", find_referenced_datapipelines) == ["A", "B"]


def test_nested_non_activity_object_does_not_create_dependency(workspace):
    definition = pipeline(
        {
            "name": "Send audit payload",
            "type": "WebActivity",
            "typeProperties": {
                "body": {
                    "type": "ExecutePipeline",
                    "typeProperties": {"pipeline": {"referenceName": PIPELINE_A_ID}},
                }
            },
        }
    )
    assert find_referenced_datapipelines(workspace, definition, "Repository") == []


def test_switch_default_activities_retain_real_dependencies(workspace):
    definition = pipeline(
        {
            "name": "Switch routes",
            "type": "Switch",
            "typeProperties": {
                "cases": [{"value": "special", "activities": [execute_pipeline(PIPELINE_B_ID)]}],
                "defaultActivities": [execute_pipeline(PIPELINE_A_ID)],
            },
        }
    )
    assert set(find_referenced_datapipelines(workspace, definition, "Repository")) == {"A", "B"}
