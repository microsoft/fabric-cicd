# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for item capabilities matrix generation."""

from fabric_cicd.constants import ACCEPTED_ITEM_TYPES_UPN
from fabric_cicd.item_capabilities import (
    CAPABILITY_DESCRIPTIONS,
    ITEM_CAPABILITIES,
    generate_capabilities_matrix,
)


def test_capability_descriptions_not_empty():
    """Test that capability descriptions are defined."""
    assert CAPABILITY_DESCRIPTIONS
    assert all(desc for desc in CAPABILITY_DESCRIPTIONS.values())


def test_item_capabilities_covers_all_accepted_types():
    """Test that all accepted item types have capability definitions."""
    # All item types in constants should have capability data
    missing_items = set(ACCEPTED_ITEM_TYPES_UPN) - set(ITEM_CAPABILITIES.keys())
    assert not missing_items, f"Missing capability data for: {missing_items}"


def test_item_capabilities_structure():
    """Test that item capabilities have the expected structure."""
    expected_keys = set(CAPABILITY_DESCRIPTIONS.keys())

    for item_type, capabilities in ITEM_CAPABILITIES.items():
        assert isinstance(capabilities, dict), f"Capabilities for {item_type} should be a dict"

        # All capability keys should be present
        missing_keys = expected_keys - set(capabilities.keys())
        assert not missing_keys, f"Missing capability keys for {item_type}: {missing_keys}"

        # All values should be boolean
        for key, value in capabilities.items():
            assert isinstance(value, bool), f"Capability {key} for {item_type} should be boolean, got {type(value)}"


def test_generate_capabilities_matrix():
    """Test that the matrix generation produces valid output."""
    matrix = generate_capabilities_matrix()

    # Should not be empty
    assert matrix
    assert isinstance(matrix, str)

    # Should contain table structure
    assert "| Item Type |" in matrix
    assert "| --- |" in matrix

    # Should contain legend and notes
    assert "Legend" in matrix
    assert "Notes" in matrix

    # Should contain checkmarks and X marks
    assert "✓" in matrix
    assert "✗" in matrix

    # Should contain all capability descriptions
    for desc in CAPABILITY_DESCRIPTIONS.values():
        assert desc in matrix


def test_generate_capabilities_matrix_includes_all_items():
    """Test that all accepted item types are included in the matrix."""
    matrix = generate_capabilities_matrix()

    # All item types should appear in the matrix
    for item_type in ACCEPTED_ITEM_TYPES_UPN:
        assert f"| {item_type} |" in matrix


def test_matrix_table_structure():
    """Test that the generated matrix has correct table structure."""
    matrix = generate_capabilities_matrix()
    lines = matrix.split("\n")

    # Find table start and end
    table_start = None
    table_end = None

    for i, line in enumerate(lines):
        if line.startswith("| Item Type |"):
            table_start = i
        elif table_start is not None and line.startswith(">"):
            table_end = i
            break

    assert table_start is not None, "Table header not found"
    assert table_end is not None, "Table end not found"

    # Check header row
    header_line = lines[table_start]
    expected_columns = len(CAPABILITY_DESCRIPTIONS) + 1  # +1 for Item Type column
    actual_columns = header_line.count("|") - 1  # -1 because of leading |
    assert actual_columns == expected_columns, f"Expected {expected_columns} columns, got {actual_columns}"

    # Check separator row
    separator_line = lines[table_start + 1]
    assert separator_line.startswith("|")
    assert separator_line.endswith("|")
    assert "---" in separator_line


def test_source_control_always_true():
    """Test that all item types have source control support (as per documentation)."""
    for item_type, capabilities in ITEM_CAPABILITIES.items():
        assert capabilities["source_control"] is True, f"{item_type} should have source control support"


def test_specific_item_capabilities():
    """Test specific known capabilities for certain item types."""
    # Dataflow should support ordered deployment (only one that does)
    assert ITEM_CAPABILITIES["Dataflow"]["ordered_deployment"] is True

    # Most items should not support ordered deployment
    items_without_ordered = [item for item in ITEM_CAPABILITIES if not ITEM_CAPABILITIES[item]["ordered_deployment"]]
    assert len(items_without_ordered) > 15  # Most items don't support this

    # Shell-only items should not have content deployment
    shell_only_items = ["Lakehouse", "Warehouse", "SQLDatabase", "Environment"]
    for item in shell_only_items:
        if item in ITEM_CAPABILITIES:
            assert ITEM_CAPABILITIES[item]["content_deployment"] is False, f"{item} should not have content deployment"
