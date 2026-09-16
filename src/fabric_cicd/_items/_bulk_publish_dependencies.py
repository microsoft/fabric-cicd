# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Dependency and batch-planning helpers for bulk item publishing."""

import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from fabric_cicd import constants
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._parameter._utils import (
    ParsedDynamicVariable,
    parse_dynamic_variable,
    process_environment_key,
    process_input_path,
)
from fabric_cicd.constants import ASYNC_PROVISIONED_ATTRIBUTES

if TYPE_CHECKING:
    from fabric_cicd._common._item import Item
    from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)

# Bulk publish compatibility detection


def has_unfiltered_items_variable(workspace_obj: "FabricWorkspace") -> bool:
    """Return whether an $items.* variable is unfiltered and requires serial deployment fallback."""
    for param_dict, parsed in _iter_dynamic_replace_values(workspace_obj):
        if parsed is None:
            continue
        if not any(param_dict.get(f) for f in constants.PARAMETER_FILE_FILTERS):
            logger.debug(
                "Found unfiltered item variable referencing '%s.%s', bulk publishing is not supported",
                parsed.item_type,
                parsed.item_name,
            )
            return True

    return False


# Dependency-aware batch computation


@dataclass
class _PublishUnits:
    """Store publish-units in both lookup directions."""

    items_by_representative: dict[str, list[str]]  # {representative_item_key: [item_key, ...]}
    representative_by_item: dict[str, str]  # {item_key: representative_item_key}


def build_dynamic_variable_dependency_graph(
    workspace_obj: "FabricWorkspace",
    publish_item_keys: set[str],
) -> list[tuple[str, str]]:
    """
    Build dependency edges between referencing and referenced items based on current-workspace $items.* variables.

    Only adds an edge (referencing_item_key -> referenced_item_key) for each replace_value that references an item
    which is NOT already deployed but IS in the current publish set.

    Args:
        workspace_obj: The FabricWorkspace object.
        publish_item_keys: A set of item keys (in "ItemType.ItemName" format) in this publish operation.

    Returns:
        A list of directed dependency edges. An empty list means all dynamic variables are resolvable
        upfront and a single bulk call is sufficient.
    """
    edges: list[tuple[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()

    repository_items = workspace_obj.repository_items
    repository_directory = workspace_obj.repository_directory
    path_cache: dict[str, Path] = {}
    referencing_cache: dict[tuple, list[str]] = {}

    for param_dict, parsed in _iter_dynamic_replace_values(workspace_obj):
        if parsed is None:
            continue

        # Extract the referenced item type and name from the parsed dynamic replacement variable
        referenced_type, referenced_name = parsed.item_type, parsed.item_name
        referenced_key = f"{referenced_type}.{referenced_name}"
        logger.debug(
            "Found parameter rule referencing '%s' attribute '%s'",
            referenced_key,
            parsed.attribute,
        )

        # Existing items are already available, so they create no publish-order dependency
        if referenced_name in workspace_obj.deployed_items.get(referenced_type, {}):
            logger.debug("Skipping dependency for already deployed item '%s'", referenced_key)
            continue

        # Resolve each unique filter combination once
        # Example cache key: ("ItemType", "ItemName", "FilePath")
        cache_key = (
            _hashable_filter(param_dict.get("item_type")),
            _hashable_filter(param_dict.get("item_name")),
            _hashable_filter(param_dict.get("file_path")),
        )
        # Compute matches only when this filter is not cached
        if cache_key not in referencing_cache:
            referencing_cache[cache_key] = _get_referencing_item_keys(
                param_dict, repository_items, repository_directory, path_cache
            )
        else:
            logger.debug(
                "Reusing %d cached referencing item(s) for item_type=%r, item_name=%r, file_path=%r",
                len(referencing_cache[cache_key]),
                param_dict.get("item_type"),
                param_dict.get("item_name"),
                param_dict.get("file_path"),
            )
        referencing_keys = referencing_cache[cache_key]

        # Keep only published items to which this parameter rule applies, excluding self-references
        published_referencing_keys = [
            key for key in referencing_keys if key in publish_item_keys and key != referenced_key
        ]
        if not published_referencing_keys:
            logger.debug("No non-self publish items match the parameter rule referencing '%s'", referenced_key)
            continue

        # Fail before publishing when selective deployment excludes a required new item
        if referenced_key not in publish_item_keys:
            referencing_items = ", ".join(sorted(published_referencing_keys))
            msg = (
                f"Cannot bulk publish {referencing_items}: referenced item '{referenced_key}' is excluded "
                "from this deployment and does not exist in the target workspace. Include the referenced "
                "item or deploy it first."
            )
            raise InputError(msg, logger)

        # Add unique in-batch edges from dependents to the referenced item
        for referencing_key in published_referencing_keys:
            edge = (referencing_key, referenced_key)
            if edge not in seen_edges:
                seen_edges.add(edge)
                edges.append(edge)
                logger.debug("Added item dependency: '%s' requires '%s'", referencing_key, referenced_key)

    logger.debug(
        "Built dynamic-variable dependency graph with %d unique edge(s) (dependent, prerequisite): %s",
        len(edges),
        edges,
    )
    return edges


def get_async_attributes_to_wait_for(
    workspace_obj: "FabricWorkspace",
    publish_item_keys: set[str],
) -> dict[str, set[str]]:
    """
    Map referenced items in the current publish operation to their asynchronously provisioned attributes.

    Each attribute must be available before subsequent publish stages can resolve its dynamic-variable reference.
    References to items outside the current publish operation are ignored.
    """
    result: dict[str, set[str]] = {}

    # Find current-workspace references to asynchronously provisioned attributes
    for _param_dict, parsed in _iter_dynamic_replace_values(workspace_obj):
        if parsed is None or parsed.attribute not in ASYNC_PROVISIONED_ATTRIBUTES:
            continue

        # Record async attributes required from source items created in this publish operation
        # Example: {"Lakehouse.Sample_LH": {"sqlendpoint"}}
        key = f"{parsed.item_type}.{parsed.item_name}"
        if key in publish_item_keys:
            result.setdefault(key, set()).add(parsed.attribute)

    logger.debug("Asynchronous attribute requirements: %s", result)
    return result


_GUID_REFERENCE_PATTERN = re.compile(r"[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")


def build_logical_id_links(
    items_with_context: list[tuple[str, "Item", object]],
) -> list[tuple[str, str]]:
    """
    Find items bound by logical-ID which must be published in the same bulk request.

    Returns:
        A list of unique logical-ID-linked item pairs.
    """
    logical_id_to_item_key: dict[str, str] = {}
    links: set[tuple[str, str]] = set()

    # Map each publishable item's logical ID to its graph key
    for item_name, item, _publisher in items_with_context:
        logical_id_to_item_key[item.logical_id.lower()] = f"{item.type}.{item_name}"

    # A reference requires at least two distinct logical IDs to relate
    if len(logical_id_to_item_key) < 2:
        return []

    # Identify logical ID bindings between items based on their file contents (skip .platform files)
    for item_name, item, _publisher in items_with_context:
        referencing_key = f"{item.type}.{item_name}"
        for file in item.item_files:
            if file.type != "text" or file.name == ".platform":
                continue

            # Search for GUID references within the file contents
            for match in _GUID_REFERENCE_PATTERN.finditer(file.contents):
                # Check if the matched GUID corresponds to a known logical ID
                referenced_key = logical_id_to_item_key.get(match.group(0).lower())
                # If a valid reference is found and it points to a different item, create a link
                if referenced_key is not None and referenced_key != referencing_key:
                    link = (
                        (referencing_key, referenced_key)
                        # Canonicalize the pair for deduplication
                        if referencing_key < referenced_key
                        else (referenced_key, referencing_key)
                    )
                    links.add(link)

    # Return the list of unique logical-ID-linked item pairs
    # Example: [("DataPipeline.Sample_PL", "Notebook.Sample_NB"), ("Notebook.Sample_NB", "Environment.Sample_ENV")]
    result = list(links)
    logger.debug("Found %d logical-ID same-batch link(s): %s", len(result), result)
    return result


def compute_publish_batches(
    items_with_context: list[tuple[str, object, object]],
    dependency_edges: list[tuple[str, str]],
    logical_id_links: list[tuple[str, str]] = (),
) -> list[list[tuple[str, object, object]]]:
    """
    Compute dependency-aware batches for bulk publishing.

    Each publish stage becomes one batch. Publish units determine which items must remain together,
    while dynamic-variable dependencies determine the order in which batches are published. When
    there are neither dependency edges nor logical-ID links, all items are returned in a single batch.

    Args:
        items_with_context: A list of tuples containing the item name, item object, and publisher context.
        dependency_edges: A list of tuples representing dynamic-variable dependencies between items.
        logical_id_links: List of item pairs bound by logical ID that must be published in the same batch.

    Raises:
        InputError: If dependencies contain a cycle, or a dynamic-variable dependency exists
            between two items forced into the same batch by a logical-ID reference.
    """
    # Return a single batch if there are no dependency edges or logical ID links to consider
    if not dependency_edges and not logical_id_links:
        logger.debug("No dependency constraints found, placing all items in one batch")
        _validate_batch_size(items_with_context)
        return [items_with_context]

    item_key_to_context: dict[str, tuple[str, object, object]] = {}
    ordered_keys: list[str] = []

    # Index each item by key while preserving its input order for deterministic batching
    for item_name, item, publisher in items_with_context:
        key = f"{item.type}.{item_name}"
        item_key_to_context[key] = (item_name, item, publisher)
        ordered_keys.append(key)

    logger.debug("Bulk publish items in input order: %s", ordered_keys)

    item_key_index = {key: index for index, key in enumerate(ordered_keys)}

    # Build publish units based on logical ID links
    publish_units = _build_publish_units(ordered_keys, logical_id_links, item_key_index)
    # Build publish stages based on dependency edges
    publish_stages = _build_publish_stages(publish_units, dependency_edges, item_key_index)

    # Form a publish batch from each dependency stage
    batches: list[list[tuple[str, object, object]]] = []
    for stage_index, stage in enumerate(publish_stages):
        batch: list[tuple[str, object, object]] = []
        # Extract all items from the unit associated with each representative in the given stage
        for representative in stage:
            for item_key in publish_units.items_by_representative[representative]:
                # Append the item along with their bulk API context to the batch
                batch.append(item_key_to_context[item_key])

        batch_number = stage_index + 1 if len(publish_stages) > 1 else None
        _validate_batch_size(batch, batch_number)
        batches.append(batch)

    return batches


# Dependency management helpers


def _build_publish_units(
    ordered_keys: list[str],
    logical_id_links: list[tuple[str, str]],
    item_key_index: dict[str, int],
) -> _PublishUnits:
    """
    Arrange items into publish units based on logical ID links. Items in the same publish unit
    must be published together in the same batch.

    Args:
        ordered_keys: List of item keys in the order they will be published.
        logical_id_links: List of item pairs bound by logical ID that must be published in the same batch.
        item_key_index: Mapping from item key to its index in the ordered_keys list.

    Returns:
        _PublishUnits: Mappings from each representative to its publish-unit members and from each item to its representative.
    """
    # Limit processing to items included in this publish operation
    publish_item_keys = set(ordered_keys)

    # Each item initially represents its own publish unit
    parent_by_item = {key: key for key in ordered_keys}

    # Merge publish units for all logical ID bound item pairs
    # Each pair is an undirected link. Connected links are merged into one publish unit:
    # [("item1", "item2"), ("item2", "item3")] -> one unit containing item1, item2, and item3
    for item_a, item_b in logical_id_links:
        if item_a in publish_item_keys and item_b in publish_item_keys:
            _merge_publish_units(item_a, item_b, parent_by_item, item_key_index)

    # Map each item directly to the item that represents its final publish unit {item_key: representative_key}
    # Example: {"item1": "item1", "item2": "item1", "item3": "item1"}
    representative_by_item = {key: _find_unit_representative(key, parent_by_item) for key in ordered_keys}

    # Collect publish unit members in their original input order {representative_key: [item_key, ...]}
    # Example: {"item1": ["item1", "item2", "item3"]}
    items_by_representative: dict[str, list[str]] = {}
    for key in ordered_keys:
        items_by_representative.setdefault(representative_by_item[key], []).append(key)

    logger.debug("Built %d publish units: %s", len(items_by_representative), items_by_representative)

    # Return the constructed publish units containing the mapping of representatives to their items and vice versa
    return _PublishUnits(
        items_by_representative=items_by_representative,
        representative_by_item=representative_by_item,
    )


def _build_publish_stages(
    publish_units: _PublishUnits,
    dependency_edges: list[tuple[str, str]],
    key_index: dict[str, int],
) -> list[list[str]]:
    """
    Arrange publish units into stages so referenced items are available before their dependents are published.

    Each stage becomes one batch. After the batch is published, its item values are refreshed so
    dynamic variables in later stages can be resolved. Independent units can share a stage.

    Args:
        publish_units: Mappings between each publish-unit representative and its member items.
        dependency_edges: List of item-level dependency edges (referencing, referenced).
        key_index: Mapping from item key to its index in the ordered_keys list.

    Returns:
        List of stages, where each stage contains publish units that can share one batch.
    """
    # Track the number of dependencies that each publish unit has
    in_degree = {unit: 0 for unit in publish_units.items_by_representative}
    # Track which publish units depend on each unit for topological sorting
    dependents: dict[str, list[str]] = {unit: [] for unit in publish_units.items_by_representative}
    # Track which unit-to-unit dependencies have already been recorded to avoid duplicates
    seen_unit_edges: set[tuple[str, str]] = set()

    # Convert item-level dependency edges into edges between publish units
    for referencing, referenced in dependency_edges:
        referencing_unit = publish_units.representative_by_item.get(referencing)
        referenced_unit = publish_units.representative_by_item.get(referenced)

        # Reject dependencies that require one publish unit to be published both together and in sequence
        if referencing_unit == referenced_unit:
            conflict = ", ".join(sorted(publish_units.items_by_representative[referencing_unit]))
            msg = (
                f"Cannot bulk publish {conflict}: a logical-ID reference requires the items in one batch, "
                f"but a dynamic variable requires separate batches. Deploy these items separately, "
                "publishing dynamic-variable dependencies first."
            )
            raise InputError(msg, logger)

        # Record each unique publish-unit dependency and update its topological-sort bookkeeping
        edge = (referencing_unit, referenced_unit)
        if edge not in seen_unit_edges:
            seen_unit_edges.add(edge)
            in_degree[referencing_unit] += 1
            dependents[referenced_unit].append(referencing_unit)
            logger.debug("Added publish-unit dependency: '%s' requires '%s'", referencing_unit, referenced_unit)
        else:
            logger.debug(
                "Ignoring duplicate publish-unit dependency: '%s' requires '%s'", referencing_unit, referenced_unit
            )

    stages: list[list[str]] = []

    # Select dependency-free publish units in their original input order
    current_units: list[str] = sorted(
        (unit for unit, degree in in_degree.items() if degree == 0),
        key=key_index.get,
    )
    processed_units: set[str] = set()

    # Build one publish stage at a time until no dependency-free publish units remain
    while current_units:
        stages.append(current_units)
        logger.debug("Scheduled publish stage %d with unit(s) %s", len(stages), current_units)
        next_units = []

        # Mark this stage complete and release publish units that depended on it
        for unit in current_units:
            processed_units.add(unit)
            for dependent_unit in dependents[unit]:
                in_degree[dependent_unit] -= 1
                if in_degree[dependent_unit] == 0:
                    next_units.append(dependent_unit)

        # Preserve input order when scheduling the newly dependency-free publish units
        current_units = sorted(next_units, key=key_index.get)

    # Any unprocessed publish units are trapped in a circular dependency
    if len(processed_units) < len(publish_units.items_by_representative):
        cycle_keys = sorted(
            member
            for unit in (set(publish_units.items_by_representative) - processed_units)
            for member in publish_units.items_by_representative[unit]
        )
        msg = f"Circular dynamic variable dependency detected among: {', '.join(cycle_keys)}"
        raise InputError(msg, logger)

    # Return the ordered stages that can be published sequentially
    return stages


# Union-find (disjoint-set) helpers


def _find_unit_representative(
    item: str,
    parent_by_item: dict[str, str],
) -> str:
    """
    Find an item's publish-unit representative and shorten its path to that representative.

    Args:
        item: The item for which to find the publish-unit representative.
        parent_by_item: Mapping from each item to its parent in the union-find structure.

    Returns:
        The representative item of the publish unit to which the given item belongs.
    """
    # Initialize the representative to the item itself
    representative = item

    # Traverse up the parent chain to find the publish unit's representative
    while parent_by_item[representative] != representative:
        representative = parent_by_item[representative]

    # Path compression: update all items along the path to point directly to the representative
    while parent_by_item[item] != representative:
        parent = parent_by_item[item]
        parent_by_item[item] = representative
        item = parent

    return representative


def _merge_publish_units(
    item_a: str,
    item_b: str,
    parent_by_item: dict[str, str],
    item_key_index: dict[str, int],
) -> None:
    """
    Merge two publish units, retaining the earliest indexed item as their representative.

    Args:
        item_a: The first item to merge.
        item_b: The second item to merge.
        parent_by_item: Mapping from each item to its parent in the union-find structure.
        item_key_index: Mapping from each item to its index, used to determine the earliest item.
    """
    unit_a = _find_unit_representative(item_a, parent_by_item)
    unit_b = _find_unit_representative(item_b, parent_by_item)

    # Return early if both items are already in the same publish unit
    if unit_a == unit_b:
        logger.debug("Items '%s' and '%s' already belong to publish unit '%s'", item_a, item_b, unit_a)
        return

    # Merge the two publish units, retaining the earliest item as the representative
    if item_key_index[unit_a] <= item_key_index[unit_b]:
        parent_by_item[unit_b] = unit_a
        representative = unit_a
    else:
        parent_by_item[unit_a] = unit_b
        representative = unit_b

    logger.debug("Merged publish units for '%s' and '%s' under representative '%s'", item_a, item_b, representative)


# Parameter dictionary processing helpers


def _iter_dynamic_replace_values(
    workspace_obj: "FabricWorkspace",
) -> Iterator[tuple[dict, Optional["ParsedDynamicVariable"]]]:
    """Yield each parameter dictionary with its parsed current-workspace item variable, or None."""
    for param_name in ("find_replace", "key_value_replace"):
        for param_dict in workspace_obj.environment_parameter.get(param_name, []):
            replace_value = param_dict.get("replace_value")
            if not isinstance(replace_value, dict):
                continue
            processed = process_environment_key(workspace_obj.environment, dict(replace_value))
            env_value = processed.get(workspace_obj.environment)
            # If the parameter environment value is a string (potentially a dynamic replacement variable)
            if isinstance(env_value, str):
                parsed = parse_dynamic_variable(env_value) if env_value.startswith("$") else None
                if parsed is not None and (parsed.kind != "item" or parsed.workspace_name is not None):
                    parsed = None
                yield param_dict, parsed


def _hashable_filter(value: object) -> object:
    """Normalizes an optional filter value (None, str, or list) to a hashable cache-key component."""
    if isinstance(value, list):
        return tuple(value)
    return value


def _get_referencing_item_keys(
    param_dict: dict,
    repository_items: dict,
    repository_directory: Path,
    path_cache: Optional[dict[str, Path]] = None,
) -> list[str]:
    """
    Find the repository items whose files are targeted by a parameter rule and set as the referencing items.

    Args:
        param_dict: The parameter dictionary containing optional filters.
        repository_items: The repository items.
        repository_directory: Local directory path of the repository where items are to be deployed from.
        path_cache: Cache for resolved item paths.

    Returns:
        list[str]: The list of referencing item keys that match the parameter rule.
    """
    item_type_filter_value = param_dict.get("item_type")
    item_name_filter_value = param_dict.get("item_name")
    file_path_filter_value = param_dict.get("file_path")
    resolved_file_paths = process_input_path(repository_directory, file_path_filter_value)

    keys = []
    for item_type, items in repository_items.items():
        # Skip every item of this type when the parameter rule's item_type filter does not include it
        if not _matches_filter(item_type, item_type_filter_value):
            continue
        for item_name, item in items.items():
            # Skip this item when the parameter rule's item_name filter does not include it
            if not _matches_filter(item_name, item_name_filter_value):
                continue

            # When file_path is specified, verify that the rule selects a file inside this item's directory
            if resolved_file_paths is not None:
                item_key = f"{item_type}.{item_name}"

                # Reuse resolved item directories across parameter rules to avoid repeated filesystem work
                item_path = path_cache.get(item_key) if path_cache is not None else None
                if item_path is None:
                    item_path = item.path.resolve()
                    if path_cache is not None:
                        path_cache[item_key] = item_path

                # A path-filtered rule applies when at least one selected file belongs to this item
                if not any(file_path.is_relative_to(item_path) for file_path in resolved_file_paths):
                    continue

            # Reaching this point means the item satisfied every filter supplied by the rule
            keys.append(f"{item_type}.{item_name}")

    # Return the item keys that matched all filters
    # Example: ["Notebook.Sample_NB", "DataPipeline.Sample_PL"]
    logger.debug(
        "Parameter filters item_type=%r, item_name=%r, file_path=%r matched %d referencing item(s): %s",
        item_type_filter_value,
        item_name_filter_value,
        file_path_filter_value,
        len(keys),
        keys,
    )
    return keys


def _matches_filter(value: str, filter_value: object) -> bool:
    """Return whether a value matches an optional scalar or list filter."""
    if filter_value is None:
        return True
    if isinstance(filter_value, list):
        return value in filter_value
    return value == filter_value


# Batch validation helpers


def _validate_batch_size(batch: list[tuple[str, object, object]], batch_number: Optional[int] = None) -> None:
    """
    Validate that the batch does not exceed the maximum allowed item count.

    Raises:
        InputError: If the batch contains more items than the API limit.
    """
    batch_count = len(batch)
    if batch_count > constants.BULK_ITEM_COUNT_LIMIT:
        batch_label = f" {batch_number}" if batch_number is not None else ""
        msg = (
            f"Bulk publish batch{batch_label} item count ({batch_count}) exceeds the API limit "
            f"of {constants.BULK_ITEM_COUNT_LIMIT} items."
        )
        raise InputError(msg, logger)
