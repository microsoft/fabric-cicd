# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Base interface for all item publishers."""

import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Optional

from fabric_cicd._common._item import Item
from fabric_cicd._common._logging import get_item_logger
from fabric_cicd.constants import ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


@dataclass
class ParallelConfig:
    """Configuration for parallel execution behavior of a publisher.

    This dataclass controls how the base ItemPublisher.publish_all() method
    executes publish_one() calls - either in parallel or sequentially.

    Attributes:
        enabled: If True, publish_one calls run in parallel using ThreadPoolExecutor.
                 If False, items are published sequentially. Default is True.
        max_workers: Maximum number of concurrent threads. None means use ThreadPoolExecutor default.
        ordered_items_func: Optional callable that returns an ordered list of item names.
                           When provided, items are published sequentially in this order.
                           This takes precedence over `enabled=True`.
    """

    enabled: bool = True
    max_workers: Optional[int] = None
    ordered_items_func: Optional[Callable[["ItemPublisher"], list[str]]] = None


class PublishError(Exception):
    """Exception raised when one or more publish operations fail.

    Attributes:
        errors: List of (item_name, exception) tuples for all failed items.
    """

    def __init__(self, errors: list[tuple[str, Exception]]) -> None:
        """Initialize with a list of (item_name, exception) tuples."""
        self.errors = errors
        failed_names = [name for name, _ in errors]
        super().__init__(f"Failed to publish {len(errors)} item(s): {failed_names}")


class Publisher(ABC):
    """Base interface for all publishers."""

    def __init__(self, fabric_workspace_obj: "FabricWorkspace") -> None:
        """
        Initialize the publisher with a FabricWorkspace object.

        Args:
            fabric_workspace_obj: The FabricWorkspace object containing items to be published.
        """
        self.fabric_workspace_obj = fabric_workspace_obj

    @abstractmethod
    def publish_one(self, name: str, obj: object) -> None:
        """
        Publish a single object.

        Args:
            name: The name of the object to publish.
            obj: The object to publish.
        """
        raise NotImplementedError

    @abstractmethod
    def publish_all(self) -> None:
        """Publish all objects."""
        raise NotImplementedError


class ItemPublisher(Publisher):
    """
    Base interface for all item type publishers.

    Provides a default parallel publish_all() implementation that:
    - Executes publish_one() calls in parallel using ThreadPoolExecutor
    - Aggregates errors from all failed items into a single PublishError
    - Supports pre/post hooks via pre_publish_all() and post_publish_all()
    - Can be configured via the parallel_config class attribute

    Subclasses can customize behavior by:
    - Setting parallel_config to control parallelization
    - Overriding pre_publish_all() for setup before publishing
    - Overriding post_publish_all() for cleanup after publishing
    - Overriding get_items_to_publish() to filter or order items
    - Overriding get_unpublish_order() for dependency-aware unpublishing
    - Overriding post_publish_all_check() for async publish state verification

    Publish Lifecycle:
        1. pre_publish_all()
        2. get_items_to_publish()
        3. publish_one() - called for each item
        4. post_publish_all()
        5. post_publish_all_check() - if has_async_publish_check

    Unpublish Hook:
        - get_unpublish_order() - if has_dependency_tracking
    """

    # region Class Attributes

    item_type: str
    """Mandatory property to be set by each publisher subclass"""

    parallel_config: ParallelConfig = ParallelConfig()
    """Configuration for parallel execution - subclasses can override with their own ParallelConfig"""

    has_async_publish_check: bool = False
    """Set to True if this publisher implements post_publish_all_check() for async state verification"""

    has_dependency_tracking: bool = False
    """Set to True if this publisher implements get_unpublish_order() for dependency ordering"""

    # endregion

    # region Initialization & Factory

    def __init__(self, fabric_workspace_obj: "FabricWorkspace") -> None:
        """
        Initialize the publisher with a FabricWorkspace object.

        Args:
            fabric_workspace_obj: The FabricWorkspace object containing items to be published.
        """
        super().__init__(fabric_workspace_obj)

    @staticmethod
    def create(item_type: ItemType, fabric_workspace_obj: "FabricWorkspace") -> "ItemPublisher":
        """
        Factory method to create the appropriate publisher for a given item type.

        Args:
            item_type: The ItemType enum value for which to create a publisher.
            fabric_workspace_obj: The FabricWorkspace object containing items to be published.

        Returns:
            An instance of the appropriate ItemPublisher subclass.

        Raises:
            ValueError: If the item type is not supported.
        """
        from fabric_cicd._items._activator import ActivatorPublisher
        from fabric_cicd._items._apacheairflowjob import ApacheAirflowJobPublisher
        from fabric_cicd._items._copyjob import CopyJobPublisher
        from fabric_cicd._items._dataagent import DataAgentPublisher
        from fabric_cicd._items._dataflowgen2 import DataflowPublisher
        from fabric_cicd._items._datapipeline import DataPipelinePublisher
        from fabric_cicd._items._environment import EnvironmentPublisher
        from fabric_cicd._items._eventhouse import EventhousePublisher
        from fabric_cicd._items._eventstream import EventstreamPublisher
        from fabric_cicd._items._graphqlapi import GraphQLApiPublisher
        from fabric_cicd._items._kqldashboard import KQLDashboardPublisher
        from fabric_cicd._items._kqldatabase import KQLDatabasePublisher
        from fabric_cicd._items._kqlqueryset import KQLQuerysetPublisher
        from fabric_cicd._items._lakehouse import LakehousePublisher
        from fabric_cicd._items._mirroreddatabase import MirroredDatabasePublisher
        from fabric_cicd._items._mlexperiment import MLExperimentPublisher
        from fabric_cicd._items._mounteddatafactory import MountedDataFactoryPublisher
        from fabric_cicd._items._notebook import NotebookPublisher
        from fabric_cicd._items._report import ReportPublisher
        from fabric_cicd._items._semanticmodel import SemanticModelPublisher
        from fabric_cicd._items._sparkjobdefinition import SparkJobDefinitionPublisher
        from fabric_cicd._items._sqldatabase import SQLDatabasePublisher
        from fabric_cicd._items._userdatafunction import UserDataFunctionPublisher
        from fabric_cicd._items._variablelibrary import VariableLibraryPublisher
        from fabric_cicd._items._warehouse import WarehousePublisher

        publisher_mapping = {
            ItemType.VARIABLE_LIBRARY: VariableLibraryPublisher,
            ItemType.WAREHOUSE: WarehousePublisher,
            ItemType.MIRRORED_DATABASE: MirroredDatabasePublisher,
            ItemType.LAKEHOUSE: LakehousePublisher,
            ItemType.SQL_DATABASE: SQLDatabasePublisher,
            ItemType.ENVIRONMENT: EnvironmentPublisher,
            ItemType.USER_DATA_FUNCTION: UserDataFunctionPublisher,
            ItemType.EVENTHOUSE: EventhousePublisher,
            ItemType.SPARK_JOB_DEFINITION: SparkJobDefinitionPublisher,
            ItemType.NOTEBOOK: NotebookPublisher,
            ItemType.SEMANTIC_MODEL: SemanticModelPublisher,
            ItemType.REPORT: ReportPublisher,
            ItemType.COPY_JOB: CopyJobPublisher,
            ItemType.KQL_DATABASE: KQLDatabasePublisher,
            ItemType.KQL_QUERYSET: KQLQuerysetPublisher,
            ItemType.REFLEX: ActivatorPublisher,
            ItemType.EVENTSTREAM: EventstreamPublisher,
            ItemType.KQL_DASHBOARD: KQLDashboardPublisher,
            ItemType.DATAFLOW: DataflowPublisher,
            ItemType.DATA_PIPELINE: DataPipelinePublisher,
            ItemType.GRAPHQL_API: GraphQLApiPublisher,
            ItemType.APACHE_AIRFLOW_JOB: ApacheAirflowJobPublisher,
            ItemType.MOUNTED_DATA_FACTORY: MountedDataFactoryPublisher,
            ItemType.DATA_AGENT: DataAgentPublisher,
            ItemType.ML_EXPERIMENT: MLExperimentPublisher,
        }

        publisher_class = publisher_mapping.get(item_type)
        if publisher_class is None:
            msg = f"No publisher found for item type: {item_type}"
            raise ValueError(msg)

        return publisher_class(fabric_workspace_obj)

    @staticmethod
    def get_item_types_to_publish(fabric_workspace_obj: "FabricWorkspace") -> list[tuple[int, ItemType]]:
        """
        Get the ordered list of item types that should be published.

        Returns item types that are both in scope and have items in the repository,
        ordered according to SERIAL_ITEM_PUBLISH_ORDER.

        Args:
            fabric_workspace_obj: The FabricWorkspace object containing scope and repository info.

        Returns:
            List of (order_num, ItemType) tuples for item types that should be published.
        """
        from fabric_cicd import constants

        result = []
        for order_num, item_type in constants.SERIAL_ITEM_PUBLISH_ORDER.items():
            if (
                item_type.value in fabric_workspace_obj.item_type_in_scope
                and item_type.value in fabric_workspace_obj.repository_items
            ):
                result.append((order_num, item_type))
        return result

    @staticmethod
    def get_item_types_to_unpublish(fabric_workspace_obj: "FabricWorkspace") -> list[str]:
        """
        Get the ordered list of item types that should be unpublished.

        Returns item types in reverse publish order that are in scope, have deployed items,
        and meet feature flag requirements. Logs warnings for skipped item types.

        Args:
            fabric_workspace_obj: The FabricWorkspace object containing scope and deployed items info.

        Returns:
            List of item type strings in the order they should be unpublished.
        """
        from fabric_cicd import constants

        unpublish_order = []
        for item_type in reversed(list(constants.SERIAL_ITEM_PUBLISH_ORDER.values())):
            if (
                item_type.value in fabric_workspace_obj.item_type_in_scope
                and item_type.value in fabric_workspace_obj.deployed_items
            ):
                unpublish_flag = constants.UNPUBLISH_FLAG_MAPPING.get(item_type.value)
                # Append item_type if no feature flag is required or the corresponding flag is enabled
                if not unpublish_flag or unpublish_flag in constants.FEATURE_FLAG:
                    unpublish_order.append(item_type.value)
                elif unpublish_flag and unpublish_flag not in constants.FEATURE_FLAG:
                    # Log warning when unpublish is skipped due to missing feature flag
                    logger.warning(
                        f"Skipping unpublish for {item_type.value} items because the '{unpublish_flag}' feature flag is not enabled."
                    )
        return unpublish_order

    @staticmethod
    def get_orphaned_items(
        fabric_workspace_obj: "FabricWorkspace",
        item_type: str,
        item_name_exclude_regex: Optional[str] = None,
        items_to_include: Optional[list[str]] = None,
    ) -> list[str]:
        """
        Get the list of orphaned items that should be unpublished for a given item type.

        Orphaned items are those deployed but not present in the repository,
        filtered by exclusion regex or items_to_include list.

        Args:
            fabric_workspace_obj: The FabricWorkspace object containing deployed and repository items.
            item_type: The item type string to check for orphans.
            item_name_exclude_regex: Optional regex pattern to exclude items from unpublishing.
            items_to_include: Optional list of items in "name.type" format to include for unpublishing.

        Returns:
            List of item names that should be unpublished.
        """
        import re

        deployed_names = set(fabric_workspace_obj.deployed_items.get(item_type, {}).keys())
        repository_names = set(fabric_workspace_obj.repository_items.get(item_type, {}).keys())
        to_delete_set = deployed_names - repository_names

        if items_to_include is not None:
            # Filter to only items in the include list
            return [name for name in to_delete_set if f"{name}.{item_type}" in items_to_include]
        if item_name_exclude_regex:
            # Filter out items matching the exclude regex
            regex_pattern = re.compile(item_name_exclude_regex)
            return [name for name in to_delete_set if not regex_pattern.match(name)]
        return list(to_delete_set)

    # endregion

    # region Public Methods

    def publish_all(self) -> None:
        """
        Execute the publish operation for this item type.

        1. Calls pre_publish_all() for any setup operations
        2. Gets items via get_items_to_publish()
        3. Publishes items (parallel or sequential based on parallel_config)
        4. Calls post_publish_all() for any finalization
        5. Raises PublishError if any items failed

        The parallel_config class attribute controls execution:
        - If ordered_items_func is set: publishes in that order sequentially
        - If enabled=True: publishes in parallel
        - If enabled=False: publishes sequentially

        Raises:
            PublishError: If one or more items failed to publish.
        """
        self.pre_publish_all()
        items = self.get_items_to_publish()
        if not items:
            self.post_publish_all()
            return

        config = getattr(self.__class__, "parallel_config", ParallelConfig())

        if config.ordered_items_func is not None:
            order = config.ordered_items_func(self)
            errors = self._publish_items_ordered(items, order)
        elif config.enabled:
            errors = self._publish_items_parallel(items)
        else:
            errors = self._publish_items_sequential(items)

        self.post_publish_all()

        if errors:
            raise PublishError(errors)

    def publish_one(self, item_name: str, _item: "Item") -> None:
        """
        Publish a single item.

        Args:
            item_name: The name of the item to publish.
            _item: The Item object to publish.

        Default implementation publishes the item using _publish_item.
        Subclasses can override this method for custom publishing logic.
        """
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)

    def get_items_to_publish(self) -> dict[str, "Item"]:
        """
        Get the items to publish for this item type.

        Returns:
            Dictionary mapping item names to Item objects.

        Subclasses can override to filter or transform the items.
        """
        return self.fabric_workspace_obj.repository_items.get(self.item_type, {})

    def get_unpublish_order(self, items_to_unpublish: list[str]) -> list[str]:
        """
        Get the ordered list of item names based on dependencies for unpublishing.

        Args:
            items_to_unpublish: List of item names to be unpublished.

        Returns:
            List of item names in the order they should be unpublished (reverse dependency order).

        Default implementation returns items in their original order.
        Subclasses with dependency tracking should override for proper ordering.
        """
        return items_to_unpublish

    def pre_publish_all(self) -> None:
        """
        Hook called before publishing any items.

        Subclasses can override to perform setup, validation, or refresh operations.
        Default implementation does nothing.
        """
        pass

    def post_publish_all(self) -> None:
        """
        Hook called after all items have been published successfully.

        Subclasses can override to perform cleanup, binding, or finalization.
        Default implementation does nothing.
        """
        pass

    def post_publish_all_check(self) -> None:
        """
        Hook called after publish_all completes to verify async publish state.

        Subclasses can override this to check the state of asynchronous publish
        operations (e.g., Environment items that have async publish workflows).
        Default implementation does nothing.

        This method is called separately from publish_all() and should be invoked
        by the orchestration layer after all items of this type have been published.
        """
        pass

    # endregion

    # region Publishing

    def _publish_items_parallel(self, items: dict[str, "Item"]) -> list[tuple[str, Exception]]:
        """
        Publish items in parallel using ThreadPoolExecutor.

        Args:
            items: Dictionary mapping item names to Item objects.

        Returns:
            List of (item_name, exception) tuples for failed items.
        """
        errors: list[tuple[str, Exception]] = []
        config = getattr(self.__class__, "parallel_config", ParallelConfig())

        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {
                executor.submit(self.publish_one, item_name, item): (item_name, item)
                for item_name, item in items.items()
            }

            for future in as_completed(futures):
                item_name, _ = futures[future]
                try:
                    future.result()
                except Exception as e:
                    item_logger = get_item_logger(__name__, item_type=self.item_type, item_name=item_name)
                    item_logger.error(f"Failed to publish: {e}")
                    errors.append((item_name, e))

        return errors

    def _publish_items_sequential(self, items: dict[str, "Item"]) -> list[tuple[str, Exception]]:
        """
        Publish items sequentially.

        Args:
            items: Dictionary mapping item names to Item objects.

        Returns:
            List of (item_name, exception) tuples for failed items.
        """
        errors: list[tuple[str, Exception]] = []

        for item_name, item in items.items():
            try:
                self.publish_one(item_name, item)
            except Exception as e:
                item_logger = get_item_logger(__name__, item_type=self.item_type, item_name=item_name)
                item_logger.error(f"Failed to publish: {e}")
                errors.append((item_name, e))

        return errors

    def _publish_items_ordered(self, items: dict[str, "Item"], order: list[str]) -> list[tuple[str, Exception]]:
        """
        Publish items in a specific order sequentially.

        Args:
            items: Dictionary mapping item names to Item objects.
            order: List of item names in the order they should be published.

        Returns:
            List of (item_name, exception) tuples for failed items.
        """
        errors: list[tuple[str, Exception]] = []

        for item_name in order:
            if item_name in items:
                item = items[item_name]
                try:
                    self.publish_one(item_name, item)
                except Exception as e:
                    item_logger = get_item_logger(__name__, item_type=self.item_type, item_name=item_name)
                    item_logger.error(f"Failed to publish: {e}")
                    errors.append((item_name, e))

        return errors

    # endregion
