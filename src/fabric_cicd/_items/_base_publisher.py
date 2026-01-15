# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Base interface for all item publishers."""

from abc import ABC, abstractmethod

from fabric_cicd._common._item import Item
from fabric_cicd.constants import ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace


class ItemPublisher(ABC):
    """Base interface for all item type publishers."""

    """
    Mandatory property to be set by each publisher.
    """
    item_type: str

    def __init__(self, fabric_workspace_obj: "FabricWorkspace") -> None:
        """
        Initialize the publisher with a FabricWorkspace object.

        Args:
            fabric_workspace_obj: The FabricWorkspace object containing items to be published.
        """
        self.fabric_workspace_obj = fabric_workspace_obj

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
        from fabric_cicd._items._orgapp import OrgAppPublisher
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
            ItemType.ORG_APP: OrgAppPublisher,
            ItemType.DATA_AGENT: DataAgentPublisher,
            ItemType.ML_EXPERIMENT: MLExperimentPublisher,
        }

        publisher_class = publisher_mapping.get(item_type)
        if publisher_class is None:
            msg = f"No publisher found for item type: {item_type}"
            raise ValueError(msg)

        return publisher_class(fabric_workspace_obj)

    @abstractmethod
    def publish_one(self, item_name: str, item: "Item") -> None:
        """
        Publish a single item.

        Args:
            item_name: The name of the item to publish.
            item: The Item object to publish.

        This method must be implemented by each concrete item publisher.
        """

    @abstractmethod
    def publish_all(self) -> None:
        """
        Execute the publish operation for this item type.

        This method must be implemented by each concrete item publisher.
        """
        pass
