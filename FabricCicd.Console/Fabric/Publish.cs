using System;
using FabricCicd.Items;

namespace FabricCicd;

public static class Publish
{
    /// <summary>
    /// Placeholder implementation of publish_all_items.
    /// </summary>
    public static void PublishAllItems(FabricWorkspace workspace)
    {
        workspace.RefreshDeployedItems();
        workspace.RefreshRepositoryItems();

        Items.ItemPublishers.PublishVariableLibraries(workspace);
        Items.ItemPublishers.PublishWarehouses(workspace);
        Items.ItemPublishers.PublishLakehouses(workspace);
        Items.ItemPublishers.PublishSqlDatabases(workspace);
        Items.ItemPublishers.PublishMirroredDatabase(workspace);
        Items.ItemPublishers.PublishEnvironments(workspace);
        Items.ItemPublishers.PublishNotebooks(workspace);
        Items.ItemPublishers.PublishSemanticModels(workspace);
        Items.ItemPublishers.PublishReports(workspace);
        Items.ItemPublishers.PublishDataPipelines(workspace);
        Items.ItemPublishers.PublishCopyJobs(workspace);
        Items.ItemPublishers.PublishEventhouses(workspace);
        Items.ItemPublishers.PublishKqlDatabases(workspace);
        Items.ItemPublishers.PublishKqlQuerysets(workspace);
        Items.ItemPublishers.PublishActivators(workspace);
        Items.ItemPublishers.PublishEventstreams(workspace);
    }

    /// <summary>
    /// Placeholder implementation of unpublish_all_orphan_items.
    /// </summary>
    public static void UnpublishAllOrphanItems(FabricWorkspace workspace)
    {
        workspace.RefreshDeployedItems();
        Console.WriteLine("Unpublishing orphaned items");
    }
}
