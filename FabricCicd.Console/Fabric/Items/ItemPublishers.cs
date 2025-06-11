namespace FabricCicd.Items;

/// <summary>
/// Collection of helper methods that mirror the item specific publish logic
/// from the Python implementation. The methods here are simplified stubs.
/// </summary>
public static class ItemPublishers
{
    public static void PublishNotebooks(FabricWorkspace ws) => Notebook.Publish(ws);

    public static void PublishReports(FabricWorkspace ws) => Report.Publish(ws);

    public static void PublishDataPipelines(FabricWorkspace ws) => DataPipeline.Publish(ws);

    public static void PublishEnvironments(FabricWorkspace ws) => Environment.Publish(ws);

    public static void PublishActivators(FabricWorkspace ws) => Activator.Publish(ws);

    public static void PublishCopyJobs(FabricWorkspace ws) => CopyJob.Publish(ws);

    public static void PublishEventhouses(FabricWorkspace ws) => Eventhouse.Publish(ws);

    public static void PublishEventstreams(FabricWorkspace ws) => Eventstream.Publish(ws);

    public static void PublishKqlDatabases(FabricWorkspace ws) => KqlDatabase.Publish(ws);

    public static void PublishKqlQuerysets(FabricWorkspace ws) => KqlQueryset.Publish(ws);

    public static void PublishLakehouses(FabricWorkspace ws) => Lakehouse.Publish(ws);

    public static void PublishMirroredDatabase(FabricWorkspace ws) => MirroredDatabase.Publish(ws);

    public static void PublishSemanticModels(FabricWorkspace ws) => SemanticModel.Publish(ws);

    public static void PublishSqlDatabases(FabricWorkspace ws) => SqlDatabase.Publish(ws);

    public static void PublishVariableLibraries(FabricWorkspace ws) => VariableLibrary.Publish(ws);

    public static void PublishWarehouses(FabricWorkspace ws) => Warehouse.Publish(ws);
}
