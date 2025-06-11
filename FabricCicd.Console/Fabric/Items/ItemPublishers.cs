namespace FabricCicd.Items;

/// <summary>
/// Collection of helper methods that mirror the item specific publish logic
/// from the Python implementation. The methods here are simplified stubs.
/// </summary>
public static class ItemPublishers
{
    public static void PublishNotebooks(FabricWorkspace ws)
    {
        foreach (var name in ws.ItemTypes)
        {
            if (name == "Notebook")
            {
                ws.PublishItem("all", "Notebook");
            }
        }
    }

    public static void PublishReports(FabricWorkspace ws)
    {
        foreach (var name in ws.ItemTypes)
        {
            if (name == "Report")
            {
                ws.PublishItem("all", "Report");
            }
        }
    }

    public static void PublishDataPipelines(FabricWorkspace ws)
    {
        foreach (var name in ws.ItemTypes)
        {
            if (name == "DataPipeline")
            {
                ws.PublishItem("all", "DataPipeline");
            }
        }
    }

    public static void PublishEnvironments(FabricWorkspace ws)
    {
        foreach (var name in ws.ItemTypes)
        {
            if (name == "Environment")
            {
                ws.PublishItem("all", "Environment");
            }
        }
    }

    // Additional item types below are implemented as no-op placeholders
    public static void PublishActivators(FabricWorkspace ws) => PublishGeneric(ws, "Reflex");
    public static void PublishCopyJobs(FabricWorkspace ws) => PublishGeneric(ws, "CopyJob");
    public static void PublishEventhouses(FabricWorkspace ws) => PublishGeneric(ws, "Eventhouse");
    public static void PublishEventstreams(FabricWorkspace ws) => PublishGeneric(ws, "Eventstream");
    public static void PublishKqlDatabases(FabricWorkspace ws) => PublishGeneric(ws, "KQLDatabase");
    public static void PublishKqlQuerysets(FabricWorkspace ws) => PublishGeneric(ws, "KQLQueryset");
    public static void PublishLakehouses(FabricWorkspace ws) => PublishGeneric(ws, "Lakehouse");
    public static void PublishMirroredDatabase(FabricWorkspace ws) => PublishGeneric(ws, "MirroredDatabase");
    public static void PublishSemanticModels(FabricWorkspace ws) => PublishGeneric(ws, "SemanticModel");
    public static void PublishSqlDatabases(FabricWorkspace ws) => PublishGeneric(ws, "SQLDatabase");
    public static void PublishVariableLibraries(FabricWorkspace ws) => PublishGeneric(ws, "VariableLibrary");
    public static void PublishWarehouses(FabricWorkspace ws) => PublishGeneric(ws, "Warehouse");

    private static void PublishGeneric(FabricWorkspace ws, string type)
    {
        if (ws.ItemTypes.Contains(type))
        {
            ws.PublishItem("all", type);
        }
    }
}
