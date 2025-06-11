using FabricCicd;
using FabricCicd.Items;
using ItemActivator = FabricCicd.Items.Activator;
using ItemEnvironment = FabricCicd.Items.Environment;

namespace FabricCicd.Tests;

public class ItemPublisherTests
{
    [Fact]
    public void InvokeAllPublishers()
    {
        var types = new[]
        {
            "Reflex", "CopyJob", "DataPipeline", "Environment", "Eventhouse",
            "Eventstream", "KQLDatabase", "KQLQueryset", "Lakehouse",
            "MirroredDatabase", "Notebook", "Report", "SemanticModel",
            "SQLDatabase", "VariableLibrary", "Warehouse"
        };
        var ws = new FabricWorkspace("id", "/repo", types);

        ItemActivator.Publish(ws);
        CopyJob.Publish(ws);
        DataPipeline.Publish(ws);
        ItemEnvironment.Publish(ws);
        Eventhouse.Publish(ws);
        Eventstream.Publish(ws);
        KqlDatabase.Publish(ws);
        KqlQueryset.Publish(ws);
        Lakehouse.Publish(ws);
        MirroredDatabase.Publish(ws);
        Notebook.Publish(ws);
        Report.Publish(ws);
        SemanticModel.Publish(ws);
        SqlDatabase.Publish(ws);
        VariableLibrary.Publish(ws);
        Warehouse.Publish(ws);
    }
}
