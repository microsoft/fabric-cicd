namespace FabricCicd.Items;

public static class SemanticModel
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("SemanticModel"))
        {
            ws.PublishItem("all", "SemanticModel");
        }
    }
}
