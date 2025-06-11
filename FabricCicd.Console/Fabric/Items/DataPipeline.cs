namespace FabricCicd.Items;

public static class DataPipeline
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("DataPipeline"))
        {
            ws.PublishItem("all", "DataPipeline");
        }
    }
}
