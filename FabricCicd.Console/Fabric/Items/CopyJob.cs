namespace FabricCicd.Items;

public static class CopyJob
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("CopyJob"))
        {
            ws.PublishItem("all", "CopyJob");
        }
    }
}
