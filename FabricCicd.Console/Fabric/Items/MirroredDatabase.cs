namespace FabricCicd.Items;

public static class MirroredDatabase
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("MirroredDatabase"))
        {
            ws.PublishItem("all", "MirroredDatabase");
        }
    }
}
