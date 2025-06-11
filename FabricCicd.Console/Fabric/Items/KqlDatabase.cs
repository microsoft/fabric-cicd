namespace FabricCicd.Items;

public static class KqlDatabase
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("KQLDatabase"))
        {
            ws.PublishItem("all", "KQLDatabase");
        }
    }
}
