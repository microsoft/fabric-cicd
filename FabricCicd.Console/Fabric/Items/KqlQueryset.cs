namespace FabricCicd.Items;

public static class KqlQueryset
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("KQLQueryset"))
        {
            ws.PublishItem("all", "KQLQueryset");
        }
    }
}
