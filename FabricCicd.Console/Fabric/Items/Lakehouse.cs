namespace FabricCicd.Items;

public static class Lakehouse
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("Lakehouse"))
        {
            ws.PublishItem("all", "Lakehouse");
        }
    }
}
