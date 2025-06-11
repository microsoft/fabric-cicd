namespace FabricCicd.Items;

public static class Warehouse
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("Warehouse"))
        {
            ws.PublishItem("all", "Warehouse");
        }
    }
}
