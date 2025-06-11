namespace FabricCicd.Items;

public static class Eventhouse
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("Eventhouse"))
        {
            ws.PublishItem("all", "Eventhouse");
        }
    }
}
