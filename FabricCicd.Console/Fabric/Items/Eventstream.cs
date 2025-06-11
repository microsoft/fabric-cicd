namespace FabricCicd.Items;

public static class Eventstream
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("Eventstream"))
        {
            ws.PublishItem("all", "Eventstream");
        }
    }
}
