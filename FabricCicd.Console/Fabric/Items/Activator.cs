namespace FabricCicd.Items;

public static class Activator
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("Reflex"))
        {
            ws.PublishItem("all", "Reflex");
        }
    }
}
