namespace FabricCicd.Items;

public static class Environment
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("Environment"))
        {
            ws.PublishItem("all", "Environment");
        }
    }
}
