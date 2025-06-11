namespace FabricCicd.Items;

public static class SqlDatabase
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("SQLDatabase"))
        {
            ws.PublishItem("all", "SQLDatabase");
        }
    }
}
