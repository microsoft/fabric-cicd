namespace FabricCicd.Items;

public static class Notebook
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("Notebook"))
        {
            ws.PublishItem("all", "Notebook");
        }
    }
}
