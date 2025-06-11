namespace FabricCicd.Items;

public static class Report
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("Report"))
        {
            ws.PublishItem("all", "Report");
        }
    }
}
