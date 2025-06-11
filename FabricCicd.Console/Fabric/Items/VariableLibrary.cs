namespace FabricCicd.Items;

public static class VariableLibrary
{
    public static void Publish(FabricWorkspace ws)
    {
        if (ws.ItemTypes.Contains("VariableLibrary"))
        {
            ws.PublishItem("all", "VariableLibrary");
        }
    }
}
