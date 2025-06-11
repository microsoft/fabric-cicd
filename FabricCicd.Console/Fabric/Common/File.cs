namespace FabricCicd.Common;

/// <summary>
/// Represents a file belonging to an item.
/// This is a very small subset of the Python implementation.
/// </summary>
public class File
{
    public string ItemPath { get; }
    public string FilePath { get; }
    public string Type { get; }
    public string Contents { get; }

    public File(string itemPath, string filePath, string type = "text", string contents = "")
    {
        ItemPath = itemPath;
        FilePath = filePath;
        Type = type;
        Contents = contents;
    }
}
