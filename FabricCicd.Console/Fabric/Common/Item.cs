using System.Collections.Generic;
using System.IO;


namespace FabricCicd.Common;

/// <summary>
/// Represents a deployable item in the workspace.
/// </summary>
public class Item
{
    public string Type { get; }
    public string Name { get; }
    public string Description { get; }
    public string Guid { get; }
    public string LogicalId { get; }
    public string Path { get; }
    public string FolderId { get; }
    public IList<File> ItemFiles { get; } = new List<File>();

    public Item(
        string type,
        string name,
        string description,
        string guid,
        string logicalId = "",
        string path = "",
        string folderId = "")
    {
        Type = type;
        Name = name;
        Description = description;
        Guid = guid;
        LogicalId = logicalId;
        Path = path;
        FolderId = folderId;
    }

    /// <summary>
    /// Collect all files for the item excluding the .platform metadata file.
    /// </summary>
    public void CollectItemFiles()
    {
        ItemFiles.Clear();
        if (string.IsNullOrEmpty(Path) || !Directory.Exists(Path))
        {
            return;
        }

        foreach (var file in Directory.EnumerateFiles(Path, "*", SearchOption.AllDirectories))
        {
            if (System.IO.Path.GetFileName(file).Equals(".platform", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }
            ItemFiles.Add(new File(Path, file));
        }
    }
}
