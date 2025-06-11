using System.Collections.Generic;

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
    public IList<File> ItemFiles { get; } = new List<File>();

    public Item(string type, string name, string description, string guid)
    {
        Type = type;
        Name = name;
        Description = description;
        Guid = guid;
    }
}
