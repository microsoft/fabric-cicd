using System;

namespace FabricCicd;

/// <summary>
/// Simplified representation of FabricWorkspace from the Python library.
/// </summary>
public class FabricWorkspace
{
    public string WorkspaceId { get; }
    public string RepositoryDirectory { get; }
    public IList<string> ItemTypes { get; }

    public FabricWorkspace(string workspaceId, string repositoryDirectory, IEnumerable<string> itemTypes)
    {
        WorkspaceId = workspaceId ?? throw new ArgumentNullException(nameof(workspaceId));
        RepositoryDirectory = repositoryDirectory ?? throw new ArgumentNullException(nameof(repositoryDirectory));
        ItemTypes = itemTypes?.ToList() ?? throw new ArgumentNullException(nameof(itemTypes));
    }

    // TODO: Add full implementation of methods from fabric_workspace.py
    public void RefreshRepositoryItems()
    {
        // Placeholder for scanning repo directory
    }

    public void RefreshDeployedItems()
    {
        // Placeholder for calling Fabric API
    }

    public void PublishItem(string name, string itemType)
    {
        // Placeholder publish logic
        Console.WriteLine($"Publishing {itemType} {name}");
    }

    public void UnpublishItem(string name, string itemType)
    {
        // Placeholder unpublish logic
        Console.WriteLine($"Unpublishing {itemType} {name}");
    }
}
