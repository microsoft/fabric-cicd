using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Core;
using System.Linq;
using FabricCicd.Common;

namespace FabricCicd;

/// <summary>
/// Simplified representation of FabricWorkspace from the Python library.
/// </summary>
public class FabricWorkspace
{
    public string WorkspaceId { get; }
    public string RepositoryDirectory { get; }
    public IList<string> ItemTypes { get; }
    public FabricEndpoint Endpoint { get; }
    public IDictionary<string, Dictionary<string, string>> DeployedItems { get; } = new Dictionary<string, Dictionary<string, string>>();
    public IDictionary<string, Dictionary<string, Item>> RepositoryItems { get; } = new Dictionary<string, Dictionary<string, Item>>();

    public FabricWorkspace(string workspaceId, string repositoryDirectory, IEnumerable<string> itemTypes, TokenCredential? credential = null)
    {
        WorkspaceId = workspaceId ?? throw new ArgumentNullException(nameof(workspaceId));
        RepositoryDirectory = repositoryDirectory ?? throw new ArgumentNullException(nameof(repositoryDirectory));
        ItemTypes = itemTypes?.ToList() ?? throw new ArgumentNullException(nameof(itemTypes));
        Endpoint = new FabricEndpoint(credential);
    }

    // TODO: Add full implementation of methods from fabric_workspace.py
    public void RefreshRepositoryItems()
        => RefreshRepositoryItemsAsync().GetAwaiter().GetResult();

    public async Task RefreshRepositoryItemsAsync()
    {
        RepositoryItems.Clear();

        if (!Directory.Exists(RepositoryDirectory))
        {
            return;
        }

        foreach (var platformFile in Directory.EnumerateFiles(RepositoryDirectory, ".platform", SearchOption.AllDirectories))
        {
            var directory = Path.GetDirectoryName(platformFile)!;

            // skip empty directories
            if (!Directory.EnumerateFileSystemEntries(directory).Any())
            {
                Console.WriteLine($"Directory {Path.GetFileName(directory)} is empty.");
                continue;
            }

            JsonDocument metadata;
            try
            {
                var json = await File.ReadAllTextAsync(platformFile).ConfigureAwait(false);
                metadata = JsonDocument.Parse(json);
            }
            catch (Exception)
            {
                continue;
            }

            if (!metadata.RootElement.TryGetProperty("metadata", out var metaElem) ||
                !metadata.RootElement.TryGetProperty("config", out var configElem))
            {
                continue;
            }

            if (!metaElem.TryGetProperty("type", out var typeElem) ||
                !metaElem.TryGetProperty("displayName", out var nameElem))
            {
                continue;
            }

            var itemType = typeElem.GetString() ?? string.Empty;
            var itemName = nameElem.GetString() ?? string.Empty;
            var description = metaElem.GetProperty("description").GetString() ?? string.Empty;
            var logicalId = configElem.GetProperty("logicalId").GetString() ?? string.Empty;

            var id = DeployedItems.ContainsKey(itemType) && DeployedItems[itemType].ContainsKey(itemName)
                ? DeployedItems[itemType][itemName]
                : string.Empty;

            if (!RepositoryItems.ContainsKey(itemType))
            {
                RepositoryItems[itemType] = new Dictionary<string, Item>();
            }

            var item = new Item(itemType, itemName, description, id, logicalId, directory);
            item.CollectItemFiles();
            RepositoryItems[itemType][itemName] = item;
        }
    }

    public void RefreshDeployedItems()
        => RefreshDeployedItemsAsync().GetAwaiter().GetResult();

    public async Task RefreshDeployedItemsAsync()
    {
        var url = $"{Constants.DefaultApiRootUrl}/v1/workspaces/{WorkspaceId}/items";
        var json = await Endpoint.InvokeAsync(HttpMethod.Get, url).ConfigureAwait(false);

        DeployedItems.Clear();
        if (json.RootElement.TryGetProperty("value", out var items))
        {
            foreach (var item in items.EnumerateArray())
            {
                var type = item.GetProperty("type").GetString() ?? string.Empty;
                var name = item.GetProperty("displayName").GetString() ?? string.Empty;
                var id = item.GetProperty("id").GetString() ?? string.Empty;

                if (!DeployedItems.ContainsKey(type))
                {
                    DeployedItems[type] = new Dictionary<string, string>();
                }
                DeployedItems[type][name] = id;
            }
        }
    }

    public async Task PublishItemAsync(string name, string itemType, object definition)
    {
        var url = $"{Constants.DefaultApiRootUrl}/v1/workspaces/{WorkspaceId}/items";
        var body = new { type = itemType, displayName = name, config = definition };
        await Endpoint.InvokeAsync(HttpMethod.Post, url, body).ConfigureAwait(false);
    }

    public async Task UnpublishItemAsync(string id)
    {
        var url = $"{Constants.DefaultApiRootUrl}/v1/workspaces/{WorkspaceId}/items/{id}";
        await Endpoint.InvokeAsync(HttpMethod.Delete, url).ConfigureAwait(false);
    }

    // Convenience synchronous wrappers
    public void PublishItem(string name, string itemType)
        => PublishItemAsync(name, itemType, new { }).GetAwaiter().GetResult();

    public void PublishItem(string name, string itemType, object definition)
        => PublishItemAsync(name, itemType, definition).GetAwaiter().GetResult();

    public void UnpublishItem(string id)
        => UnpublishItemAsync(id).GetAwaiter().GetResult();
}
