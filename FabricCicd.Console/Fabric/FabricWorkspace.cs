using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Core;

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

    public FabricWorkspace(string workspaceId, string repositoryDirectory, IEnumerable<string> itemTypes, TokenCredential? credential = null)
    {
        WorkspaceId = workspaceId ?? throw new ArgumentNullException(nameof(workspaceId));
        RepositoryDirectory = repositoryDirectory ?? throw new ArgumentNullException(nameof(repositoryDirectory));
        ItemTypes = itemTypes?.ToList() ?? throw new ArgumentNullException(nameof(itemTypes));
        Endpoint = new FabricEndpoint(credential);
    }

    // TODO: Add full implementation of methods from fabric_workspace.py
    public void RefreshRepositoryItems()
    {
        // Placeholder for scanning repo directory
    }

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
