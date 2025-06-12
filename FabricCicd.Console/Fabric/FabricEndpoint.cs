using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;

namespace FabricCicd;

/// <summary>
/// Handles authentication and HTTP interactions with the Fabric API.
/// This is a minimal counterpart to the Python FabricEndpoint class.
/// </summary>
public class FabricEndpoint
{
    private readonly TokenCredential _credential;
    private readonly HttpClient _client;
    private string? _token;
    private DateTimeOffset _tokenExpires = DateTimeOffset.MinValue;

    public FabricEndpoint(TokenCredential? credential = null, HttpClient? client = null)
    {
        _credential = credential ?? new DefaultAzureCredential();
        _client = client ?? new HttpClient();
    }

    private async Task RefreshTokenAsync()
    {
        if (_token == null || _tokenExpires < DateTimeOffset.UtcNow)
        {
            var ctx = await _credential.GetTokenAsync(new TokenRequestContext(new[] { "https://api.fabric.microsoft.com/.default" }));
            _token = ctx.Token;
            _tokenExpires = ctx.ExpiresOn;
        }
    }

    /// <summary>
    /// Invokes an HTTP request against the Fabric API and returns the JSON body.
    /// </summary>
    public async Task<JsonDocument> InvokeAsync(HttpMethod method, string url, object? body = null)
    {
        await RefreshTokenAsync().ConfigureAwait(false);

        using var request = new HttpRequestMessage(method, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
        request.Headers.UserAgent.ParseAdd(Constants.UserAgent);

        if (body != null)
        {
            var json = JsonSerializer.Serialize(body);
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");
        }

        using var response = await _client.SendAsync(request).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
        return await JsonDocument.ParseAsync(stream).ConfigureAwait(false);
    }
}
