namespace FabricCicd;

/// <summary>
/// Mirrors values from constants.py.
/// </summary>
public static class Constants
{
    public const string Version = "0.1.19";
    public const string DefaultWorkspaceId = "00000000-0000-0000-0000-000000000000";
    public const string DefaultApiRootUrl = "https://api.powerbi.com";
    public static readonly HashSet<string> FeatureFlag = new();

    public static readonly string[] AcceptedItemTypesUpn =
    {
        "DataPipeline",
        "Environment",
        "Notebook",
        "Report",
        "SemanticModel",
        "Lakehouse",
        "MirroredDatabase",
        "VariableLibrary",
        "CopyJob",
        "Eventhouse",
        "KQLDatabase",
        "KQLQueryset",
        "Reflex",
        "Eventstream",
        "Warehouse",
        "SQLDatabase",
    };

    public static readonly IReadOnlyDictionary<string, int> MaxRetryOverride = new Dictionary<string, int>
    {
        ["SemanticModel"] = 10,
        ["Report"] = 10,
        ["Eventstream"] = 10,
        ["KQLDatabase"] = 10,
        ["VariableLibrary"] = 7,
        ["SQLDatabase"] = 7,
    };

    public static readonly string[] ShellOnlyPublish =
        { "Environment", "Lakehouse", "Warehouse", "SQLDatabase" };

    public static readonly string UserAgent = $"ms-fabric-cicd/{Version}";
}
