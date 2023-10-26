using ScriptManager.Enums;

namespace ScriptManager;

public readonly ref struct ExecutionConfiguration
{
    public required DataProviderType DatabaseType { get; init; }

    public required ExecutionRunType ExecutionType { get; init; }

    public required DataSourceType SourceType { get; init; }

    public required ServerEnvironmentType EnvironmentType { get; init; }

    public required ExecutionOptimizationType OptimizationType { get; init; }

    public required ExecutionThreadType ThreadType { get; init; }

    public int? SequenceNumber { get; init; }

    public required string Database { get; init; }

    public required string Server { get; init; }

    public required string ScriptExtension { get; init; }

    public string? Username { get; init; }

    public string? Password { get; init; }

    public required PathEnvironmentInfo DataSourceInfo { get; init; }

    public required PathEnvironmentInfo ScriptsLocationInfo { get; init; }

    public ExecutionConfiguration()
    {
    }
}