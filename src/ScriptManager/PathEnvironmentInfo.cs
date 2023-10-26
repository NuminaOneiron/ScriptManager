using ScriptManager.Enums;

namespace ScriptManager;

public readonly struct PathEnvironmentInfo
{
    public required readonly ServerEnvironmentType Environment { get; init; }

    public required readonly string? ContainerName { get; init; }

    public required readonly string? LocalPath { get; init; }

    public required readonly string? ServerPath { get; init; }

    public PathEnvironmentInfo()
    {
    }
}