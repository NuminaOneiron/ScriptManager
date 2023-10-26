using ScriptManager.Enums;

namespace ScriptManager;

public readonly ref struct ScriptExecutionResults
{
    public readonly int AlreadyRanScripts { get; }

    public readonly int SuccessfulScripts { get; }

    public readonly int SuccessfulExecutedScripts { get; }

    public readonly int FailedScripts { get; }

    public readonly int IgnoredScripts { get; }

    public readonly int UnhandledScripts { get; }

    public readonly int TotalScripts { get; }

    public readonly int TotalExecutedScripts { get; }

    public ScriptExecutionResults(IEnumerable<Script> scripts)
    {
        if (scripts is null) return;

        TotalScripts = scripts.Count();

        TotalExecutedScripts = scripts.Count(static x => x.IsAlreadyRan is false);

        AlreadyRanScripts = scripts.Count(static x => x.IsAlreadyRan);

        SuccessfulScripts = scripts.Count(static x => x.Status is ScriptStatusType.SUCCESS);

        SuccessfulExecutedScripts = scripts.Count(static x => x.IsAlreadyRan is false && x.Status is ScriptStatusType.SUCCESS);

        FailedScripts = scripts.Count(static x => x.Status is ScriptStatusType.FAIL);

        IgnoredScripts = scripts.Count(static x => x.Status is ScriptStatusType.IGNORE);

        UnhandledScripts = scripts.Count(static x => x.Status is ScriptStatusType.NONE);
    }
}
