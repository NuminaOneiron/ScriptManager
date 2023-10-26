using ScriptManager.Enums;
using ScriptManager.Utilities;

namespace ScriptManager.Extensions;

internal static class ScriptExecutionExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static void ExecuteScript(this ScriptExecutionManager executionManager, ref Script script, ILogger logger)
    {
        if (script.ExceptionErrors is null) script.ExceptionErrors = new List<string>();
        else script.ExceptionErrors.Clear();

        string scriptText = script.File.ReadAllText();

        scriptText.ReplaceChar(Constants.NonUnicode, char.MinValue);

        script!.Status = ScriptExecutors.ExecuteScriptText(executionManager.DbConnection, executionManager.DatabaseType, scriptText, script.ExceptionErrors!, logger, executionManager.CancelToken!.Value);

        if (script.Status == ScriptStatusType.SUCCESS || executionManager.CancelToken?.IsCancellationRequested is true) return;

        logger.LogScriptExecutionFailure(1, script.SequenceNumber, script.ExceptionErrors.Count);

        script.Status = ScriptExecutors.ExecuteScriptFile(script.ExecutionType, executionManager.ConnectionString, executionManager.DbCommandTool, script.File!, script.ExceptionErrors!, logger!, executionManager.CancelToken!.Value);

        if (script.Status == ScriptStatusType.SUCCESS || executionManager.CancelToken?.IsCancellationRequested is true) return;

        logger.LogScriptExecutionFailure(2, script.SequenceNumber, script.ExceptionErrors.Count);
    }
}