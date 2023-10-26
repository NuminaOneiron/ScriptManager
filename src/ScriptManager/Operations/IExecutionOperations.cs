using ScriptManager.Enums;

namespace ScriptManager.Operations;

public interface IExecutionOperations
{
    ExecutionThreadType ThreadType { get; }

    ExecutionRunType ExecutionType { get; }

    ExecutionOptimizationType OptimizationType { get; }

    IProgress<ExecutionProgress> ExecutionProgress { get; set; }

    event EventHandler<Script>? OnExecutionEnded;

    event EventHandler<Script>? OnExecutionStarted;

    event EventHandler<Script>? OnScanEnded;

    event EventHandler<Script>? OnScanStarted;

    int LastExecutedSequence { get; }

    IScriptExecutionManager CancelExecution();

    IScriptExecutionManager ChangeExecutionRunType(ExecutionRunType executionType);

    static abstract IScriptExecutionManager Create(in ExecutionConfiguration config, ILoggerFactory logger, in CancellationToken? cancelToken = null!);

    IScriptExecutionManager ExecutionResults(out ScriptExecutionResults executionReport);

    IScriptExecutionManager Initialize(int? sequenceNumber = null!, in ScriptCreator? scriptCreator = null!);

    IScriptExecutionManager QuickInitialize(int? sequenceNumber = null!, in ScriptCreator? scriptCreator = null!);

    IScriptExecutionManager Run(Script script, ExecutionRunType? executionType = null!);

    IScriptExecutionManager Run(ExecutionRunType? executionType = null!);

    IScriptExecutionManager RunOptimized(ExecutionRunType? executionType = null!, in ScriptCreator? scriptCreator = null!);

    IScriptExecutionManager Scan(in ScriptCreator? scriptCreator = null!);
}
