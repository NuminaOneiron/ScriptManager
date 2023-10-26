using ScriptManager.Operations;

namespace ScriptManager;

public interface IScriptExecutionManager : IDisposable, IExecutionOperations, IConnectionOperations, IDataSourceOperations, IScriptOperations, IDatabaseOperations
{
    public ILogger Logger { get; init; }
}
