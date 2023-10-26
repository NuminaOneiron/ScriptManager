using System.Data;

namespace ScriptManager.Operations;

public interface IConnectionOperations
{
    ConnectionStringInfo ConnectionString { get; }

    IDbConnection DbConnection { get; }

    ICommandTool DbCommandTool { get; }

    IScriptExecutionManager ChangeConnectionString(in ExecutionConfiguration config);
}
