using ScriptManager.Enums;
using ScriptManager.Utilities;

namespace ScriptManager;

public interface ICommandTool
{
    CommandLineResult ExecuteScriptFile(ExecutionRunType executionType, in ConnectionStringInfo connectionString, IPathInfo file, in CancellationToken? cancelToken = null);

    CommandLineResult ExecuteScriptText(ExecutionRunType executionType, in ConnectionStringInfo connectionString, string scriptText, in CancellationToken? cancelToken = null);
}