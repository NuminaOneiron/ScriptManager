using ScriptManager.Enums;

namespace ScriptManager.Operations;

public interface IScriptOperations
{
    SortedSet<Script> Scripts { get; }

    ScriptCreator? ScriptCreator { get; set; }

    IPathInfo ScriptsLocation { get; }

    IScriptExecutionManager ChangeScriptsLocation(IPathInfo path);

    Script GetLatestScript();

    ScriptStatusType GetScriptStatus(int sequenceNumber);

    IScriptExecutionManager InsertScript(ScriptHistory script);

    bool ScriptExists(int sequenceNumber);

    IScriptExecutionManager UpdateScript(ScriptHistory script);

    IScriptExecutionManager UpsertScript(ScriptHistory script);

    IScriptExecutionManager BulkInsertScripts(IEnumerable<Script> scripts);

    IScriptExecutionManager BulkUpsertScripts(IEnumerable<Script> scripts);
}
