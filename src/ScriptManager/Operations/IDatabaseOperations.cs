using ScriptManager.Enums;

namespace ScriptManager.Operations;

public interface IDatabaseOperations
{
    ServerEnvironmentType EnvironmentType { get; }

    DataProviderType DatabaseType { get; }

    IPathInfo DatabaseLocation { get; }

    IScriptExecutionManager CreateDatabaseSnapshot(int sequenceNumber);

    IScriptExecutionManager DeleteDatabaseSnapshot(int? sequenceNumber = null);

    IScriptExecutionManager RestoreDatabaseBackup(string backupFilePath);

    IScriptExecutionManager RestoreDatabaseSnapshot(int sequenceNumber);
}
