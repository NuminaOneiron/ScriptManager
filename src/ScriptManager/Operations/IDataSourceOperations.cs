using ScriptManager.Enums;

namespace ScriptManager.Operations;

public interface IDataSourceOperations
{
    DataSourceType SourceType { get; }

    IPathInfo DataSource { get; }

    PathEnvironmentInfo DataSourceInfo { get; }

    IScriptExecutionManager CreateDataSource();

    IScriptExecutionManager CreateDataSourceBackup(int sequenceNumber);

    IScriptExecutionManager DeleteDataSource();

    IScriptExecutionManager DeleteDataSourceBackup(int? sequenceNumber = null);

    IScriptExecutionManager DeleteExistingDataSource();

    IScriptExecutionManager RestoreDataSourceBackup(int sequenceNumber);
}
