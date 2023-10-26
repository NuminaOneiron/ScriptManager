namespace ScriptManager.Extensions;

internal static partial class LoggerExtensions
{
    [LoggerMessage(EventId = 0, Level = LogLevel.Information, Message = "[{script}]")]
    public static partial void LogScript(this ILogger logger, Script script);

    [LoggerMessage(EventId = 1, Level = LogLevel.Error, Message = "Phase {phase} failed to execute script [{sequence}] with {errors} error(s).")]
    public static partial void LogScriptExecutionFailure(this ILogger logger, int phase, int sequence, int errors);

    [LoggerMessage(EventId = 2)]
    public static partial void LogException(this ILogger logger, LogLevel level, Exception exception);

    [LoggerMessage(EventId = 3, Message = "{message}")]
    public static partial void LogException(this ILogger logger, LogLevel level, string message, Exception exception);


    [LoggerMessage(EventId = 4, Message = "{message}")]
    public static partial void LogMessage(this ILogger logger, LogLevel level, string message, Exception? exception = null);

    [LoggerMessage(EventId = 5, Message = "{message} [Execution Time: {time}]")]
    public static partial void LogMessage(this ILogger logger, LogLevel level, string message, string time, Exception? exception = null);

    [LoggerMessage(EventId = 6, Message = "{message} {obj} [Execution Time: {time}]")]
    public static partial void LogMessage(this ILogger logger, LogLevel level, string message, object obj, string time, Exception? exception = null);


    [LoggerMessage(EventId = 7, Message = "{dataSourceName} {dataSourceAction} in {database} database as {dataSourceType} data source.")]
    public static partial void LogDataSource(this ILogger logger, LogLevel level, string dataSourceName, string dataSourceAction, string database, string dataSourceType, Exception? exception = null);

    [LoggerMessage(EventId = 8, Message = "{dataSourceName} {dataSourceAction} in {database} database as {dataSourceType} data source. [Execution Time: {time}]")]
    public static partial void LogDataSource(this ILogger logger, LogLevel level, string dataSourceName, string dataSourceAction, string database, string dataSourceType, string time, Exception? exception = null);


    [LoggerMessage(EventId = 9, Message = "{action} \"{path}\"")]
    public static partial void LogPath(this ILogger logger, LogLevel level, string action, string path, Exception? exception = null);

    [LoggerMessage(EventId = 10, Message = "{action} \"{path}\" [Execution Time: {time}]")]
    public static partial void LogPath(this ILogger logger, LogLevel level, string action, string path, string time, Exception? exception = null);


    [LoggerMessage(EventId = 11, Message = "Backup {backupAction} for {database} database at sequence number {sequence}.")]
    public static partial void LogBackup(this ILogger logger, LogLevel level, string backupAction, string database, int? sequence, Exception? exception = null);

    [LoggerMessage(EventId = 12, Message = "Backup {backupAction} for {database} database at sequence number {sequence}. [Execution Time: {time}]")]
    public static partial void LogBackup(this ILogger logger, LogLevel level, string backupAction, string database, int? sequence, string time);


    [LoggerMessage(EventId = 13, Message = "Snapshot {snapshotAction} for {database} database at sequence number {sequence}")]
    public static partial void LogSnapshot(this ILogger logger, LogLevel level, string snapshotAction, string database, int? sequence, Exception? exception = null);

    [LoggerMessage(EventId = 14, Message = "Snapshot {snapshotAction} for {database} database at sequence number {sequence}. [Execution Time: {time}]")]
    public static partial void LogSnapshot(this ILogger logger, LogLevel level, string snapshotAction, string database, int? sequence, string time);
}