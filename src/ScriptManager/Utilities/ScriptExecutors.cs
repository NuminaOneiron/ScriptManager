using System.Data;
using System.Text.RegularExpressions;

using Dapper;

using ScriptManager.Enums;
using ScriptManager.Extensions;

namespace ScriptManager.Utilities;

internal static partial class ScriptExecutors
{
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static ScriptStatusType ExecuteScriptText(IDbConnection connection, DataProviderType databaseType, string scriptText, List<string> errors, ILogger logger, CancellationToken cancelToken)
    {
        errors?.Clear();

        int errorCount = 0;

        try
        {
            if (connection?.State is not ConnectionState.Open) connection?.Open();
        }
        catch (Exception ex)
        {
            logger?.LogException(LogLevel.Warning, "Failed to open database connection.".AsCached(), ex);
            return ScriptStatusType.FAIL;
        }

        Span<string> scriptCommands = GetScriptCommands(databaseType, scriptText);

        using IDbTransaction transaction = connection!.BeginTransaction();

        for (int i = 0; i < scriptCommands.Length; i++)
        {
            if (cancelToken.IsCancellationRequested is true) break;

            ref string text = ref scriptCommands[i];

            if (string.IsNullOrWhiteSpace(text) is false || string.IsNullOrEmpty(text) is false)
            {
                try
                {
                    _ = connection.Execute(text, transaction: transaction, commandTimeout: 0, commandType: CommandType.Text);
                }
                catch (Exception ex)
                {
                    errorCount++;
                    errors?.Add(ex.Message);
                    logger?.LogException(LogLevel.Warning, "Failed to execute command.".AsCached(), ex);
                }
            }
        }

        if (errorCount is 0)
        {
            transaction.Commit();
        }
        else
        {
            transaction.Rollback();
        }

        transaction.Dispose();

        return errorCount > 0 ? ScriptStatusType.FAIL : ScriptStatusType.SUCCESS;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static ScriptStatusType ExecuteScriptFile(ExecutionRunType executionType, in ConnectionStringInfo connectionString, ICommandTool commandTool, IPathInfo file, List<string> errors, ILogger logger, CancellationToken cancelToken)
    {
        errors?.Clear();

        using CommandLineResult result = commandTool.ExecuteScriptFile(executionType, in connectionString, file, cancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            foreach (string? error in result.ErrorOutput)
            {
                errors?.Add(error!);
                logger?.LogException(LogLevel.Warning, new Exception(error));
            }

            return ScriptStatusType.FAIL;
        }

        return ScriptStatusType.SUCCESS;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Span<string> GetScriptCommands(DataProviderType databaseType, string scriptText)
    {
        return databaseType switch
        {
            DataProviderType.MSSQLServer => MSSQLBatchSeparator().Split(scriptText).AsSpan(),
            DataProviderType.SQLLite => throw new NotImplementedException("Support for SQLite is not yet available."),
            _ => default!
        };
    }

    [GeneratedRegex("(?i)\\bgo\\b", RegexOptions.Multiline | RegexOptions.Compiled)]
    private static partial Regex MSSQLBatchSeparator();
}