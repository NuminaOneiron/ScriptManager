using System.Text;
using System.Text.RegularExpressions;

using ScriptManager.Enums;
using ScriptManager.Extensions;
using ScriptManager.Utilities;

namespace ScriptManager.CommandLineTools;

public readonly partial struct SqlCmd : ICommandTool
{
    private const string MSG = "Msg";

    public const string EXE = "sqlcmd.exe";

    private const string CHANGED_DATABASE_CONTEXT = "Changed database context to";

    private static readonly char[] _splitCharacters = new char[2] { Constants.Return, Constants.Newline };

    public SqlCmd()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public readonly CommandLineResult ExecuteScriptText(ExecutionRunType executionType, in ConnectionStringInfo connectionString, string scriptText, in CancellationToken? cancelToken = null)
    {
        if (executionType is ExecutionRunType.TestRun)
        {
            scriptText = GetTransactionQuery(executionType, connectionString.Database, scriptText);
        }

        StringBuilder arguments = StringBuilderCache.Acquire();
        if (string.IsNullOrEmpty(connectionString.Username) is false)
        {
            _ = arguments
            .AppendCached("-S ")
            .AppendCached(connectionString.DataSource)

            .AppendCached(" -d ")
            .AppendCached(connectionString.Database)

            .AppendCached(" -U ")
            .Append(connectionString.Username)

            .AppendCached(" -P ")
            .AppendCached(connectionString.Password)

            .AppendCached(" -Q ")
            .Append(Constants.DoubleQuotes)
            .AppendCached(scriptText)
            .Append(Constants.DoubleQuotes)
            .AppendCached(" -h -1 -b");
        }
        else
        {
            _ = arguments
            .AppendCached("-S ")
            .AppendCached(connectionString.DataSource)

            .AppendCached(" -d ")
            .AppendCached(connectionString.Database)

            .AppendCached(" -Q ")
            .Append(Constants.DoubleQuotes)
            .AppendCached(scriptText)
            .Append(Constants.DoubleQuotes)
            .AppendCached(" -h -1 -b");
        }

        CommandLineResult result = CommandLineExecutors.RunProcess(EXE, arguments.ToString(), cancelToken is not null ? cancelToken.Value : CancellationToken.None);

        FormatOutput(result);

        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public readonly CommandLineResult ExecuteScriptFile(ExecutionRunType executionType, in ConnectionStringInfo connectionString, IPathInfo file, in CancellationToken? cancelToken = null)
    {
        CommandLineResult result;

        StringBuilder arguments = StringBuilderCache.Acquire();

        if (executionType is ExecutionRunType.TestRun)
        {
            string scriptText = GetTransactionQuery(executionType, connectionString.Database, $":r {file.FullPath}");

            FileInfo tempFile = new FileInfo(Path.Combine(Path.GetTempPath(), file.GetFileName(true)));

            File.WriteAllText(tempFile.FullName, scriptText);

            if (string.IsNullOrEmpty(connectionString.Username) is false)
            {
                _ = arguments
                .AppendCached("-S ")
                .AppendCached(connectionString.DataSource)

                .AppendCached(" -d ")
                .AppendCached(connectionString.Database)

                .AppendCached(" -U ")
                .AppendCached(connectionString.Username)

                .AppendCached(" -P ")
                .AppendCached(connectionString.Password)

                .AppendCached(" -i ")
                .Append(Constants.DoubleQuotes)
                .AppendCached(tempFile.FullName)
                .Append(Constants.DoubleQuotes)
                .AppendCached(" -h -1 -b");
            }
            else
            {
                _ = arguments
                .AppendCached("-S ")
                .AppendCached(connectionString.DataSource)

                .AppendCached(" -d ")
                .AppendCached(connectionString.Database)

                .AppendCached(" -i ")
                .Append(Constants.DoubleQuotes)
                .AppendCached(tempFile.FullName)
                .Append(Constants.DoubleQuotes)
                .AppendCached(" -h -1 -b");
            }

            try
            {
                result = CommandLineExecutors.RunProcess(EXE, arguments.ToString(), cancelToken is not null ? cancelToken.Value : CancellationToken.None);
            }
            finally
            {
                tempFile.Delete();
            }
        }
        else
        {
            if (string.IsNullOrEmpty(connectionString.Username) is false)
            {
                _ = arguments
                .AppendCached("-S ")
                .AppendCached(connectionString.DataSource)

                .AppendCached(" -d ")
                .AppendCached(connectionString.Database)

                .AppendCached(" -U ")
                .AppendCached(connectionString.Username)

                .AppendCached(" -P ")
                .AppendCached(connectionString.Password)

                .AppendCached(" -i ")
                .Append(Constants.DoubleQuotes)
                .AppendCached(file.FullPath)
                .Append(Constants.DoubleQuotes)
                .AppendCached(" -h -1 -b");
            }
            else
            {
                _ = arguments
                .AppendCached("-S ")
                .AppendCached(connectionString.DataSource)

                .AppendCached(" -d ")
                .AppendCached(connectionString.Database)

                .AppendCached(" -i ")
                .Append(Constants.DoubleQuotes)
                .AppendCached(file.FullPath)
                .Append(Constants.DoubleQuotes)
                .AppendCached(" -h -1 -b");
            }

            result = CommandLineExecutors.RunProcess(EXE, arguments, cancelToken is not null ? cancelToken.Value : CancellationToken.None);
        }

        FormatOutput(result);

        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void FormatOutput(CommandLineResult result)
    {
        if (result.StandardOutput?.FirstOrDefault()!.Contains(CHANGED_DATABASE_CONTEXT) is true)
        {
            result.StandardOutput = RemoveChangedDatabasePattern().Replace(result.StandardOutput!, string.Empty).Trim();
        }

        if (result.StandardOutput?.Any(static x => x!.Contains(MSG)) is true)
        {
            StringTokenizer entries = new StringTokenizer(result.StandardOutput!, _splitCharacters);

            result.StandardOutput = string.Empty;

            if (entries.TryGetNonEnumeratedCount(out int count) is false) count = entries.Count();

            for (int i = 0; i < count; i++)
            {
                StringSegment entry = entries.ElementAt(i);

                if (entry.StartsWith(MSG, StringComparison.Ordinal))
                {
                    for (int c = i + 1; c < count; c++)
                    {
                        StringSegment message = entries.ElementAt(c);

                        if (StringSegment.IsNullOrEmpty(message) is false)
                        {
                            result.AppendErrorOutput(string.Concat(entry.Value.AsSpan(), Environment.NewLine.AsSpan(), message.Value.AsSpan()));
                            i = c;
                            break;
                        }
                    }
                }
                else if (StringSegment.IsNullOrEmpty(entry) is false)
                {
                    result.AppendStandardOutput(entry.Value!);
                }
            }

            result.ConsolidateErrorOutput();

            if (StringValues.IsNullOrEmpty(result.StandardOutput!.Value) is false)
            {
                result.ConsolidateStandardOutput();
            }
            else
            {
                result.StandardOutput = null;
            }
        }
    }

    private static string GetTransactionQuery(ExecutionRunType executionType, string databaseName, ReadOnlySpan<char> scriptText)
    {
        StringBuilder strBuilder = StringBuilderCache.Acquire();

        _ = strBuilder.AppendLine("SET NOCOUNT ON;");
        _ = strBuilder.Append("USE [").Append(databaseName).AppendLine("];");
        _ = strBuilder.AppendLine("SET XACT_ABORT ON;\n");

        _ = strBuilder.AppendLine("BEGIN TRY\n");
        _ = strBuilder.AppendLine("BEGIN TRANSACTION");

        _ = strBuilder.AppendLine();
        _ = strBuilder.Append(scriptText);
        _ = strBuilder.AppendLine();

        if (executionType is ExecutionRunType.TestRun)
        {
            _ = strBuilder.AppendLine("ROLLBACK TRANSACTION\n");
        }
        else
        {
            _ = strBuilder.AppendLine("COMMIT TRANSACTION\n");
        }

        _ = strBuilder.AppendLine("SELECT 'SUCCESS';\n");

        _ = strBuilder.AppendLine("END TRY");
        _ = strBuilder.AppendLine("BEGIN CATCH\n");
        _ = strBuilder.AppendLine("ROLLBACK TRANSACTION\n");
        _ = strBuilder.AppendLine("DECLARE @ErrorMessage NVARCHAR(4000);");
        _ = strBuilder.AppendLine("DECLARE @ErrorSeverity INT;");
        _ = strBuilder.AppendLine("DECLARE @ErrorState INT;\n");
        _ = strBuilder.AppendLine("SELECT @ErrorMessage = CONCAT('FAIL: ',' ', ERROR_MESSAGE()), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();\n");
        _ = strBuilder.AppendLine("RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);\n");
        _ = strBuilder.AppendLine("END CATCH");

        return strBuilder.ToString();
    }

    [GeneratedRegex($"{CHANGED_DATABASE_CONTEXT} '[^']*'.", RegexOptions.Compiled)]
    private static partial Regex RemoveChangedDatabasePattern();
}