using System.Data;
using System.Diagnostics;
using System.Text;

using Dapper;

using Humanizer;

using Microsoft.Data.SqlClient;

using ScriptManager.CommandLineTools;
using ScriptManager.Enums;
using ScriptManager.Extensions;
using ScriptManager.Utilities;

namespace ScriptManager.Managers;

public sealed class MSSQLManager : ScriptExecutionManager
{
    public override ICommandTool DbCommandTool { get; internal set; }


    internal MSSQLManager(in ExecutionConfiguration config, ILoggerFactory logger, in CancellationToken? cancelToken = null!) : base(in config, logger, in cancelToken)
    {
        DbCommandTool = new SqlCmd();

        DatabaseLocation = GetDatabasePath();
    }


    internal override int GetLatestSequence()
    {
        int sequence;

        StringBuilder latestSequenceQuery = StringBuilderCache.Acquire();
        _ = latestSequenceQuery.AppendCached("SET NOCOUNT ON;");
        _ = latestSequenceQuery.AppendCached($"USE [{ConnectionString.Database}];");
        _ = latestSequenceQuery.AppendLineCached($"SELECT TOP 1 [{nameof(ScriptHistory.SequenceNumber)}] FROM [{nameof(ScriptHistory)}] ORDER BY [{nameof(ScriptHistory.SequenceNumber)}] DESC");

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, latestSequenceQuery.ToString(), in CancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            Exception exception = new Exception(result.ErrorOutput);
            Logger?.LogException(LogLevel.Warning, "Failed to get latest script sequence number.".AsInterned(), exception);

            long timestamp = Stopwatch.GetTimestamp();
            try
            {
                DbConnection = CreateDbConnection();
                sequence = (int)DbConnection?.ExecuteScalar<int>(latestSequenceQuery.ToString())!;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                DbConnection?.Dispose();
            }
        }

        _ = int.TryParse(result.StandardOutput?.FirstOrDefault(), out sequence);

        if (LastExecutedSequence > sequence)
        {
            Logger?.LogMessage(LogLevel.Information, "Latest executed script sequence number is".AsInterned(), LastExecutedSequence, result.ExecutionTime!.Value.Humanize());
            return LastExecutedSequence;
        }

        Logger?.LogMessage(LogLevel.Information, "Latest executed script sequence number is".AsInterned(), sequence, result.ExecutionTime!.Value.Humanize());
        return sequence!;
    }


    internal override ConnectionStringInfo CreateConnectionString(in ExecutionConfiguration config)
    {
        string connectionString = default!;

        if (string.IsNullOrEmpty(config.Username) is false && string.IsNullOrEmpty(config.Password) is false)
        {
            connectionString = $"Data Source={config.Server};Initial Catalog={config.Database};Persist Security Info=True;TrustServerCertificate=True;User ID={config.Username};Password={config.Password};ApplicationIntent=ReadWrite;Connect Timeout=0";
        }
        else if (config.Username![0] == Constants.Period)
        {
            connectionString = $"Data Source={config.Server};Initial Catalog={config.Database};Persist Security Info=True;TrustServerCertificate=True;Integrated Security=True;ApplicationIntent=ReadWrite;Connect Timeout=0";
        }

        SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder(connectionString);

        return new ConnectionStringInfo
        {
            ConnectionString = connectionString,
            Database = connectionBuilder.InitialCatalog,
            DataSource = connectionBuilder.DataSource,
            Password = connectionBuilder.Password,
            Username = connectionBuilder.UserID
        };
    }

    internal override IDbConnection CreateDbConnection()
    {
        if (string.IsNullOrEmpty(ConnectionString.ConnectionString))
        {
            NullReferenceException exception = new NullReferenceException($"Null reference exception of {nameof(ConnectionString.ConnectionString)}.");
            throw exception;
        }

        try
        {
            return new SqlConnection(ConnectionString.ConnectionString);
        }
        catch (Exception)
        {
            throw;
        }
    }


    internal override void BulkInsertTableScripts(IEnumerable<ScriptHistory> scripts)
    {
        if (DbConnection?.State is not ConnectionState.Open) DbConnection?.Open();

        if (scripts.TryGetNonEnumeratedCount(out int count) is false) count = scripts.Count();

        StringBuilder sb = new StringBuilder(count);
        _ = sb.AppendCached($"USE [{ConnectionString.Database}];");

        foreach (ScriptHistory script in scripts)
        {
            script.Validate(SourceType, DatabaseType);

            ReadOnlySpan<char> insertQuery =
            $"""

            IF NOT EXISTS (SELECT TOP 1 [{nameof(ScriptHistory.SequenceNumber)}] FROM [{nameof(ScriptHistory)}] WHERE [{nameof(ScriptHistory.SequenceNumber)}] = {script.SequenceNumber.AsCached()})
            BEGIN
                INSERT INTO [{nameof(ScriptHistory)}] ({nameof(ScriptHistory.SequenceNumber)}, {nameof(ScriptHistory.Author)}, {nameof(ScriptHistory.Description)}, {nameof(ScriptHistory.Status)}, {nameof(ScriptHistory.CreatedDate)}) 
                VALUES ({script.SequenceNumber.AsCached()}, '{script.Author}', '{script.Description}', '{script.Status.AsString()}', TRY_CONVERT(DATETIME2(0), '{script.CreatedDate.AsString()}'))
            END

            """;

            _ = sb.Append(insertQuery);
        }

        try
        {
            _ = TryOpenDBConnection();
            _ = DbConnection?.Execute(sb.ToString(), commandTimeout: 0);
        }
        catch (Exception ex)
        {
            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, sb.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                throw new Exception("Failed to bulk insert scripts.".AsInterned(), ex);
            }
        }
    }

    internal override void BulkUpsertTableScripts(IEnumerable<ScriptHistory> scripts)
    {
        if (scripts.TryGetNonEnumeratedCount(out int count) is false) count = scripts.Count();

        StringBuilder sb = new StringBuilder(count);
        _ = sb.AppendCached($"USE [{ConnectionString.Database}];");

        foreach (ScriptHistory script in scripts)
        {
            script.Validate(SourceType, DatabaseType);

            ReadOnlySpan<char> upsertQuery =
            $"""

            IF EXISTS (SELECT TOP 1 [{nameof(ScriptHistory.SequenceNumber)}] FROM [{nameof(ScriptHistory)}] WHERE [{nameof(ScriptHistory.SequenceNumber)}] = {script.SequenceNumber.AsString()})
            BEGIN
                UPDATE [{nameof(ScriptHistory)}] 
                SET [{nameof(ScriptHistory.Author)}] = '{script.Author}', 
                [{nameof(ScriptHistory.Description)}] = '{script.Description}',
                [{nameof(ScriptHistory.Status)}] = '{script.Status.AsString()}',
                [{nameof(ScriptHistory.CreatedDate)}] = TRY_CONVERT(DATETIME2(0), '{script.CreatedDate.AsString()}')
                WHERE [{nameof(ScriptHistory.SequenceNumber)}] = {script.SequenceNumber.AsString()}
            END
            ELSE
            BEGIN
                INSERT INTO [{nameof(ScriptHistory)}] ({nameof(ScriptHistory.SequenceNumber)}, {nameof(ScriptHistory.Author)}, {nameof(ScriptHistory.Description)}, {nameof(ScriptHistory.Status)}, {nameof(ScriptHistory.CreatedDate)}) 
                VALUES ({script.SequenceNumber.AsCached()}, '{script.Author}', '{script.Description}', '{script.Status.AsString()}', TRY_CONVERT(DATETIME2(0), '{script.CreatedDate.AsString()}'))
            END

            """;

            _ = sb.Append(upsertQuery);
        }

        try
        {
            _ = TryOpenDBConnection();
            _ = DbConnection?.Execute(sb.ToString(), commandTimeout: 0);
        }
        catch (Exception ex)
        {
            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, sb.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                throw new Exception("Failed to bulk upsert scripts.".AsInterned(), ex);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public override ScriptStatusType GetScriptStatus(int sequenceNumber)
    {
        StringBuilder scriptStatusQuery = StringBuilderCache.Acquire();
        _ = scriptStatusQuery.AppendCached($"USE [{ConnectionString.Database}];");
        _ = scriptStatusQuery.AppendLineCached($"SELECT TOP 1 [{nameof(ScriptHistory.Status)}] FROM [{nameof(ScriptHistory)}] WHERE [{nameof(ScriptHistory.SequenceNumber)}] = ");
        _ = scriptStatusQuery.AppendCached(sequenceNumber.AsString());

        try
        {
            _ = TryOpenDBConnection();
            ReadOnlySpan<char> result = DbConnection?.ExecuteScalar<string>(scriptStatusQuery.ToString());
            return result.IsEmpty || result.IsWhiteSpace() ? ScriptStatusType.NONE : Enum.Parse<ScriptStatusType>(result);
        }
        catch (Exception ex)
        {
            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, scriptStatusQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                throw new Exception("Failed to get script status.".AsInterned(), ex);
            }
            else if (result.StandardOutput.HasValue)
            {
                if (Enum.TryParse(result.StandardOutput.Value.FirstOrDefault().AsSpan(), out ScriptStatusType status))
                {
                    return status;
                }
            }
        }

        return ScriptStatusType.NONE;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal override void InsertTableScript(ScriptHistory script)
    {
        script.Validate(SourceType, DatabaseType);

        StringBuilder insertQuery = StringBuilderCache.Acquire();
        _ = insertQuery.AppendCached($"USE [{ConnectionString.Database}];");
        _ = insertQuery.AppendLineCached($"INSERT INTO [{nameof(ScriptHistory)}] ({nameof(ScriptHistory.SequenceNumber)}, {nameof(ScriptHistory.Author)}, {nameof(ScriptHistory.Description)}, {nameof(ScriptHistory.CreatedDate)}, {nameof(ScriptHistory.Status)})");
        _ = insertQuery.AppendLineCached($"VALUES (@{nameof(ScriptHistory.SequenceNumber)}, @{nameof(ScriptHistory.Author)}, @{nameof(ScriptHistory.Description)}, @{nameof(ScriptHistory.CreatedDate)}, @{nameof(ScriptHistory.Status)})");

        try
        {
            _ = TryOpenDBConnection();
            _ = DbConnection?.Execute(insertQuery.ToString(), new { script.SequenceNumber, script.Author, script.Description, script.CreatedDate, Status = script.Status.AsString() });
        }
        catch (Exception ex)
        {
            _ = insertQuery.Clear();
            _ = insertQuery.AppendCached($"USE [{ConnectionString.Database}];");
            _ = insertQuery.AppendLineCached($"INSERT INTO [{nameof(ScriptHistory)}] ({nameof(ScriptHistory.SequenceNumber)}, {nameof(ScriptHistory.Author)}, {nameof(ScriptHistory.Description)}, {nameof(ScriptHistory.CreatedDate)}, {nameof(ScriptHistory.Status)})");
            _ = insertQuery.AppendLineCached($"VALUES ({script.SequenceNumber.AsString()}, '{script.Author}', '{script.Description}', TRY_CONVERT(DATETIME2(0), '{script.CreatedDate}'), '{script.Status.AsString()}'");

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, insertQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                throw new Exception("Failed to insert script.".AsInterned(), ex);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public override bool ScriptExists(int sequenceNumber)
    {
        bool isExisting = false;

        StringBuilder scriptExistsQuery = StringBuilderCache.Acquire();
        _ = scriptExistsQuery.AppendCached($"USE [{ConnectionString.Database}];");
        _ = scriptExistsQuery.AppendLineCached($"SELECT TOP 1 [{nameof(ScriptHistory.SequenceNumber)}] FROM [{nameof(ScriptHistory)}] WHERE [{nameof(ScriptHistory.SequenceNumber)}] = ");
        _ = scriptExistsQuery.AppendCached(sequenceNumber.AsString());

        try
        {
            _ = TryOpenDBConnection();
            int? result = DbConnection?.ExecuteScalar<int>(scriptExistsQuery.ToString())!;
            if (result == sequenceNumber) isExisting = true;
        }
        catch (Exception ex)
        {
            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, scriptExistsQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                throw new Exception("Failed to find script.".AsInterned(), ex);
            }

            isExisting = int.TryParse(result.StandardOutput?[0], out int number) && number == sequenceNumber;
        }

        return isExisting;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal override void UpdateTableScript(ScriptHistory script)
    {
        StringBuilder updateQuery = StringBuilderCache.Acquire();
        _ = updateQuery.AppendCached($"USE [{ConnectionString.Database}];");
        _ = updateQuery.AppendLineCached($"UPDATE [{nameof(ScriptHistory)}]");
        _ = updateQuery.AppendLineCached($"SET [{nameof(ScriptHistory.Author)}] = @{nameof(ScriptHistory.Author)},");
        _ = updateQuery.AppendLineCached($"SET [{nameof(ScriptHistory.Description)}] = @{nameof(ScriptHistory.Description)},");
        _ = updateQuery.AppendLineCached($"SET [{nameof(ScriptHistory.CreatedDate)}] = @{nameof(ScriptHistory.CreatedDate)}");
        _ = updateQuery.AppendLineCached($"SET [{nameof(ScriptHistory.Status)}] = @{nameof(ScriptHistory.Status)},");
        _ = updateQuery.AppendLineCached($"WHERE [{nameof(ScriptHistory.SequenceNumber)}] = @{nameof(ScriptHistory.SequenceNumber)}");

        try
        {
            _ = TryOpenDBConnection();
            _ = DbConnection?.Execute(updateQuery.ToString(), new { script.SequenceNumber, script.Author, script.Description, script.CreatedDate, Status = script.Status.AsString() });
        }
        catch (Exception ex)
        {
            _ = updateQuery.Clear();
            _ = updateQuery.AppendCached($"USE [{ConnectionString.Database}];");
            _ = updateQuery.AppendLineCached($"UPDATE [{nameof(ScriptHistory)}]");
            _ = updateQuery.AppendLineCached($"SET [{nameof(ScriptHistory.Author)}] = '{script.Author}',");
            _ = updateQuery.AppendLineCached($"SET [{nameof(ScriptHistory.Description)}] = '{script.Description}',");
            _ = updateQuery.AppendLineCached($"SET [{nameof(ScriptHistory.CreatedDate)}] = TRY_CONVERT(DATETIME2(0), '{script.CreatedDate.AsString()}'),");
            _ = updateQuery.AppendLineCached($"SET [{nameof(ScriptHistory.Status)}] = '{script.Status.AsString()}',");
            _ = updateQuery.AppendLineCached($"WHERE [{nameof(ScriptHistory.SequenceNumber)}] = {script.SequenceNumber.AsString()}");

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, updateQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                throw new Exception("Failed to upsert script.".AsInterned(), ex);
            }
        }
    }


    internal override IPathInfo CreateCsvDataSource()
    {
        IPathInfo csvDataSource = CreateDataSourceFile();

        IPathInfo csvFormatFile = CreateCsvFormatFile();

        bool viewExists = DataSourceExists(SourceType);

        if (viewExists is true && csvDataSource.Exists && csvFormatFile.Exists)
        {
            Logger?.LogDataSource(LogLevel.Information, $"{nameof(ScriptHistory)} view".AsInterned(), "already exists".AsInterned(), ConnectionString.Database, "csv".AsInterned());
        }
        else
        {
            StringBuilder createViewQuery = StringBuilderCache.Acquire();
            _ = createViewQuery.Append(
            $"""
             {"SET NOCOUNT ON;".AsInterned()}
             {$"USE [{ConnectionString.Database}];".AsInterned()}
             GO
             CREATE VIEW {nameof(ScriptHistory)}
             AS
             SELECT * 
             FROM OPENROWSET(
                 BULK '{csvDataSource.FullPath}',
                 FORMATFILE='{csvFormatFile.FullPath}',
                 FIRSTROW=2
             ) AS CsvData;
             """.AsSpan());

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, createViewQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                Logger?.LogDataSource(LogLevel.Warning, $"{nameof(ScriptHistory)} view".AsInterned(), "failed to be created".AsInterned(), ConnectionString.Database, "csv".AsInterned());

                foreach (string? error in result.ErrorOutput)
                {
                    Logger?.LogException(LogLevel.Warning, new Exception(error));
                }

                long timestamp = Stopwatch.GetTimestamp();
                try
                {
                    DbConnection = CreateDbConnection();
                    _ = DbConnection?.Execute(createViewQuery.ToString());
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                    DbConnection?.Dispose();
                }
            }

            Logger?.LogDataSource(LogLevel.Information, $"{nameof(ScriptHistory)} view".AsInterned(), "created".AsInterned(), ConnectionString.Database, "csv".AsInterned(), result.ExecutionTime!.Value.Humanize());
        }

        return csvDataSource;
    }

    internal override IPathInfo CreateJsonDataSource()
    {
        IPathInfo jsonDataSource = CreateDataSourceFile();

        bool viewExists = DataSourceExists(SourceType);

        if (viewExists is true && jsonDataSource.Exists)
        {
            Logger?.LogDataSource(LogLevel.Information, $"{nameof(ScriptHistory)} view".AsInterned(), "already exists".AsInterned(), ConnectionString.Database, "json".AsInterned());
        }
        else
        {
            StringBuilder createViewQuery = StringBuilderCache.Acquire();
            _ = createViewQuery.Append(
            $"""
             {"SET NOCOUNT ON;".AsInterned()}
             {$"USE [{ConnectionString.Database}];".AsInterned()}

             IF EXISTS (SELECT name FROM sys.databases WHERE name = '{ConnectionString.Database}' AND compatibility_level <= 120)
             BEGIN
                 ALTER DATABASE {ConnectionString.Database} SET COMPATIBILITY_LEVEL = 130
             END
             GO
             CREATE VIEW [{nameof(ScriptHistory)}]
             AS
             SELECT [{nameof(ScriptHistory.SequenceNumber)}],[{nameof(ScriptHistory.Author)}],[{nameof(ScriptHistory.Description)}],[{nameof(ScriptHistory.Status)}],[{nameof(ScriptHistory.CreatedDate)}]
             FROM OPENROWSET(BULK '{jsonDataSource.FullPath}', SINGLE_CLOB) AS [jsonData]
             CROSS APPLY OPENJSON([jsonData].[BulkColumn])
             WITH 
             (
                [{nameof(ScriptHistory.SequenceNumber)}] INT '$.{nameof(ScriptHistory.SequenceNumber)}',
                [{nameof(ScriptHistory.Author)}] VARCHAR(30) '$.{nameof(ScriptHistory.Author)}',
                [{nameof(ScriptHistory.Description)}] NVARCHAR(50) '$.{nameof(ScriptHistory.Description)}',
                [{nameof(ScriptHistory.Status)}] VARCHAR(10) '$.{nameof(ScriptHistory.Status)}',
                [{nameof(ScriptHistory.CreatedDate)}] DATETIME2(0) '$.{nameof(ScriptHistory.CreatedDate)}'
             )
             """.AsSpan());

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, createViewQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                Logger?.LogDataSource(LogLevel.Warning, $"{nameof(ScriptHistory)} view".AsInterned(), "failed to be created".AsInterned(), ConnectionString.Database, "json".AsInterned());

                foreach (string? error in result.ErrorOutput)
                {
                    Logger?.LogException(LogLevel.Warning, new Exception(error));
                }

                long timestamp = Stopwatch.GetTimestamp();
                try
                {
                    DbConnection = CreateDbConnection();
                    _ = DbConnection?.Execute(createViewQuery.ToString());
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                    DbConnection?.Dispose();
                }
            }

            Logger?.LogDataSource(LogLevel.Information, $"{nameof(ScriptHistory)} view".AsInterned(), "created".AsInterned(), ConnectionString.Database, "csv".AsInterned(), result.ExecutionTime!.Value.Humanize());
        }

        return jsonDataSource;
    }

    internal override void CreateTableDataSource()
    {
        bool tableExists = DataSourceExists(SourceType);

        if (tableExists is false)
        {
            StringBuilder createTableQuery = StringBuilderCache.Acquire();
            _ = createTableQuery.Append(
            $"""
            {"SET NOCOUNT ON;".AsInterned()}
            {$"USE [{ConnectionString.Database}];".AsInterned()}

            IF OBJECT_ID('{nameof(ScriptHistory)}', 'U') IS NULL
            BEGIN
                CREATE TABLE {nameof(ScriptHistory)}
                (
                    [{nameof(ScriptHistory.SequenceNumber)}] INT NOT NULL PRIMARY KEY,
                    [{nameof(ScriptHistory.Author)}] VARCHAR(30) NULL,
                    [{nameof(ScriptHistory.Description)}] VARCHAR(50) NULL,
                    [{nameof(ScriptHistory.Status)}] VARCHAR(10) NOT NULL,
                    [{nameof(ScriptHistory.CreatedDate)}] DATETIME2(0) NULL,
                );
            END

            IF OBJECT_ID('{nameof(ScriptHistory)}', 'U') IS NOT NULL
            BEGIN
                PRINT 'true'
            END
            ELSE
            BEGIN
                PRINT 'false'
            END
            """.AsSpan());

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, createTableQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                Logger?.LogDataSource(LogLevel.Warning, $"{nameof(ScriptHistory)} table".AsInterned(), "failed to be created".AsInterned(), ConnectionString.Database, "internal".AsInterned());

                foreach (string? error in result.ErrorOutput)
                {
                    Logger?.LogException(LogLevel.Warning, new Exception(error));
                }
            }

            bool exists = result.StandardOutput?[0] is "true";

            if (exists is false)
            {
                long timestamp = Stopwatch.GetTimestamp();
                try
                {
                    DbConnection = CreateDbConnection();
                    exists = DbConnection?.ExecuteScalar<string>(createTableQuery.ToString()) is "true";
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                    DbConnection?.Dispose();
                }
            }

            if (exists)
            {
                Logger?.LogDataSource(LogLevel.Information, $"{nameof(ScriptHistory)} table".AsInterned(), "created".AsInterned(), ConnectionString.Database, "internal".AsInterned());
            }
        }
        else
        {
            Logger?.LogDataSource(LogLevel.Information, $"{nameof(ScriptHistory)} table".AsInterned(), "already exists".AsInterned(), ConnectionString.Database, "internal".AsInterned());
        }
    }

    private IPathInfo CreateCsvFormatFile()
    {
        string formatFilePath = DatabaseLocation.GetPathFromDirectory($"{nameof(ScriptHistory)}_CSVFormatFile.fmt".AsInterned());

        IPathInfo csvFormatFile = CreatePathInfo(formatFilePath);

        if (csvFormatFile.Exists is false)
        {
            const string formatText =
            $"""
            <?xml version="1.0"?>
            <BCPFORMAT xmlns="http://schemas.microsoft.com/sqlserver/2004/bulkload/format" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
             <RECORD>
              <FIELD ID="1" xsi:type="CharTerm" TERMINATOR="," MAX_LENGTH="12"/>
              <FIELD ID="2" xsi:type="CharTerm" TERMINATOR="," MAX_LENGTH="30"/>
              <FIELD ID="3" xsi:type="CharTerm" TERMINATOR="," MAX_LENGTH="50"/>
              <FIELD ID="4" xsi:type="CharTerm" TERMINATOR="," MAX_LENGTH="10"/>
              <FIELD ID="5" xsi:type="CharTerm" TERMINATOR="\n" MAX_LENGTH="30"/>
             </RECORD>
             <ROW>
              <COLUMN SOURCE="1" NAME="{nameof(ScriptHistory.SequenceNumber)}" xsi:type="SQLINT"/>
              <COLUMN SOURCE="2" NAME="{nameof(ScriptHistory.Author)}" xsi:type="SQLVARYCHAR"/>
              <COLUMN SOURCE="3" NAME="{nameof(ScriptHistory.Description)}" xsi:type="SQLVARYCHAR"/>
              <COLUMN SOURCE="4" NAME="{nameof(ScriptHistory.Status)}" xsi:type="SQLVARYCHAR"/>
              <COLUMN SOURCE="5" NAME="{nameof(ScriptHistory.CreatedDate)}" xsi:type="SQLDATETIME2" SCALE="0"/>
             </ROW>
            </BCPFORMAT>
            """;

            string tempPath = Path.Combine(Path.GetTempPath(), csvFormatFile.GetFileName(true));

            File.WriteAllText(Path.Combine(tempPath), formatText);

            csvFormatFile.CopyFrom(tempPath, true);

            File.Delete(tempPath);

            if (csvFormatFile.Exists)
            {
                Logger?.LogPath(LogLevel.Information, "Created csv format file at".AsInterned(), formatFilePath.AsInterned());
            }
            else
            {
                Logger?.LogPath(LogLevel.Warning, "Failed to create csv format file at".AsInterned(), formatFilePath.AsInterned());
            }
        }

        return csvFormatFile;
    }

    internal override bool DataSourceExists(DataSourceType sourceType)
    {
        string conditionText = sourceType switch
        {
            DataSourceType.Internal => $"IF OBJECT_ID('{nameof(ScriptHistory)}', 'U') IS NOT NULL".AsInterned(),
            DataSourceType.Json or DataSourceType.Csv => $"IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'{nameof(ScriptHistory)}'))".AsInterned(),
            _ => null!
        };

        StringBuilder checkScriptHistoryExistsQuery = StringBuilderCache.Acquire();
        _ = checkScriptHistoryExistsQuery.Append(
        $"""
        {"SET NOCOUNT ON;".AsInterned()}
        {$"USE [{ConnectionString.Database}];".AsInterned()}
        
        {conditionText}
        BEGIN
            PRINT 'true'
        END
        ELSE
        BEGIN
            PRINT 'false'
        END
        """.AsSpan());

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, checkScriptHistoryExistsQuery.ToString(), in CancelToken);

        bool exists = result.StandardOutput?[0] is "true";

        if (result.ErrorOutput?.Count > 0)
        {
            Logger?.LogMessage(LogLevel.Warning, "Failed to locate data source.".AsInterned());

            foreach (string? error in result.ErrorOutput)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }

            try
            {
                DbConnection = CreateDbConnection();
                exists = DbConnection?.ExecuteScalar<string>(checkScriptHistoryExistsQuery.ToString()) is "true";
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                DbConnection?.Dispose();
            }
        }

        return exists;
    }

    internal override void DeleteExternalDataSource(string fileExtension)
    {
        StringBuilder dropViewQuery = StringBuilderCache.Acquire();
        _ = dropViewQuery.Append(
        $"""
        {"SET NOCOUNT ON;".AsInterned()}
        {$"USE [{ConnectionString.Database}];".AsInterned()}
        
        IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'{nameof(ScriptHistory)}'))
        BEGIN
            DROP VIEW {nameof(ScriptHistory)};
        END
        
        IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'{nameof(ScriptHistory)}'))
        BEGIN
            PRINT 'true';
        END
        ELSE
        BEGIN
            PRINT 'false';
        END
        """.AsSpan());

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, dropViewQuery.ToString(), in CancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            Logger?.LogDataSource(LogLevel.Warning, $"{nameof(ScriptHistory)} view".AsInterned(), "failed to be dropped".AsInterned(), ConnectionString.Database, fileExtension.AsSpan(1).AsCached());

            foreach (string? error in result.ErrorOutput)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }
        }

        bool isDeleted = result.StandardOutput?[0] is "true";

        if (isDeleted is false)
        {
            long timestamp = Stopwatch.GetTimestamp();
            try
            {
                DbConnection = CreateDbConnection();
                isDeleted = DbConnection?.ExecuteScalar<string>(dropViewQuery.ToString()) is "true";
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                DbConnection?.Dispose();
            }
        }

        if (isDeleted)
        {
            Logger?.LogDataSource(LogLevel.Information, $"{nameof(ScriptHistory)} view".AsInterned(), "dropped".AsInterned(), ConnectionString.Database, fileExtension.AsSpan(1).AsCached(), result.ExecutionTime!.Value.Humanize());
        }

        IPathInfo dataSourceFile = CreatePathInfo(DatabaseLocation.Directory, $"{ConnectionString.Database}_{nameof(ScriptHistory)}{fileExtension}".AsInterned());

        if (dataSourceFile.Exists)
        {
            dataSourceFile.Delete();
            Logger?.LogPath(LogLevel.Information, "Deleted external data source at".AsInterned(), dataSourceFile.FullPath);
        }

        if (SourceType is DataSourceType.Csv)
        {
            IPathInfo formatFilePath = CreatePathInfo(DatabaseLocation.Directory, $"{nameof(ScriptHistory)}_CSVFormatFile.fmt".AsInterned());

            if (formatFilePath.Exists)
            {
                formatFilePath.Delete();
                Logger?.LogPath(LogLevel.Information, "Deleted csv format file at".AsInterned(), formatFilePath.FullPath);
            }
        }
    }

    internal override void DeleteTableDataSource()
    {
        StringBuilder dropTableQuery = StringBuilderCache.Acquire();
        _ = dropTableQuery.Append(
        $"""
        {"SET NOCOUNT ON;".AsInterned()}
        {$"USE [{ConnectionString.Database}];".AsInterned()}
        
        IF OBJECT_ID('{nameof(ScriptHistory)}', 'U') IS NOT NULL
        BEGIN
           DROP TABLE {nameof(ScriptHistory)};
        END
        
        IF OBJECT_ID('{nameof(ScriptHistory)}', 'U') IS NULL
        BEGIN
            PRINT 'true'
        END
        ELSE
        BEGIN
            PRINT 'false'
        END
        """.AsSpan());

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, dropTableQuery.ToString(), in CancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            Logger?.LogDataSource(LogLevel.Warning, $"{nameof(ScriptHistory)} table".AsInterned(), "failed to be dropped".AsInterned(), ConnectionString.Database, "internal".AsInterned());

            foreach (string? error in result.ErrorOutput)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }
        }

        bool isDeleted = result.StandardOutput?[0] is "true";

        if (isDeleted is false)
        {
            long timestamp = Stopwatch.GetTimestamp();
            try
            {
                DbConnection = CreateDbConnection();
                isDeleted = DbConnection?.ExecuteScalar<string>(dropTableQuery.ToString()) is "true";
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                DbConnection?.Dispose();
            }
        }

        if (isDeleted)
        {
            Logger?.LogDataSource(LogLevel.Information, $"{nameof(ScriptHistory)} table".AsInterned(), "dropped".AsInterned(), ConnectionString.Database, "internal".AsInterned(), result.ExecutionTime!.Value.Humanize());
        }
    }

    internal override void CreateExternalDataSourceBackup(int sequenceNumber)
    {
        string backupName = $"{DataSource.GetFileName()}_{sequenceNumber}_{DataSource.GetExtension()}".AsInterned();

        IPathInfo backupFile = CreatePathInfo(DatabaseLocation?.GetParentDirectory(DatabaseLocation.Directory)!, "backup".AsInterned(), $"{backupName}.bak".AsInterned());

        if (backupFile.DirectoryExists(backupFile.Directory) is false)
        {
            backupFile.CreateDirectory(backupFile.Directory);
        }

        if (backupFile.Exists)
        {
            Logger?.LogBackup(LogLevel.Information, "already exists".AsInterned(), ConnectionString.Database, sequenceNumber);
            return;
        }

        DataSource.CopyTo(backupFile.FullPath, false);

        if (backupFile.Exists)
        {
            Logger?.LogBackup(LogLevel.Information, "file created".AsInterned(), ConnectionString.Database, sequenceNumber);
        }
        else
        {
            Logger?.LogBackup(LogLevel.Warning, "file not created".AsInterned(), ConnectionString.Database, sequenceNumber);
        }
    }

    internal override void DeleteExternalDataSourceBackup(int? sequenceNumber = null!)
    {
        if (sequenceNumber is not null)
        {
            DeleteSingleBackup();
        }
        else
        {
            DeleteBackups();
        }

        void DeleteSingleBackup()
        {
            string backupName = $"{DataSource.GetFileName()}_{sequenceNumber}_{DataSource.GetExtension()}".AsInterned();

            IPathInfo backupFile = CreatePathInfo(DatabaseLocation?.GetParentDirectory(DatabaseLocation.Directory)!, "backup".AsInterned(), $"{backupName}.bak".AsInterned());

            if (backupFile.Exists)
            {
                backupFile.Delete();
                Logger?.LogBackup(LogLevel.Information, "file deleted".AsInterned(), ConnectionString.Database, sequenceNumber);
            }
        }

        void DeleteBackups()
        {
            IPathInfo backupPath = CreatePathInfo(DatabaseLocation?.GetParentDirectory(DatabaseLocation.Directory)!, "backup".AsInterned());

            string fileName = $"{ConnectionString.Database}_{nameof(ScriptHistory)}_".AsInterned();
            IEnumerable<string> backupFiles = backupPath.EnumerateDirectoryFiles(backupPath.FullPath, "*.bak".AsInterned()).Where(x => x.StartsWith(fileName, StringComparison.Ordinal));

            if (backupFiles.Any())
            {
                foreach (string file in backupFiles)
                {
                    backupPath.Delete(file);
                    Logger?.LogPath(LogLevel.Information, "Deleted backup file at".AsInterned(), file);
                }
            }
        }
    }

    internal override void RestoreExternalDataSourceBackup(int sequenceNumber)
    {
        string backupName = $"{DataSource.GetFileName()}_{sequenceNumber}_{DataSource.GetExtension()}".AsInterned();

        IPathInfo backupFile = CreatePathInfo(DatabaseLocation?.GetParentDirectory(DatabaseLocation.Directory)!, "backup".AsInterned(), $"{backupName}.bak".AsInterned());

        if (backupFile.Exists is false)
        {
            Logger?.LogBackup(LogLevel.Warning, "file does not exist".AsInterned(), ConnectionString.Database, sequenceNumber);
            return;
        }

        backupFile.CopyTo(DataSource.FullPath, false);
        Logger?.LogBackup(LogLevel.Information, "file restored".AsInterned(), ConnectionString.Database, sequenceNumber);
    }


    internal override IPathInfo GetDatabasePath()
    {
        StringBuilder databasePathQuery = StringBuilderCache.Acquire();
        _ = databasePathQuery.AppendCached("SET NOCOUNT ON;");
        _ = databasePathQuery.AppendLineCached("SELECT TOP 1 physical_name FROM sys.master_files WHERE [name] = '").AppendCached(ConnectionString.Database).Append("';");

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, databasePathQuery.ToString(), in CancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            Logger?.LogMessage(LogLevel.Warning, "Failed to locate database path.".AsInterned());

            foreach (string? error in result.ErrorOutput)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }
        }

        if (result.StandardOutput.HasValue is false)
        {
            try
            {
                DbConnection = CreateDbConnection();
                result.StandardOutput = DbConnection?.ExecuteScalar<string>(databasePathQuery.ToString())!;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                DbConnection?.Dispose();
            }
        }

        IPathInfo databaseFile = CreatePathInfo(result.StandardOutput?.FirstOrDefault()!);

        if (databaseFile.Exists is false)
        {
            Logger?.LogPath(LogLevel.Warning, "Failed to connect to database file in".AsInterned(), databaseFile.FullPath);
        }
        else
        {
            Logger?.LogPath(LogLevel.Information, "Located database file in".AsInterned(), databaseFile.FullPath, result.ExecutionTime!.Value.Humanize());
        }

        return databaseFile;
    }

    internal override void CreateDatabaseBackup(int sequenceNumber)
    {
        string backupName = $"{ConnectionString.Database}_{nameof(ScriptHistory)}_{sequenceNumber}".AsInterned();

        IPathInfo backupFile = CreatePathInfo(DatabaseLocation?.GetParentDirectory(DatabaseLocation.Directory)!, "backup".AsInterned(), $"{backupName}.bak".AsInterned());

        if (backupFile.DirectoryExists(backupFile.Directory) is false)
        {
            backupFile.CreateDirectory(backupFile.Directory);
        }

        if (backupFile.Exists)
        {
            Logger?.LogBackup(LogLevel.Information, "already exists".AsInterned(), ConnectionString.Database, sequenceNumber);
            return;
        }

        StringBuilder createBackupQuery = StringBuilderCache.Acquire();
        _ = createBackupQuery.Append(
        $"""
        {"SET NOCOUNT ON;".AsInterned()}
        {"USE [master];".AsInterned()}

        BACKUP DATABASE [{ConnectionString.Database}] TO DISK = N'{backupFile.FullPath}' 
        WITH NOFORMAT, NOINIT, NAME = N'{backupName} Full Database Backup', SKIP, NOREWIND, NOUNLOAD, STATS = 10;
        """.AsSpan());

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, createBackupQuery.ToString(), in CancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            Logger?.LogBackup(LogLevel.Warning, "not created".AsInterned(), ConnectionString.Database, sequenceNumber);

            foreach (string? error in result.ErrorOutput)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }

            long timestamp = Stopwatch.GetTimestamp();
            try
            {
                DbConnection = CreateDbConnection();
                _ = DbConnection?.Execute(createBackupQuery.ToString())!;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                DbConnection?.Dispose();
            }
        }

        if (backupFile.Exists)
        {
            Logger?.LogBackup(LogLevel.Information, "created".AsInterned(), ConnectionString.Database, sequenceNumber, result.ExecutionTime!.Value.Humanize());
        }
        else
        {
            Logger?.LogBackup(LogLevel.Warning, "not created".AsInterned(), ConnectionString.Database, sequenceNumber);
        }
    }

    public override IScriptExecutionManager CreateDatabaseSnapshot(int sequenceNumber)
    {
        string snapshotName = $"{ConnectionString.Database}_{nameof(ScriptHistory)}_{sequenceNumber}".AsInterned();

        IPathInfo snapshotFile = CreatePathInfo(DatabaseLocation?.Directory!, $"{snapshotName}.ss".AsInterned());

        if (snapshotFile.Exists)
        {
            Logger?.LogSnapshot(LogLevel.Information, "already exists".AsInterned(), ConnectionString.Database, sequenceNumber);
            return this;
        }

        StringBuilder createSnapshotQuery = StringBuilderCache.Acquire();
        _ = createSnapshotQuery.Append(
         $"""
         {"SET NOCOUNT ON;".AsInterned()}
         {"USE [master];".AsInterned()}

         IF NOT EXISTS(SELECT name FROM sys.databases WHERE name = '{snapshotName}')
         BEGIN
             CREATE DATABASE {snapshotName} 
             ON (NAME = {ConnectionString.Database}, FILENAME = '{snapshotFile.FullPath}')
             AS SNAPSHOT OF {ConnectionString.Database};
         END

         IF EXISTS(SELECT name FROM sys.databases WHERE name = '{snapshotName}')
         BEGIN
             SELECT 'true'
         END
         ELSE
         BEGIN
             SELECT 'false'
         END
         """.AsSpan());

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, createSnapshotQuery.ToString(), in CancelToken);

        bool isCreated = result.StandardOutput?[0] is "true";

        if (result.ErrorOutput?.Count > 0 || isCreated is false)
        {
            Logger?.LogSnapshot(LogLevel.Warning, "not created".AsInterned(), ConnectionString.Database, sequenceNumber);

            foreach (string? error in result.ErrorOutput!)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }

            long timestamp = Stopwatch.GetTimestamp();
            try
            {
                DbConnection = CreateDbConnection();
                _ = DbConnection?.Execute(createSnapshotQuery.ToString())!;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                DbConnection?.Dispose();
            }
        }

        if (snapshotFile.Exists && isCreated)
        {
            Logger?.LogSnapshot(LogLevel.Information, "created".AsInterned(), ConnectionString.Database, sequenceNumber, result.ExecutionTime!.Value.Humanize());
        }
        else
        {
            Logger?.LogSnapshot(LogLevel.Warning, "not created".AsInterned(), ConnectionString.Database, sequenceNumber);
        }

        return this;
    }

    internal override void DeleteDatabaseBackup(int? sequenceNumber = null!)
    {
        if (sequenceNumber is not null)
        {
            DeleteSingleBackup();
        }
        else
        {
            DeleteBackups();
        }

        void DeleteSingleBackup()
        {
            string backupName = $"{ConnectionString.Database}_{nameof(ScriptHistory)}_{sequenceNumber}".AsInterned();

            IPathInfo backupFile = CreatePathInfo(DatabaseLocation?.GetParentDirectory(DatabaseLocation.Directory)!, "backup".AsInterned(), $"{backupName}.bak".AsInterned());

            if (GetBackupCount() is 0) return;

            StringBuilder deleteBackupQuery = StringBuilderCache.Acquire();
            _ = deleteBackupQuery.Append(
            $"""
            {"SET NOCOUNT ON;".AsInterned()}
            {"USE [master];".AsInterned()}

            IF EXISTS (SELECT 1 FROM msdb.dbo.backupset WHERE database_name = '{ConnectionString.Database}')
            BEGIN
                  DELETE FROM msdb.dbo.backupset
                  WHERE database_name LIKE '{backupName}%';
            END
            """.AsSpan());

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, deleteBackupQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                Logger?.LogBackup(LogLevel.Warning, "failed to delete".AsInterned(), ConnectionString.Database, sequenceNumber);

                foreach (string? error in result.ErrorOutput)
                {
                    Logger?.LogException(LogLevel.Warning, new Exception(error));
                }

                long timestamp = Stopwatch.GetTimestamp();
                try
                {
                    DbConnection = CreateDbConnection();
                    _ = DbConnection?.Execute(deleteBackupQuery.ToString())!;
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                    DbConnection?.Dispose();
                }
            }

            if (backupFile.Exists)
            {
                backupFile.Delete();
            }

            Logger?.LogBackup(LogLevel.Information, "deleted".AsInterned(), ConnectionString.Database, sequenceNumber, result.ExecutionTime!.Value.Humanize()!);
        }

        void DeleteBackups()
        {
            IPathInfo backupPath = CreatePathInfo(DatabaseLocation?.GetParentDirectory(DatabaseLocation.Directory)!, "backup".AsInterned());

            StringBuilder deleteBackupsQuery = StringBuilderCache.Acquire();
            _ = deleteBackupsQuery.Append(
            $"""
            {"SET NOCOUNT ON;".AsInterned()}
            {"USE [master];".AsInterned()}
            EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'{ConnectionString.Database}'
            """.AsSpan());

            //StringBuilder deleteBackupsQuery = StringBuilderCache.Acquire();
            //deleteBackupsQuery.Append(string.Intern("SET NOCOUNT ON;").AsSpan()).AppendLine();
            //deleteBackupsQuery.Append(string.Intern("USE [master];").AsSpan()).AppendLine();
            //deleteBackupsQuery.Append("EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'".AsSpan()).Append(ConnectionString.Database.AsSpan()).Append('\'');

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, deleteBackupsQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                Logger?.LogMessage(LogLevel.Warning, $"Failed to delete backup(s) for {ConnectionString.Database}".AsInterned());

                foreach (string? error in result.ErrorOutput)
                {
                    Logger?.LogException(LogLevel.Warning, new Exception(error));
                }

                long timestamp = Stopwatch.GetTimestamp();
                try
                {
                    DbConnection = CreateDbConnection();
                    _ = DbConnection?.Execute(deleteBackupsQuery.ToString())!;
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                    DbConnection?.Dispose();
                }
            }

            string fileName = $"{ConnectionString.Database}_{nameof(ScriptHistory)}_";
            IEnumerable<string>? backupFiles = backupPath.EnumerateDirectoryFiles(backupPath.FullPath, "*.bak".AsInterned())?.Where(x => x.StartsWith(fileName, StringComparison.Ordinal));

            if (backupFiles?.Any() is true)
            {
                foreach (string file in backupFiles)
                {
                    backupPath.Delete(file);
                    Logger?.LogPath(LogLevel.Information, "Deleted backup file".AsInterned(), file);
                }
            }

            int backupCount = GetBackupCount();

            if (backupCount > 0)
            {
                Logger?.LogMessage(LogLevel.Information, "Deleted all backups for".AsInterned(), ConnectionString.Database, result.ExecutionTime!.Value.Humanize()!);
            }
        }

        int GetBackupCount()
        {
            StringBuilder backupCountQuery = StringBuilderCache.Acquire();
            _ = backupCountQuery.Append(
            $"""
            {"SET NOCOUNT ON;".AsInterned()}
            SELECT COUNT(*)
            FROM msdb.dbo.backupset 
            WHERE database_name = '{ConnectionString.Database}'
            """.AsSpan());

            //StringBuilder backupCountQuery = StringBuilderCache.Acquire();
            //backupCountQuery.Append(string.Intern("SET NOCOUNT ON;").AsSpan()).AppendLine();
            //backupCountQuery.Append(string.Intern("SELECT COUNT(*)").AsSpan()).AppendLine();
            //backupCountQuery.Append("FROM msdb.dbo.backupset".AsSpan()).AppendLine();
            //backupCountQuery.Append("WHERE database_name = '".AsSpan()).Append(ConnectionString.Database.AsSpan()).Append('\'');

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, backupCountQuery.ToString(), in CancelToken);

            bool valueParsed = int.TryParse(result.StandardOutput?[0], out int backupCount);

            if (result.ErrorOutput?.Count > 0 || valueParsed is false)
            {
                try
                {
                    DbConnection = CreateDbConnection();
                    backupCount = (int)DbConnection?.ExecuteScalar<int>(backupCountQuery.ToString())!;
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    DbConnection?.Dispose();
                }
            }

            return backupCount;
        }
    }

    public override IScriptExecutionManager DeleteDatabaseSnapshot(int? sequenceNumber = null!)
    {
        if (sequenceNumber is not null)
        {
            DropSingleSnapshot();
        }
        else
        {
            DropAllSnapshots();
        }

        void DropSingleSnapshot()
        {
            string snapshotName = $"{ConnectionString.Database}_{nameof(ScriptHistory)}_{sequenceNumber})".AsInterned();

            IPathInfo snapshotFile = CreatePathInfo(DatabaseLocation?.Directory!, $"{snapshotName}.ss".AsInterned());

            if (GetSnapshotCount() is 0) return;

            StringBuilder snapshotDropQuery = StringBuilderCache.Acquire();
            _ = snapshotDropQuery.Append(
            $"""
            {"SET NOCOUNT ON;".AsInterned()}
            {"USE [master];".AsInterned()}
            IF EXISTS (SELECT * FROM sys.databases WHERE name = '{snapshotName}' AND source_database_id IS NOT NULL)
            BEGIN
               DROP DATABASE {snapshotName};
            END
            """.AsSpan());

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, snapshotDropQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                Logger?.LogSnapshot(LogLevel.Warning, "failed to delete".AsInterned(), ConnectionString.Database, sequenceNumber);

                foreach (string? error in result.ErrorOutput)
                {
                    Logger?.LogException(LogLevel.Warning, new Exception(error));
                }

                long timestamp = Stopwatch.GetTimestamp();
                try
                {
                    DbConnection = CreateDbConnection();
                    _ = DbConnection?.Execute(snapshotDropQuery.ToString())!;
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                    DbConnection?.Dispose();
                }
            }

            if (snapshotFile.Exists)
            {
                snapshotFile.Delete();
            }

            Logger?.LogSnapshot(LogLevel.Information, "deleted".AsInterned(), ConnectionString.Database, sequenceNumber, result.ExecutionTime!.Value.Humanize()!);
        }

        void DropAllSnapshots()
        {
            StringBuilder dropSnapshotsQuery = DatabaseSnapshotsDropQuery(ConnectionString.Database);

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, dropSnapshotsQuery.ToString(), in CancelToken);

            if (result.ErrorOutput?.Count > 0)
            {
                Logger?.LogMessage(LogLevel.Warning, $"Failed to delete snapshot(s) for {ConnectionString.Database} database".AsInterned());

                foreach (string? error in result.ErrorOutput)
                {
                    Logger?.LogException(LogLevel.Warning, new Exception(error));
                }

                long timestamp = Stopwatch.GetTimestamp();
                try
                {
                    DbConnection = CreateDbConnection();
                    _ = DbConnection?.Execute(dropSnapshotsQuery.ToString())!;
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                    DbConnection?.Dispose();
                }
            }

            string filename = $"{ConnectionString.Database}_{nameof(ScriptHistory)}_";
            IEnumerable<string>? snapshotFiles = DatabaseLocation.EnumerateDirectoryFiles(DatabaseLocation.Directory, "*.ss".AsInterned())?.Where(x => x.StartsWith(filename, StringComparison.Ordinal));

            if (snapshotFiles?.Any() is true)
            {
                foreach (string file in snapshotFiles)
                {
                    DatabaseLocation.Delete(file);
                    Logger?.LogPath(LogLevel.Information, "Deleted snapshot file".AsInterned(), file);
                }
            }

            int snapshotCount = GetSnapshotCount();

            if (snapshotCount > 0)
            {
                Logger?.LogMessage(LogLevel.Information, "Deleted all snapshots for".AsInterned(), ConnectionString.Database, result.ExecutionTime!.Value.Humanize()!);
            }
        }

        StringBuilder DatabaseSnapshotsDropQuery(string snapshot)
        {
            StringBuilder dropSnapshotsQuery = StringBuilderCache.Acquire();
            _ = dropSnapshotsQuery.AppendLine("SET NOCOUNT ON;\n");
            _ = dropSnapshotsQuery.AppendLine("USE [master];\n");
            _ = dropSnapshotsQuery.AppendLine("DECLARE @SnapshotName VARCHAR(100)");
            _ = dropSnapshotsQuery.Append("SET @SnapshotName = '").Append(snapshot).AppendLine("'\n");
            _ = dropSnapshotsQuery.AppendLine("DECLARE @SnapshotToDelete VARCHAR(100)");
            _ = dropSnapshotsQuery.AppendLine("DECLARE @SnapshotCursor CURSOR");
            _ = dropSnapshotsQuery.AppendLine("SET @SnapshotCursor = CURSOR FOR");
            _ = dropSnapshotsQuery.AppendLine("SELECT name FROM sys.databases WHERE name LIKE '%'+@SnapshotName+'[_]%'\n");
            _ = dropSnapshotsQuery.AppendLine("OPEN @SnapshotCursor");
            _ = dropSnapshotsQuery.AppendLine("FETCH NEXT FROM @SnapshotCursor INTO @SnapshotToDelete\n");
            _ = dropSnapshotsQuery.AppendLine("WHILE @@FETCH_STATUS = 0");
            _ = dropSnapshotsQuery.AppendLine("BEGIN");
            _ = dropSnapshotsQuery.AppendLine(" DECLARE @SqlString NVARCHAR(4000)");
            _ = dropSnapshotsQuery.AppendLine(" SET @SqlString = N'DROP DATABASE [' + REPLACE(@SnapshotToDelete, ']', ']]') + N']'");
            _ = dropSnapshotsQuery.AppendLine(" EXEC sp_executesql @SqlString");
            _ = dropSnapshotsQuery.AppendLine(" FETCH NEXT FROM @SnapshotCursor INTO @SnapshotToDelete");
            _ = dropSnapshotsQuery.AppendLine("END\n");
            _ = dropSnapshotsQuery.AppendLine("CLOSE @SnapshotCursor");
            _ = dropSnapshotsQuery.AppendLine("DEALLOCATE @SnapshotCursor");

            return dropSnapshotsQuery;
        }

        int GetSnapshotCount()
        {
            StringBuilder snapshotCountQuery = StringBuilderCache.Acquire();
            _ = snapshotCountQuery.Append(
            $"""
            {"SET NOCOUNT ON;".AsInterned()}
            SELECT COUNT(*)
            FROM sys.databases
            WHERE name LIKE '{ConnectionString.Database}_{nameof(ScriptHistory)}_%' AND snapshot_isolation_state = 1;
            """.AsSpan());

            //StringBuilder snapshotCountQuery = StringBuilderCache.Acquire();
            //snapshotCountQuery.Append(string.Intern("SET NOCOUNT ON;").AsSpan()).AppendLine();
            //snapshotCountQuery.Append(string.Intern("USE [master];").AsSpan()).AppendLine();
            //snapshotCountQuery.Append(string.Intern("SELECT COUNT(*)").AsSpan()).AppendLine();
            //snapshotCountQuery.Append("FROM sys.databases".AsSpan()).AppendLine();
            //snapshotCountQuery.Append("WHERE name LIKE '".AsSpan()).Append(ConnectionString.Database.AsSpan()).Append('_').Append(nameof(ScriptHistory)).Append("_%' AND snapshot_isolation_state = 1;".AsSpan());

            using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, snapshotCountQuery.ToString(), in CancelToken);

            bool valueParsed = int.TryParse(result.StandardOutput?[0], out int snapshotCount);

            if (result.ErrorOutput?.Count > 0 || valueParsed is false)
            {
                try
                {
                    DbConnection = CreateDbConnection();
                    snapshotCount = (int)DbConnection?.ExecuteScalar<int>(snapshotCountQuery.ToString())!;
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    DbConnection?.Dispose();
                }
            }

            return snapshotCount;
        }

        return this;
    }

    internal override void RestoreDatabaseBackup(int sequenceNumber)
    {
        string backupName = $"{ConnectionString.Database}_{nameof(ScriptHistory)}_{sequenceNumber}".AsInterned();

        IPathInfo backupFile = CreatePathInfo(DatabaseLocation?.GetParentDirectory(DatabaseLocation.Directory)!, "backup".AsInterned(), $"{backupName}.bak".AsInterned());

        if (backupFile.Exists is false)
        {
            Logger?.LogBackup(LogLevel.Warning, "file could not be found".AsInterned(), ConnectionString.Database, sequenceNumber);
            return;
        }

        _ = DeleteDatabaseSnapshot(null!);

        StringBuilder restoreBackupQuery = StringBuilderCache.Acquire();
        _ = restoreBackupQuery.Append(
        $"""
         {"SET NOCOUNT ON;".AsInterned()}
         {"USE [master];".AsInterned()}

         ALTER DATABASE [{ConnectionString.Database}] 
         SET OFFLINE WITH ROLLBACK IMMEDIATE;
         RESTORE DATABASE [{ConnectionString.Database}]
         FROM DISK = N'{backupFile.FullPath}' WITH REPLACE;
         ALTER DATABASE [{ConnectionString.Database}] SET ONLINE;
         """.AsSpan());

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, restoreBackupQuery.ToString(), in CancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            Logger?.LogBackup(LogLevel.Warning, "failed to restore".AsInterned(), ConnectionString.Database, sequenceNumber, new Exception(result.ErrorOutput));

            foreach (string? error in result.ErrorOutput)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }

            long timestamp = Stopwatch.GetTimestamp();
            try
            {
                DbConnection = CreateDbConnection();
                _ = DbConnection?.Execute(restoreBackupQuery.ToString())!;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                DbConnection?.Dispose();
            }
        }

        Logger?.LogBackup(LogLevel.Information, "restored".AsInterned(), ConnectionString.Database, sequenceNumber, result.ExecutionTime!.Value.Humanize());
    }

    public override IScriptExecutionManager RestoreDatabaseBackup(string backupFilePath)
    {
        IPathInfo backupFile = CreatePathInfo(backupFilePath);

        if (backupFile.Exists is false)
        {
            Logger?.LogPath(LogLevel.Warning, "Backup file could not be found at".AsInterned(), backupFilePath);
            return this;
        }

        _ = DeleteDatabaseSnapshot(null!);

        StringBuilder restoreBackupQuery = StringBuilderCache.Acquire();
        _ = restoreBackupQuery.Append(
        $"""
         {"SET NOCOUNT ON;".AsInterned()}
         {"USE [master];".AsInterned()}

         ALTER DATABASE [{ConnectionString.Database}] 
         SET OFFLINE WITH ROLLBACK IMMEDIATE;
         RESTORE DATABASE [{ConnectionString.Database}]
         FROM DISK = N'{backupFile.FullPath}' WITH REPLACE;
         ALTER DATABASE [{ConnectionString.Database}] SET ONLINE;
         """.AsSpan());

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, restoreBackupQuery.ToString(), in CancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            Logger?.LogPath(LogLevel.Warning, $"Backup could not be restored for {ConnectionString.Database} database at".AsInterned(), backupFilePath);

            foreach (string? error in result.ErrorOutput)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }

            long timestamp = Stopwatch.GetTimestamp();
            try
            {
                DbConnection = CreateDbConnection();
                _ = DbConnection?.Execute(restoreBackupQuery.ToString())!;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                DbConnection?.Dispose();
            }
        }

        Logger?.LogPath(LogLevel.Information, $"Backup restored for {ConnectionString.Database} database at".AsInterned(), backupFilePath, result.ExecutionTime!.Value.Humanize());

        return this;
    }

    public override IScriptExecutionManager RestoreDatabaseSnapshot(int sequenceNumber)
    {
        string snapshotName = $"{ConnectionString.Database}_ScriptHistory_{sequenceNumber}".AsInterned();

        IPathInfo snapshotFile = CreatePathInfo(DatabaseLocation?.Directory!, $"{snapshotName}.ss".AsInterned());

        if (snapshotFile.Exists is false)
        {
            Logger?.LogSnapshot(LogLevel.Warning, "file could not be found".AsInterned(), ConnectionString.Database, sequenceNumber);
            return this;
        }

        StringBuilder restoreSnapshotQuery = StringBuilderCache.Acquire();
        _ = restoreSnapshotQuery.Append(
        $"""
        {"SET NOCOUNT ON;".AsInterned()}
        {"USE [master];".AsInterned()}

        ALTER DATABASE [{ConnectionString.Database}] SET SINGLE_USER;
        RESTORE DATABASE [{ConnectionString.Database}] FROM DATABASE_SNAPSHOT = '{snapshotName}';
        ALTER DATABASE [{ConnectionString.Database}] SET MULTI_USER WITH NO_WAIT;
        """.AsSpan());

        using CommandLineResult result = DbCommandTool.ExecuteScriptText(ExecutionRunType.DefaultRun, ConnectionString, restoreSnapshotQuery.ToString(), in CancelToken);

        if (result.ErrorOutput?.Count > 0)
        {
            Logger?.LogSnapshot(LogLevel.Warning, "failed to restore".AsInterned(), ConnectionString.Database, sequenceNumber);

            foreach (string? error in result.ErrorOutput)
            {
                Logger?.LogException(LogLevel.Warning, new Exception(error));
            }

            long timestamp = Stopwatch.GetTimestamp();
            try
            {
                DbConnection = CreateDbConnection();
                _ = DbConnection?.Execute(restoreSnapshotQuery.ToString())!;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                result.ExecutionTime = Stopwatch.GetElapsedTime(timestamp);
                DbConnection?.Dispose();
            }
        }

        Logger?.LogBackup(LogLevel.Information, "restored".AsInterned(), ConnectionString.Database, sequenceNumber, result.ExecutionTime!.Value.Humanize());

        return this;
    }
}