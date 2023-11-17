using System.Data;
using System.Diagnostics;
using System.Text.Json;

using ScriptManager.Enums;
using ScriptManager.Environments;
using ScriptManager.Extensions;
using ScriptManager.Managers;
using ScriptManager.Utilities;

namespace ScriptManager;

public abstract class ScriptExecutionManager : IScriptExecutionManager
{
    internal const int UPSERT_SCRIPT_THRESHOLD = 1000;

    internal readonly static Comparer<IPathInfo> FileComparer = Comparer<IPathInfo>.Create(new Comparison<IPathInfo>(CompareFiles));

    internal readonly static Comparer<Script> ScriptComparer = Comparer<Script>.Create(new Comparison<Script>(CompareScripts));

    internal CancellationTokenSource CancelSource = default!;

    internal CancellationToken? CancelToken = default!;

    internal string ScriptExtension = default!;

    internal ILoggerFactory LoggerFactory = default!;

    internal ExecutionProgress Progress = default!;


    public ILogger Logger { get; init; } = default!;


    public ExecutionThreadType ThreadType { get; internal set; } = default!;

    public ExecutionRunType ExecutionType { get; internal set; } = default!;

    public ExecutionOptimizationType OptimizationType { get; internal set; } = default!;

    public DataSourceType SourceType { get; internal set; } = default!;

    public int LastExecutedSequence { get; internal set; } = default!;


    public IProgress<ExecutionProgress> ExecutionProgress { get; set; } = default!;

    public event EventHandler<Script>? OnExecutionEnded = default;

    public event EventHandler<Script>? OnExecutionStarted = default;

    public event EventHandler<Script>? OnScanEnded = default;

    public event EventHandler<Script>? OnScanStarted = default;


    public ConnectionStringInfo ConnectionString { get; internal set; } = default!;

    public IDbConnection DbConnection { get; internal set; } = default!;

    public abstract ICommandTool DbCommandTool { get; internal set; }


    public ServerEnvironmentType EnvironmentType { get; internal set; } = default!;

    public DataProviderType DatabaseType { get; internal set; } = default!;

    public IPathInfo DatabaseLocation { get; internal set; } = default!;

    public IPathInfo DataSource { get; internal set; } = default!;

    public PathEnvironmentInfo DataSourceInfo { get; internal set; } = default!;


    public SortedSet<Script> Scripts { get; internal set; } = default!;

    public ScriptCreator? ScriptCreator { get; set; } = default!;

    public IPathInfo ScriptsLocation { get; internal set; } = default!;


    internal ScriptExecutionManager(in ExecutionConfiguration config, ILoggerFactory logger, in CancellationToken? cancelToken = null!)
    {
        ArgumentException.ThrowIfNullOrEmpty(nameof(config));

        LoggerFactory = logger;
        Logger = LoggerFactory?.CreateLogger("ScriptManager".AsCached())!;
        ScriptExtension = config.ScriptExtension;

        ExecutionType = config.ExecutionType;
        EnvironmentType = config.EnvironmentType;
        ThreadType = config.ThreadType;
        OptimizationType = config.OptimizationType;
        DatabaseType = config.DatabaseType;
        SourceType = config.SourceType;
        DataSourceInfo = config.DataSourceInfo;
        ScriptsLocation = config.ScriptsLocationInfo.CreatePathInfo();

        if (config.SequenceNumber.HasValue) LastExecutedSequence = config.SequenceNumber.Value;

        ConnectionString = CreateConnectionString(config);

        SetCancellationToken(cancelToken);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareFiles(IPathInfo file1, IPathInfo file2)
    {
        _ = file1.TryGetSequenceNumber(out int x);

        _ = file2.TryGetSequenceNumber(out int y);

        return x.CompareTo(y);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareScripts(Script script1, Script script2)
    {
        return script1.SequenceNumber.CompareTo(script2.SequenceNumber);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected IPathInfo CreatePathInfo(string fullFilePath)
    {
        ArgumentNullException.ThrowIfNull(fullFilePath);

        return EnvironmentType switch
        {
            ServerEnvironmentType.Local => new LocalPathInfo(fullFilePath),
            ServerEnvironmentType.Docker => new DockerPathInfo(fullFilePath, DataSourceInfo.ContainerName!),
            ServerEnvironmentType.Remote => new RemotePathInfo(fullFilePath, DataSourceInfo),
            _ => null!
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected IPathInfo CreatePathInfo(params string[] pathTokens)
    {
        ArgumentNullException.ThrowIfNull(pathTokens);

        IPathInfo pathInfo = null!;

        switch (EnvironmentType)
        {
            case ServerEnvironmentType.Local:
                pathInfo = new LocalPathInfo(pathTokens);
                break;
            case ServerEnvironmentType.Docker:
                pathInfo = new DockerPathInfo(DataSourceInfo.ContainerName!, pathTokens);
                break;
            case ServerEnvironmentType.Remote:
                pathInfo = new RemotePathInfo(DataSourceInfo, pathTokens);
                break;
            default: break;
        }

        return pathInfo;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetCsvSequenceNumber(ReadOnlySpan<char> text)
    {
        if (text.IsEmpty) return -1;

        int colonIndex = text.IndexOf(Constants.Comma);

        ReadOnlySpan<char> chars = text.Slice(0, colonIndex);

        _ = int.TryParse(chars, out int sequence);

        return sequence;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetJsonSequenceNumber(ReadOnlySpan<char> text)
    {
        if (text.IsEmpty) return -1;

        int colonIndex = text.IndexOf(Constants.Colon) + 1;

        int commaIndex = text.IndexOf(Constants.Comma);

        int length = commaIndex - colonIndex;

        int sequence = 0;

        if (length > sequence)
        {
            ReadOnlySpan<char> chars = text.Slice(colonIndex, length);

            _ = int.TryParse(chars, out sequence);
        }

        return sequence;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetCancellationToken(in CancellationToken? cancelToken = null!)
    {
        if ((cancelToken is null && CancelToken is null) || CancelToken?.IsCancellationRequested is true)
        {
            CancelSource = new CancellationTokenSource();
            CancelToken = CancelSource.Token;
        }
        else
        {
            CancelToken ??= cancelToken;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected bool TryOpenDBConnection()
    {
        try
        {
            if (DbConnection?.State is not ConnectionState.Open) DbConnection?.Open();
            return true;
        }
        catch
        {
            return false;
        }
    }

    public void Dispose()
    {
        _ = CancelExecution();
        Scripts?.Clear();
        DbConnection?.Dispose();
        CancelSource = null!;
        CancelToken = null!;
        ConnectionString = default;
        DatabaseLocation = null!;
        DataSource = null!;
        ScriptExtension = null!;
        LoggerFactory = null!;
        Progress = null!;
        ScriptCreator = null!;
        ScriptsLocation = null!;
    }



    internal abstract ConnectionStringInfo CreateConnectionString(in ExecutionConfiguration config);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal abstract IDbConnection CreateDbConnection();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IScriptExecutionManager ChangeConnectionString(in ExecutionConfiguration config)
    {
        ScriptExtension = config.ScriptExtension;

        ExecutionType = config.ExecutionType;
        EnvironmentType = config.EnvironmentType;
        ThreadType = config.ThreadType;
        OptimizationType = config.OptimizationType;
        DatabaseType = config.DatabaseType;
        SourceType = config.SourceType;
        DataSourceInfo = config.DataSourceInfo;
        ScriptsLocation = config.ScriptsLocationInfo.CreatePathInfo();

        ConnectionString = CreateConnectionString(config);
        DatabaseLocation = GetDatabasePath();
        LastExecutedSequence = GetLatestSequence();

        return this;
    }



    public IScriptExecutionManager CreateDataSource()
    {
        _ = DeleteExistingDataSource();

        switch (SourceType)
        {
            case DataSourceType.Internal:
                CreateTableDataSource();
                break;
            case DataSourceType.Csv:
                DataSource = CreateCsvDataSource();
                break;
            case DataSourceType.Json:
                DataSource = CreateJsonDataSource();
                break;
        }

        if (LastExecutedSequence == 0) LastExecutedSequence = GetLatestSequence();

        return this;
    }

    public IScriptExecutionManager CreateDataSourceBackup(int sequenceNumber)
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                CreateDatabaseBackup(sequenceNumber);
                break;
            default:
                CreateDatabaseBackup(sequenceNumber);
                CreateExternalDataSourceBackup(sequenceNumber);
                break;
        }

        return this;
    }

    internal abstract void CreateExternalDataSourceBackup(int sequenceNumber);

    internal IPathInfo CreateDataSourceFile()
    {
        string dataSourceFilePath = string.Empty;

        IPathInfo dataSource = null!;

        if (SourceType is DataSourceType.Csv)
        {
            dataSourceFilePath = DatabaseLocation.GetPathFromDirectory($"{ConnectionString.Database}_{nameof(ScriptHistory)}.csv".AsCached());

            dataSource = CreatePathInfo(dataSourceFilePath);

            if (dataSource.Exists) return dataSource;

            dataSource.WriteAllText($"{nameof(ScriptHistory.SequenceNumber)},{nameof(ScriptHistory.Author)},{nameof(ScriptHistory.Description)},{nameof(ScriptHistory.Status)},{nameof(ScriptHistory.CreatedDate)}".AsCached());
        }
        else if (SourceType is DataSourceType.Json)
        {
            dataSourceFilePath = DatabaseLocation.GetPathFromDirectory($"{ConnectionString.Database}_{nameof(ScriptHistory)}.json".AsCached());

            dataSource = CreatePathInfo(dataSourceFilePath);

            if (dataSource.Exists) return dataSource;

            dataSource.WriteAllText("[\n\n]".AsInterned());
        }

        if (dataSource.Exists)
        {
            Logger.LogPath(LogLevel.Information, "Data source file created at".AsInterned(), dataSourceFilePath);
        }
        else
        {
            Logger.LogPath(LogLevel.Information, "Data source file could not be created at".AsInterned(), dataSourceFilePath);
        }

        return dataSource;
    }

    internal abstract IPathInfo CreateCsvDataSource();

    internal abstract IPathInfo CreateJsonDataSource();

    internal abstract void CreateTableDataSource();

    internal abstract bool DataSourceExists(DataSourceType sourceType);

    public IScriptExecutionManager DeleteDataSource()
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                DeleteTableDataSource();
                break;
            case DataSourceType.Csv:
                DeleteExternalDataSource(".csv".AsInterned());
                break;
            case DataSourceType.Json:
                DeleteExternalDataSource(".json".AsInterned());
                break;
        }

        return this;
    }

    public IScriptExecutionManager DeleteDataSourceBackup(int? sequenceNumber = null)
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                DeleteDatabaseBackup(sequenceNumber);
                break;
            default:
                DeleteDatabaseBackup(sequenceNumber);
                DeleteExternalDataSourceBackup(sequenceNumber);
                break;
        }

        return this;
    }

    internal abstract void DeleteExternalDataSourceBackup(int? sequenceNumber = null!);

    public IScriptExecutionManager DeleteExistingDataSource()
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                if (DataSourceExists(DataSourceType.Json)) DeleteExternalDataSource(".json".AsInterned());
                if (DataSourceExists(DataSourceType.Csv)) DeleteExternalDataSource(".csv".AsInterned());
                break;
            case DataSourceType.Csv:
                if (DataSourceExists(DataSourceType.Internal)) DeleteTableDataSource();
                if (DataSourceExists(DataSourceType.Json)) DeleteExternalDataSource(".json".AsInterned());
                break;
            case DataSourceType.Json:
                if (DataSourceExists(DataSourceType.Internal)) DeleteTableDataSource();
                if (DataSourceExists(DataSourceType.Csv)) DeleteExternalDataSource(".csv".AsInterned());
                break;
        }

        return this;
    }

    internal abstract void DeleteExternalDataSource(string fileExtension);

    internal abstract void DeleteTableDataSource();

    public IScriptExecutionManager RestoreDataSourceBackup(int sequenceNumber)
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                RestoreDatabaseBackup(sequenceNumber);
                break;
            default:
                RestoreDatabaseBackup(sequenceNumber);
                RestoreExternalDataSourceBackup(sequenceNumber);
                break;
        }

        return this;
    }

    internal abstract void RestoreExternalDataSourceBackup(int sequenceNumber);



    internal abstract int GetLatestSequence();

    internal abstract IPathInfo GetDatabasePath();

    internal abstract void CreateDatabaseBackup(int sequenceNumber);

    public abstract IScriptExecutionManager CreateDatabaseSnapshot(int sequenceNumber);

    internal abstract void DeleteDatabaseBackup(int? sequenceNumber = null);

    public abstract IScriptExecutionManager DeleteDatabaseSnapshot(int? sequenceNumber = null);

    internal abstract void RestoreDatabaseBackup(int sequenceNumber);

    public abstract IScriptExecutionManager RestoreDatabaseBackup(string backupFilePath);

    public abstract IScriptExecutionManager RestoreDatabaseSnapshot(int sequenceNumber);



    public virtual IScriptExecutionManager BulkInsertScripts(IEnumerable<Script> scripts)
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                BulkInsertTableScripts(scripts);
                break;
            case DataSourceType.Csv:
                BulkInsertCsvScripts(scripts);
                break;
            case DataSourceType.Json:
                BulkInsertJsonScripts(scripts);
                break;
        }

        return this;
    }

    internal void BulkInsertCsvScripts(IEnumerable<ScriptHistory> scripts)
    {
        List<string> existingScripts = new List<string>(DataSource.ReadAllLines());

        foreach (ScriptHistory script in scripts)
        {
            script.Validate(SourceType, DatabaseType);

            existingScripts.Add(script.ToCsvEntry());
        }

        DataSource.WriteAllLines(existingScripts.ToArray());
    }

    internal void BulkInsertJsonScripts(IEnumerable<ScriptHistory> scripts)
    {
        _ = Parallel.ForEach(scripts, Validate);

        List<ScriptHistory>? existingScripts = JsonSerializer.Deserialize<List<ScriptHistory>>(DataSource.ReadAllText(), SourceGenerationContext.Default.ListScriptHistory);

        if (scripts.TryGetNonEnumeratedCount(out int count) is false) count = scripts.Count();

        existingScripts?.AddRange(scripts);

        string json = JsonSerializer.Serialize<List<ScriptHistory>>(existingScripts!, SourceGenerationContext.Default.ListScriptHistory);

        DataSource.WriteAllText(json);

        void Validate(ScriptHistory script)
        {
            script.Validate(SourceType, DatabaseType);
        }
    }

    internal abstract void BulkInsertTableScripts(IEnumerable<ScriptHistory> scripts);

    public virtual IScriptExecutionManager BulkUpsertScripts(IEnumerable<Script> scripts)
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                BulkUpsertTableScripts(scripts);
                break;
            case DataSourceType.Csv:
                BulkUpsertCsvScripts(scripts);
                break;
            case DataSourceType.Json:
                BulkUpsertJsonScripts(scripts);
                break;
            default: break;
        }

        return this;
    }

    internal void BulkUpsertCsvScripts(IEnumerable<ScriptHistory> scripts)
    {
        if (scripts.TryGetNonEnumeratedCount(out int count) is false) count = scripts.Count();

        string[] csvBuilder = new string[count + 1];

        csvBuilder[0] = $"{nameof(ScriptHistory.SequenceNumber)},{nameof(ScriptHistory.Author)},{nameof(ScriptHistory.Description)},{nameof(ScriptHistory.Status)},{nameof(ScriptHistory.CreatedDate)}".AsCached();

        for (int i = 0; i < count; i++)
        {
            ScriptHistory script = scripts.ElementAt(i);

            script.Validate(SourceType, DatabaseType);

            csvBuilder[i + 1] = script.ToCsvEntry();
        }

        DataSource.WriteAllLines(csvBuilder);
    }

    internal void BulkUpsertJsonScripts(IEnumerable<ScriptHistory> scripts)
    {
        _ = Parallel.ForEach(scripts, Validate);

        string json = JsonSerializer.Serialize(scripts, SourceGenerationContext.Default.IEnumerableScriptHistory);

        DataSource.WriteAllText(json);

        void Validate(ScriptHistory script)
        {
            script.Validate(SourceType, DatabaseType);
        }
    }

    internal abstract void BulkUpsertTableScripts(IEnumerable<ScriptHistory> scripts);

    public IScriptExecutionManager ChangeScriptsLocation(IPathInfo path)
    {
        ScriptsLocation = path;

        return this;
    }

    public Script GetLatestScript()
    {
        return Scripts?.OrderByDescending(static x => x.SequenceNumber)
                       .FirstOrDefault(static x => x.Status == ScriptStatusType.SUCCESS)!;
    }

    internal Span<IPathInfo> GetScriptFiles()
    {
        Span<IPathInfo> scriptFiles = ScriptsLocation?.GetDirectoryFiles(ScriptsLocation.FullPath, $"*{ScriptExtension}")!;

        scriptFiles.Sort(FileComparer);

        return scriptFiles;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public abstract ScriptStatusType GetScriptStatus(int sequenceNumber);

    internal IScriptExecutionManager InitializeScripts(IEnumerable<Script> scripts)
    {
        ILogger? logger = LoggerFactory?.CreateLogger(Constants.Initialize);

        DbConnection = CreateDbConnection();

        try
        {
            DbConnection?.Open();

            if (scripts.TryGetNonEnumeratedCount(out int count) is false) count = scripts.Count();

            if (count <= UPSERT_SCRIPT_THRESHOLD)
            {
                if (ExecutionProgress is not null)
                {
                    Progress ??= new ExecutionProgress();
                    Progress.Current = 0;
                    Progress.Total = count;
                }

                int progress = 0;
                foreach (Script script in scripts)
                {
                    bool isExisting = ScriptExists(script.SequenceNumber);

                    if (isExisting is false)
                    {
                        _ = InsertScript(script);

                        script.IsAlreadyRan = true;

                        logger?.LogScript(script);
                    }

                    Progress.Current = progress++;
                    ExecutionProgress?.Report(Progress);
                }
            }
            else
            {
                _ = BulkInsertScripts(scripts);
            }

            LastExecutedSequence = GetLatestSequence();
        }
        catch (Exception)
        {
            logger?.LogMessage(LogLevel.Error, "Failed to initialize scripts.");
            throw;
        }
        finally
        {
            DbConnection?.Dispose();
        }

        return this;
    }

    internal void LoadScripts(int sequenceNumber)
    {
        if (Scripts is null) Scripts = new SortedSet<Script>(ScriptComparer);
        else if (Scripts.Count > 0) Scripts.Clear();

        Span<IPathInfo> files = GetScriptFiles();

        for (int i = 0; i < files.Length; i++)
        {
            if (CancelToken?.IsCancellationRequested is true) break;

            ref IPathInfo file = ref files[i];

            bool isAlreadyRan = file.TryGetSequenceNumber(out int number) && (number <= sequenceNumber);

            if (isAlreadyRan is false) break;

            ref Script? script = ref Unsafe.AsRef(ScriptCreator?.Create(file));

            if (script is not null)
            {
                script.Status = ScriptStatusType.SUCCESS;

                _ = Scripts.Add(script);
            }
        }
    }

    public virtual IScriptExecutionManager InsertScript(ScriptHistory script)
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                InsertTableScript(script);
                break;
            case DataSourceType.Csv:
                InsertCsvScript(script);
                break;
            case DataSourceType.Json:
                InsertJsonScript(script);
                break;
        }

        return this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void InsertCsvScript(ScriptHistory script)
    {
        script.Validate(SourceType, DatabaseType);

        DataSource.AppendLine(script.ToCsvEntry());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void InsertJsonScript(ScriptHistory script)
    {
        script.Validate(SourceType, DatabaseType);

        if (EnvironmentType is ServerEnvironmentType.Local || EnvironmentType is ServerEnvironmentType.Remote)
        {
            DataSource.Edit(script.ToJsonEntry(), CompiledDelegates.EditLocalJsonFile);
        }
        else if (EnvironmentType is ServerEnvironmentType.Docker)
        {
            DataSource.Edit(script.ToJsonEntry(), CompiledDelegates.EditDockerJsonFile);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal abstract void InsertTableScript(ScriptHistory script);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public virtual IScriptExecutionManager UpdateScript(ScriptHistory script)
    {
        switch (SourceType)
        {
            case DataSourceType.Internal:
                UpdateTableScript(script);
                break;
            case DataSourceType.Csv:
                UpdateCsvScript(script);
                break;
            case DataSourceType.Json:
                UpdateJsonScript(script);
                break;
        }

        return this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void UpdateCsvScript(ScriptHistory script)
    {
        script.Validate(SourceType, DatabaseType);

        string[] csvLines = DataSource.ReadAllLines();

        Span<string> lines = csvLines.AsSpan();

        for (int i = 1; i < lines.Length; i++)
        {
            ref string line = ref lines[i];

            if (GetCsvSequenceNumber(line) == script.SequenceNumber)
            {
                line = script.ToCsvEntry();

                break;
            }
        }

        DataSource.WriteAllLines(csvLines);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void UpdateJsonScript(ScriptHistory script)
    {
        script.Validate(SourceType, DatabaseType);

        string[] jsonLines = DataSource.ReadAllLines();

        Span<string> lines = jsonLines.AsSpan();

        for (int i = 1; i <= lines.Length - 2; i++)
        {
            ref string line = ref lines[i];

            if (GetJsonSequenceNumber(line) == script.SequenceNumber)
            {
                line = script.ToJsonEntry(i != lines.Length - 2);

                break;
            }
        }

        DataSource.WriteAllLines(jsonLines);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal abstract void UpdateTableScript(ScriptHistory script);

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public abstract bool ScriptExists(int sequenceNumber);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public virtual IScriptExecutionManager UpsertScript(ScriptHistory script)
    {
        bool isExisting = ScriptExists(script.SequenceNumber);

        if (isExisting)
        {
            _ = UpdateScript(script);
        }
        else
        {
            _ = InsertScript(script);
        }

        return this;
    }



    public IScriptExecutionManager CancelExecution()
    {
        CancelSource?.Cancel();

        return this;
    }

    public IScriptExecutionManager ChangeExecutionRunType(ExecutionRunType executionType)
    {
        ExecutionType = executionType;

        return this;
    }

    public static IScriptExecutionManager Create(in ExecutionConfiguration config, ILoggerFactory logger, in CancellationToken? cancelToken = null!)
    {
        return config.DatabaseType switch
        {
            DataProviderType.MSSQLServer => new MSSQLManager(in config, logger, cancelToken),
            DataProviderType.SQLLite => throw new NotImplementedException("Support for SQLite not yet available"),
            _ => null!
        };
    }

    public IScriptExecutionManager ExecutionResults(out ScriptExecutionResults executionResult)
    {
        executionResult = new ScriptExecutionResults(Scripts);

        return this;
    }

    public IScriptExecutionManager Initialize(int? sequenceNumber = null!, in ScriptCreator? scriptCreator = null!)
    {
        SetCancellationToken();

        if (sequenceNumber.HasValue) LastExecutedSequence = sequenceNumber.Value;

        if (scriptCreator.HasValue) ScriptCreator = scriptCreator.Value;
        else ScriptCreator ??= new ScriptCreator();

        LoadScripts(LastExecutedSequence);

        return InitializeScripts(Scripts);
    }

    public IScriptExecutionManager QuickInitialize(int? sequenceNumber = null!, in ScriptCreator? scriptCreator = null!)
    {
        SetCancellationToken();

        if (sequenceNumber.HasValue) LastExecutedSequence = sequenceNumber.Value;

        if (scriptCreator.HasValue) ScriptCreator = scriptCreator.Value;
        else ScriptCreator ??= new ScriptCreator();

        ILogger? logger = LoggerFactory?.CreateLogger(Constants.Initialize);

        DbConnection = CreateDbConnection();

        try
        {
            if (LastExecutedSequence >= UPSERT_SCRIPT_THRESHOLD || OptimizationType is ExecutionOptimizationType.Super)
            {
                LoadScripts(LastExecutedSequence);

                Scripts.AsParallel().ForAll(static x => x.IsAlreadyRan = true);

                _ = BulkInsertScripts(Scripts);
            }
            else
            {
                if (Scripts is null) Scripts = new SortedSet<Script>(ScriptComparer);
                else if (Scripts.Count > 0) Scripts.Clear();

                Span<IPathInfo> files = GetScriptFiles();

                if (ExecutionProgress is not null)
                {
                    Progress ??= new ExecutionProgress();
                }

                for (int i = 0; i < files.Length; i++)
                {
                    if (CancelToken?.IsCancellationRequested is true) break;

                    ref IPathInfo file = ref files[i];

                    bool isAlreadyRan = file!.TryGetSequenceNumber(out int number) && (number <= LastExecutedSequence);

                    if (isAlreadyRan is false) break;

                    ref Script? script = ref Unsafe.AsRef(ScriptCreator?.Create(file!));

                    if (script is not null)
                    {
                        script.Status = ScriptStatusType.SUCCESS;

                        bool isExisting = ScriptExists(script.SequenceNumber);

                        if (isExisting is false)
                        {
                            _ = InsertScript(script);

                            script.IsAlreadyRan = isAlreadyRan;

                            _ = Scripts.Add(script);

                            logger?.LogScript(script);
                        }
                    }

                    Progress.Current = i;
                    ExecutionProgress?.Report(Progress);
                }
            }

            LastExecutedSequence = GetLatestSequence();
        }
        catch (Exception)
        {
            logger?.LogMessage(LogLevel.Warning, "Failed to initialize scripts.");
            throw;
        }
        finally
        {
            DbConnection?.Dispose();
        }

        return this;
    }

    public IScriptExecutionManager Run(Script script, ExecutionRunType? executionType = null!)
    {
        SetCancellationToken();

        if (executionType.HasValue) script.ExecutionType = executionType.Value;

        ILogger? logger = LoggerFactory?.CreateLogger(Constants.Run);

        if (script.ExecutionType is ExecutionRunType.ScanOnly)
        {
            CancelSource?.Cancel();
            throw new Exception("Cannot execute script when execution mode is set to scan.");
        }

        try
        {
            DbConnection = CreateDbConnection();

            _ = TryOpenDBConnection();

            script.Status = GetScriptStatus(script.SequenceNumber);

            EventId eventId = new EventId(script.SequenceNumber, script.Description);

            this.ExecuteScript(ref script, logger!);
        }
        catch (Exception)
        {
            logger?.LogError("Failed to complete run.");
            CancelSource?.Cancel();
            throw;
        }
        finally
        {
            DbConnection?.Dispose();

            OnExecutionStarted?.Invoke(this, script);
        }

        return this;
    }

    public IScriptExecutionManager Run(ExecutionRunType? executionType = null!)
    {
        SetCancellationToken();

        if (executionType.HasValue) ExecutionType = executionType.Value;

        if (ExecutionType is ExecutionRunType.DefaultRun)
        {
            DbConnection = CreateDbConnection();
            DefaultRunScripts();
            LastExecutedSequence = GetLatestScript().SequenceNumber;
        }
        else if (ExecutionType is ExecutionRunType.TestRun)
        {
            DbConnection = CreateDbConnection();
            TestRunScripts();
        }
        else
        {
            CancelSource?.Cancel();
            throw new Exception("Cannot execute scripts when execution mode is not set to Run or Test Run.");
        }

        return this;

        void DefaultRunScripts()
        {
            try
            {
                _ = TryOpenDBConnection();

                RunScripts();
            }
            catch (Exception)
            {
                Logger?.LogError("Failed to complete run.");
                CancelSource?.Cancel();
                throw;
            }
            finally
            {
                DbConnection?.Dispose();
            }
        }

        void TestRunScripts()
        {
            try
            {
                _ = CreateDataSourceBackup(LastExecutedSequence);

                _ = TryOpenDBConnection();

                RunScripts();
            }
            catch (Exception)
            {
                Logger?.LogError("Failed to complete test scan.");
                CancelSource?.Cancel();
                throw;
            }
            finally
            {
                DbConnection?.Dispose();

                _ = RestoreDataSourceBackup(LastExecutedSequence);

                _ = DeleteDataSourceBackup(LastExecutedSequence);
            }
        }
    }

    public IScriptExecutionManager RunOptimized(ExecutionRunType? executionType = null!, in ScriptCreator? scriptCreator = null!)
    {
        SetCancellationToken();

        if (executionType.HasValue) ExecutionType = executionType.Value;

        if (scriptCreator.HasValue) ScriptCreator = scriptCreator.Value;
        else ScriptCreator ??= new ScriptCreator();

        if (ExecutionType is ExecutionRunType.DefaultRun)
        {
            DbConnection = CreateDbConnection();
            DefaultScanRunScripts();
            LastExecutedSequence = GetLatestScript().SequenceNumber;
        }
        else if (ExecutionType is ExecutionRunType.TestRun)
        {
            DbConnection = CreateDbConnection();
            TestScanRunScripts();
        }
        else
        {
            CancelSource?.Cancel();
            throw new Exception("Cannot execute scripts when execution mode is not set to Run or Test Run.");
        }

        return this;

        void DefaultScanRunScripts()
        {
            try
            {
                _ = TryOpenDBConnection();

                if (OptimizationType is ExecutionOptimizationType.Quick)
                {
                    QuickRunScripts();
                }
                else if (OptimizationType is ExecutionOptimizationType.Super)
                {
                    SuperRunScripts();
                }
            }
            catch (Exception)
            {
                Logger?.LogError("Failed to complete run.");
                CancelSource?.Cancel();
                throw;
            }
            finally
            {
                DbConnection?.Dispose();
            }
        }

        void TestScanRunScripts()
        {
            try
            {
                _ = CreateDataSourceBackup(LastExecutedSequence);

                _ = TryOpenDBConnection();

                if (OptimizationType is ExecutionOptimizationType.Quick)
                {
                    QuickRunScripts();
                }
                else if (OptimizationType is ExecutionOptimizationType.Super)
                {
                    SuperRunScripts();
                }
            }
            catch (Exception)
            {
                Logger?.LogError("Failed to complete test scan.");
                CancelSource?.Cancel();
                throw;
            }
            finally
            {
                DbConnection?.Dispose();

                _ = RestoreDataSourceBackup(LastExecutedSequence);

                _ = DeleteDataSourceBackup(LastExecutedSequence);
            }
        }
    }

    public IScriptExecutionManager Scan(in ScriptCreator? scriptCreator = null!)
    {
        SetCancellationToken();

        if (scriptCreator.HasValue) ScriptCreator = scriptCreator.Value;
        else ScriptCreator ??= new ScriptCreator();

        try
        {
            DbConnection = CreateDbConnection();

            _ = TryOpenDBConnection();

            ScanScripts();
        }
        catch (Exception)
        {
            Logger?.LogError("Failed to complete scan.");
            CancelSource?.Cancel();
            throw;
        }
        finally
        {
            DbConnection?.Dispose();
        }

        return this;
    }



    internal void ScanScripts()
    {
        ILogger? logger = LoggerFactory?.CreateLogger(Constants.Scan);

        Span<IPathInfo> scriptFiles = GetScriptFiles();

        if (scriptFiles.IsEmpty)
        {
            CancelSource?.Cancel();
            throw new FileNotFoundException($"No {ScriptExtension} files(s) were found in directory: {ScriptsLocation.FullPath}");
        }

        Scripts ??= new SortedSet<Script>(ScriptComparer);

        if (Scripts.Count > 0) Scripts.Clear();

        ref int totalFiles = ref Unsafe.AsRef(scriptFiles.Length);

        ref int fileCount = ref Unsafe.AsRef(0);

        if (ExecutionProgress is not null)
        {
            Progress ??= new ExecutionProgress();
            Progress.Current = fileCount;
            Progress.Total = totalFiles;
        }

        for (fileCount += 1; fileCount <= totalFiles; fileCount++)
        {
            if (CancelToken?.IsCancellationRequested is true) break;

            ref Script? script = ref Unsafe.AsRef(ScriptCreator?.Create(scriptFiles[fileCount - 1]))!;

            if (script is not null)
            {
                OnScanStarted?.Invoke(this, script);

                script.Status = GetScriptStatus(script.SequenceNumber);

                if (script.Status is ScriptStatusType.SUCCESS) script.IsAlreadyRan = true;

                _ = Scripts.Add(script);

                logger?.LogScript(script!);

                OnScanEnded?.Invoke(this, script);
            }

            Progress.Current = fileCount;
            ExecutionProgress?.Report(Progress);
        }
    }

    internal void RunScripts()
    {
        ILogger? logger = LoggerFactory?.CreateLogger(ExecutionType is ExecutionRunType.DefaultRun ? Constants.Run : Constants.TestRun);

        if (Scripts.Count is 0)
        {
            CancelSource?.Cancel();
            throw new Exception("No script files were scanned.");
        }

        ref int totalScripts = ref Unsafe.AsRef(Scripts.Count);

        ref int scriptCount = ref Unsafe.AsRef(0);

        if (ExecutionProgress is not null)
        {
            Progress ??= new ExecutionProgress();
            Progress.Current = scriptCount;
            Progress.Total = totalScripts;
        }

        bool isBulkScript = totalScripts >= UPSERT_SCRIPT_THRESHOLD;

        for (scriptCount += 1; scriptCount <= totalScripts; scriptCount++)
        {
            if (CancelToken?.IsCancellationRequested is true) break;

            ref Script script = ref Unsafe.AsRef(Scripts.ElementAt(scriptCount - 1));

            if (script is not null)
            {
                script.Status = GetScriptStatus(script.SequenceNumber);

                if (ExecutionType is ExecutionRunType.TestRun) script.ExecutionType = ExecutionRunType.DefaultRun;

                if (script.SequenceNumber <= LastExecutedSequence) script.Status = ScriptStatusType.SUCCESS;

                if (script.Status is ScriptStatusType.SUCCESS) script.IsAlreadyRan = true;

                OnExecutionStarted?.Invoke(this, script);

                if (script.Status is ScriptStatusType.NONE || script.Status is ScriptStatusType.FAIL)
                {
                    long startTime = Stopwatch.GetTimestamp();

                    this.ExecuteScript(ref script, logger!);

                    if (ExecutionType is ExecutionRunType.DefaultRun && isBulkScript is false)
                    {
                        _ = UpsertScript(script);
                    }

                    script.ExecutionTime = Stopwatch.GetElapsedTime(startTime);
                }

                logger?.LogScript(script);

                OnExecutionEnded?.Invoke(this, script);
            }

            Progress.Current = scriptCount;
            ExecutionProgress?.Report(Progress);
        }

        if (ExecutionType is ExecutionRunType.DefaultRun && isBulkScript)
        {
            _ = BulkUpsertScripts(Scripts);
        }
    }

    internal void QuickRunScripts()
    {
        ILogger? logger = LoggerFactory?.CreateLogger(ExecutionType is ExecutionRunType.DefaultRun ? Constants.Run : Constants.TestRun);

        Span<IPathInfo> scriptFiles = GetScriptFiles();

        if (scriptFiles.IsEmpty)
        {
            CancelSource?.Cancel();
            throw new FileNotFoundException($"No {ScriptExtension} files(s) were found in directory: {ScriptsLocation.FullPath}");
        }

        Scripts ??= new SortedSet<Script>(ScriptComparer);

        if (Scripts.Count > 0)
        {
            scriptFiles = scriptFiles.Slice(Scripts.Count);
        }

        ref int totalFiles = ref Unsafe.AsRef(scriptFiles.Length);

        ref int fileCount = ref Unsafe.AsRef(0);

        if (ExecutionProgress is not null)
        {
            Progress ??= new ExecutionProgress();
            Progress.Current = fileCount;
            Progress.Total = totalFiles;
        }

        bool isBulkScript = totalFiles >= UPSERT_SCRIPT_THRESHOLD;

        for (fileCount += 1; fileCount <= totalFiles; fileCount++)
        {
            if (CancelToken?.IsCancellationRequested is true) break;

            ref Script? script = ref Unsafe.AsRef(ScriptCreator?.Create(scriptFiles[fileCount - 1]))!;

            if (script is not null)
            {
                script.Status = GetScriptStatus(script.SequenceNumber);

                if (script.Status is not ScriptStatusType.NONE) continue;

                if (ExecutionType is ExecutionRunType.TestRun) script.ExecutionType = ExecutionRunType.DefaultRun;

                OnExecutionStarted?.Invoke(this, script);

                long startTime = Stopwatch.GetTimestamp();

                this.ExecuteScript(ref script, logger!);

                if (ExecutionType is ExecutionRunType.DefaultRun && isBulkScript is false)
                {
                    _ = UpsertScript(script);
                }

                script.ExecutionTime = Stopwatch.GetElapsedTime(startTime);

                logger?.LogScript(script);

                OnExecutionEnded?.Invoke(this, script);

                _ = Scripts.Add(script);
            }

            Progress.Current = fileCount;
            ExecutionProgress?.Report(Progress);
        }

        if (ExecutionType is ExecutionRunType.DefaultRun && isBulkScript)
        {
            _ = BulkUpsertScripts(Scripts);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal void SuperRunScripts()
    {
        ILogger? logger = LoggerFactory?.CreateLogger(ExecutionType is ExecutionRunType.DefaultRun ? Constants.Run : Constants.TestRun);

        Span<IPathInfo> scriptFiles = GetScriptFiles();

        if (scriptFiles.IsEmpty)
        {
            CancelSource?.Cancel();
            throw new FileNotFoundException($"No {ScriptExtension} files(s) were found in directory: {ScriptsLocation.FullPath}");
        }

        Scripts ??= new SortedSet<Script>(ScriptComparer);

        if (Scripts.Count > 0)
        {
            scriptFiles = scriptFiles.Slice(Scripts.Count);
        }

        ref int totalFiles = ref Unsafe.AsRef(scriptFiles.Length);

        ref int fileCount = ref Unsafe.AsRef(0);

        if (ExecutionProgress is not null)
        {
            Progress ??= new ExecutionProgress();
            Progress.Current = fileCount;
            Progress.Total = totalFiles;
        }

        for (fileCount += 1; fileCount <= totalFiles; fileCount++)
        {
            if (CancelToken?.IsCancellationRequested is true) break;

            ref Script? script = ref Unsafe.AsRef(ScriptCreator?.Create(scriptFiles[fileCount - 1]))!;

            if (script is not null)
            {
                script.Status = GetScriptStatus(script.SequenceNumber);

                if (script.Status is not ScriptStatusType.NONE) continue;

                OnExecutionStarted?.Invoke(this, script);

                long startTime = Stopwatch.GetTimestamp();

                if (ExecutionType is ExecutionRunType.TestRun) script.ExecutionType = ExecutionRunType.DefaultRun;

                this.ExecuteScript(ref script, logger!);

                script.ExecutionTime = Stopwatch.GetElapsedTime(startTime);

                OnExecutionEnded?.Invoke(this, script);

                logger?.LogScript(script);

                _ = Scripts.Add(script);
            }

            Progress.Current = fileCount;
            ExecutionProgress?.Report(Progress);
        }

        if (ExecutionType is ExecutionRunType.DefaultRun)
        {
            _ = BulkUpsertScripts(Scripts);
        }
    }
}