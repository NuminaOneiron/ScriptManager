using System.Buffers;
using System.Diagnostics;

using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

using Humanizer;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Primitives;

using NReco.Logging.File;

using ScriptManager;
using ScriptManager.Enums;

using Spectre.Console;
using Spectre.Console.Rendering;

#region DEBUG TEST MODE

#if DEBUG

(string Database, string Folder, string Sequence, string Backup)[] databases = new (string Database, string Folder, string Sequence, string Backup)[3]
{
    /*0*/("FDSTenantSpace", "TENANT", "10", "/var/opt/mssql/backup/FDSTenantSpace-Mod10.bak"),
    /*1*/("FCMain", "FCMAIN", "185", "/var/opt/mssql/backup/FCMain185.bak"),
    /*2*/("IIMS", "IIMS","156", "/var/opt/mssql/backup/IIMS-Mod156.bak")
};

string[] dataSources = new string[3]
{
    /*0*/"internal",
    /*1*/"csv",
    /*2*/"json"
};

(string Database, string Folder, string Sequence, string Backup) database = databases[1];
string dataSource = dataSources[0];

args = new string[] { "-st", "-e", "docker container=IIMS", "-s", "localhost,1433", "-d", database.Database, "-u", "sa", "-p", "Password123*", "-v", "sql", "-c", dataSource, "-l", database.Sequence, "-o", ".", "-f", $"C:\\Users\\Administrator\\source\\Workspaces\\IIMS\\Scripts\\{database.Folder}\\In" };

Console.WriteLine("DEBUG TEST MODE");
Console.WriteLine(string.Join(" ", args));

#endif 

#endregion

PrintHeader();

if (ValidateArguments() is false) return;

ILoggerFactory logger = default!;
IScriptExecutionManager scriptExecutionManager = default!;

LogLevel logLevel = GetMinimalLogLevel();
bool hideLogs = logLevel > LogLevel.Information;
Stopwatch timer = Stopwatch.StartNew();

try
{
    logger = GetLoggerFactory();

    ScriptManagerConfig config = new ScriptManagerConfig(args, logger?.CreateLogger("[Configuration]")!);

    if (string.IsNullOrEmpty(config.LogsLocation) is false) logger?.AddProvider(GetFileLogger(config));

    scriptExecutionManager = ScriptExecutionManager.Create(GetExecutionConfiguration(config), logger!);

#if DEBUG
    scriptExecutionManager.DeleteDataSource().RestoreDatabaseBackup(database.Backup);
#endif
    Setup();
    Execute();
}
catch (Exception ex)
{
    scriptExecutionManager?.CancelExecution();
    PrintException(ex);
}
finally
{
    if (timer.IsRunning) timer.Stop();

#if DEBUG
    scriptExecutionManager?.DeleteDataSource().RestoreDatabaseBackup(database.Backup);
#endif

    scriptExecutionManager?.Dispose();

    logger?.Dispose();

    GC.Collect();
}



bool ValidateArguments()
{
    if (args.Length is 0)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Markup("Use [lime]/help[/] or [lime]/?[/] to view the help documentation."));
        AnsiConsole.WriteLine();
        return false;
    }
    else if (args.Length is 1)
    {
        if (args[0] is "/help" or "/?") PrintHelpDocumentation();
        AnsiConsole.WriteLine();
        return false;
    }

    return true;
}

LogLevel GetMinimalLogLevel()
{
    LogLevel logLevel = LogLevel.Information;

    bool hideLogs = args.Contains(ArgumentDefinitions.h) || args.Contains(ArgumentDefinitions.hide);

    bool hideAllLogs = args.Contains(ArgumentDefinitions.ha) || args.Contains(ArgumentDefinitions.hideall);

    if (hideLogs)
    {
        logLevel = LogLevel.Warning;
    }

    if (hideAllLogs)
    {
        logLevel = LogLevel.None;
    }

    return logLevel;
}

ILoggerFactory GetLoggerFactory()
{
    ILoggerFactory loggerFactory = default!;

    if (hideLogs)
    {
        const string consoleProvider = "Microsoft.Extensions.Logging.Console.ConsoleLoggerProvider";

        if (logLevel is LogLevel.Warning)
        {
            //loggerFactory = LoggerFactory.Create(static builder => builder.ClearProviders()
            loggerFactory = LoggerFactory.Create(static builder => builder.ClearProviders().AddConsole(static options => options.FormatterName = nameof(ConsoleFormatter)).AddConsoleFormatter<ConsoleFormatter, ConsoleFormatterOptions>()
            .AddFilter(static (provider, _, level) =>
            {
                if (provider is consoleProvider && level < LogLevel.Warning)
                {
                    return false;
                }
                return true;
            }));
        }

        if (logLevel is LogLevel.None)
        {
            //loggerFactory = LoggerFactory.Create(static builder => builder.ClearProviders().AddConsole()
            loggerFactory = LoggerFactory.Create(static builder => builder.ClearProviders().AddConsole(static options => options.FormatterName = nameof(ConsoleFormatter)).AddConsoleFormatter<ConsoleFormatter, ConsoleFormatterOptions>()
            .AddFilter(static (provider, _, level) =>
            {
                if (provider is consoleProvider && level < LogLevel.None)
                {
                    return false;
                }
                return true;
            }));
        }
    }
    else
    {
        //loggerFactory = LoggerFactory.Create(static builder => builder.ClearProviders().AddConsole());
        loggerFactory = LoggerFactory.Create(static builder => builder.ClearProviders().AddConsole(static options => options.FormatterName = nameof(ConsoleFormatter)).AddConsoleFormatter<ConsoleFormatter, ConsoleFormatterOptions>());
    }

    return loggerFactory;
}

ILoggerProvider GetFileLogger(ScriptManagerConfig config)
{
    string? basePath = config.LogsLocation;

    char[] forbiddenChars = new char[8] { '\\', '/', ':', '*', '?', '"', '<', '>' };

    string filepart1 = string.Concat($"{config.Server}-{config.Database}_{Environment.UserDomainName}-{Environment.UserName}".SkipWhile(x => forbiddenChars.Contains(x)));

    string filepart2 = $"_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.sm.log";

    string filePath = Path.Combine(basePath!, string.Concat(filepart1, filepart2));


    static string LogMessage(LogMessage message)
    {
        using ValueStringBuilder stringBuilder = new ValueStringBuilder(120);
        stringBuilder.Append(string.Intern("["));
        stringBuilder.Append(string.Intern(Enum.GetName(message.LogLevel)!));
        stringBuilder.Append(string.Intern("]"));

        stringBuilder.Append(string.Intern("::"));

        stringBuilder.Append(string.Intern("["));
        stringBuilder.Append(message.LogName);
        stringBuilder.Append(string.Intern("]"));

        stringBuilder.Append(string.Intern(" => "));

        stringBuilder.Append(message.Message);

        if (message.Exception is not null)
        {
            stringBuilder.Append(Environment.NewLine);
            stringBuilder.Append(message.Exception.ToString());
        }

        return stringBuilder.ToString();
    }

    return new FileLoggerProvider(filePath, new FileLoggerOptions() { UseUtcTimestamp = false, Append = true, FormatLogEntry = LogMessage });
}

ExecutionConfiguration GetExecutionConfiguration(ScriptManagerConfig config)
{
    return new ExecutionConfiguration
    {
        Database = config.Database,
        DatabaseType = config.DatabaseType!.Value,
        DataSourceInfo = config.DataSourceInfo!.Value,
        EnvironmentType = config.EnvironmentType!.Value,
        ExecutionType = config.ExecutionType!.Value,
        OptimizationType = config.OptimizationType!.Value,
        Password = config.Password,
        ScriptExtension = config.ScriptExtension,
        ScriptsLocationInfo = config.ScriptsLocationInfo!.Value,
        SequenceNumber = config.SequenceNumber!.Value,
        Server = config.Server,
        SourceType = config.SourceType!.Value,
        ThreadType = config.ThreadType!.Value,
        Username = config.Username
    };
}



void Setup()
{
    scriptExecutionManager.ExecutionProgress = new Progress<ExecutionProgress>(OnExecutionProgress);

    if (hideLogs is false && scriptExecutionManager.OptimizationType is not ExecutionOptimizationType.Super)
    {
        scriptExecutionManager.OnExecutionStarted += OnExecutionStarted;
        scriptExecutionManager.OnExecutionEnded += OnExecutionEnded;
    }
}

void Execute()
{
    switch (scriptExecutionManager.ExecutionType)
    {
        case ExecutionRunType.DefaultRun or ExecutionRunType.TestRun or ExecutionRunType.ScanOnly:
            _ = scriptExecutionManager.CreateDataSource();
            if (scriptExecutionManager.LastExecutedSequence > 0) Initialize();
            Run();
            break;
        case ExecutionRunType.CreateBackup or ExecutionRunType.CreateSnapshot:
            Backup();
            break;
        case ExecutionRunType.DeleteData or ExecutionRunType.DeleteBackup or ExecutionRunType.DeleteSnapshot:
            Delete();
            break;
        case ExecutionRunType.RestoreData or ExecutionRunType.RestoreBackup or ExecutionRunType.RestoreSnapshot:
            Restore();
            break;
        default: break;
    }
}

void Initialize()
{
    try
    {
        if (hideLogs) StatusHelper.StartStatus("Initializing Scripts:");

        switch (scriptExecutionManager.OptimizationType is ExecutionOptimizationType.Quick || scriptExecutionManager.OptimizationType is ExecutionOptimizationType.Super)
        {
            case true: _ = scriptExecutionManager.QuickInitialize(); break;
            case false: _ = scriptExecutionManager.Initialize(); break;
        }
    }
    catch (Exception ex)
    {
        scriptExecutionManager?.CancelExecution();
        PrintException(ex);
    }
    finally
    {
        if (hideLogs) StatusHelper.StopStatus();
    }
}

void Run()
{
    bool isExceptionCaught = false;

    try
    {
        scriptExecutionManager.ScriptCreator = new ScriptCreator();

        if (scriptExecutionManager.OptimizationType is ExecutionOptimizationType.Quick || scriptExecutionManager.OptimizationType is ExecutionOptimizationType.Super)
        {
            if (hideLogs) ProgressHelper.StartProgress(GetExecutionName(scriptExecutionManager.ExecutionType, scriptExecutionManager.OptimizationType)!);
            scriptExecutionManager.RunOptimized();
        }
        else
        {
            if (hideLogs is false)
            {
                switch (scriptExecutionManager.ExecutionType)
                {
                    case ExecutionRunType.DefaultRun or ExecutionRunType.TestRun: _ = scriptExecutionManager.Scan().Run(); break;
                    case ExecutionRunType.ScanOnly: _ = scriptExecutionManager.Scan(); break;
                    default: break;
                }
            }
            else
            {
                switch (scriptExecutionManager.ExecutionType)
                {
                    case ExecutionRunType.DefaultRun:
                        ProgressHelper.StartProgress(GetExecutionName(ExecutionRunType.ScanOnly, scriptExecutionManager.OptimizationType)!);
                        _ = scriptExecutionManager.Scan();
                        ProgressHelper.StartProgress(GetExecutionName(scriptExecutionManager.ExecutionType, scriptExecutionManager.OptimizationType)!);
                        _ = scriptExecutionManager.Run();
                        break;
                    case ExecutionRunType.TestRun:
                        ProgressHelper.StartProgress(GetExecutionName(ExecutionRunType.ScanOnly, scriptExecutionManager.OptimizationType)!);
                        _ = scriptExecutionManager.Scan();
                        ProgressHelper.StartProgress(GetExecutionName(scriptExecutionManager.ExecutionType, scriptExecutionManager.OptimizationType)!);
                        _ = scriptExecutionManager.Run();
                        break;
                    case ExecutionRunType.ScanOnly:
                        ProgressHelper.StartProgress(GetExecutionName(scriptExecutionManager.ExecutionType, scriptExecutionManager.OptimizationType)!);
                        _ = scriptExecutionManager.Scan();
                        break;
                    default: break;
                }
            }
        }
    }
    catch (Exception ex)
    {
        scriptExecutionManager?.CancelExecution();
        PrintException(ex);
        isExceptionCaught = true;
    }
    finally
    {
        scriptExecutionManager.ExecutionResults(out ScriptExecutionResults results);

        timer.Stop();

        PrintExecutionTime();

        if (isExceptionCaught is false) PrintExecutionResults(results);
    }
}

void Delete()
{
    try
    {
        switch (scriptExecutionManager.ExecutionType)
        {
            case ExecutionRunType.DeleteData:
                _ = scriptExecutionManager
                    .DeleteExistingDataSource()
                    .DeleteDataSource()
                    .DeleteDatabaseSnapshot()
                    .DeleteDataSourceBackup();
                break;
            case ExecutionRunType.DeleteBackup:
                if (scriptExecutionManager.LastExecutedSequence > 0) _ = scriptExecutionManager.DeleteDataSourceBackup(scriptExecutionManager.LastExecutedSequence);
                else _ = scriptExecutionManager.DeleteDataSourceBackup();
                break;
            case ExecutionRunType.DeleteSnapshot:
                if (scriptExecutionManager.LastExecutedSequence > 0) _ = scriptExecutionManager.DeleteDatabaseSnapshot(scriptExecutionManager.LastExecutedSequence);
                else _ = scriptExecutionManager.DeleteDatabaseSnapshot();
                break;
            default: break;
        }
    }
    catch (Exception ex)
    {
        scriptExecutionManager?.CancelExecution();
        PrintException(ex);
    }
    finally
    {
        timer.Stop();

        PrintExecutionTime();
    }
}

void Restore()
{
    try
    {
        switch (scriptExecutionManager.ExecutionType)
        {
            case ExecutionRunType.RestoreData:
                _ = scriptExecutionManager
                .RestoreDataSourceBackup(scriptExecutionManager.LastExecutedSequence)
                .RestoreDatabaseSnapshot(scriptExecutionManager.LastExecutedSequence);
                break;
            case ExecutionRunType.RestoreBackup:
                if (scriptExecutionManager.LastExecutedSequence > 0) _ = scriptExecutionManager.RestoreDataSourceBackup(scriptExecutionManager.LastExecutedSequence);
                else _ = scriptExecutionManager.RestoreDataSourceBackup(scriptExecutionManager.LastExecutedSequence);
                break;
            case ExecutionRunType.DeleteSnapshot:
                if (scriptExecutionManager.LastExecutedSequence > 0) _ = scriptExecutionManager.RestoreDatabaseSnapshot(scriptExecutionManager.LastExecutedSequence);
                else _ = scriptExecutionManager.RestoreDatabaseSnapshot(scriptExecutionManager.LastExecutedSequence);
                break;
            default: break;
        }
    }
    catch (Exception ex)
    {
        scriptExecutionManager?.CancelExecution();
        PrintException(ex);
    }
    finally
    {
        timer.Stop();

        PrintExecutionTime();
    }
}

void Backup()
{
    try
    {
        switch (scriptExecutionManager.ExecutionType)
        {
            case ExecutionRunType.CreateBackup:
                _ = scriptExecutionManager.CreateDataSourceBackup(scriptExecutionManager.LastExecutedSequence);
                break;
            case ExecutionRunType.CreateSnapshot:
                _ = scriptExecutionManager.CreateDatabaseSnapshot(scriptExecutionManager.LastExecutedSequence);
                break;
            default: break;
        }
    }
    catch (Exception ex)
    {
        scriptExecutionManager?.CancelExecution();
        PrintException(ex);
    }
    finally
    {
        timer.Stop();

        PrintExecutionTime();
    }
}



void PrintHeader()
{
    FileVersionInfo fileVerInfo = Process.GetCurrentProcess().MainModule!.FileVersionInfo!;

    using ValueStringBuilder headerText = new ValueStringBuilder(fileVerInfo.FileDescription!.Length + fileVerInfo.FileVersion!.Length + fileVerInfo.CompanyName!.Length + 7);
    headerText.Append(fileVerInfo.FileDescription.AsSpan());
    headerText.Append(" [[v".AsSpan());
    headerText.Append(fileVerInfo.FileVersion.AsSpan());
    headerText.Append("]]".AsSpan());
    headerText.Append(Environment.NewLine.AsSpan());
    headerText.Append(fileVerInfo.CompanyName.AsSpan());

    Panel panel = new Panel(headerText.ToString());
    panel.Border = BoxBorder.Ascii;

    AnsiConsole.WriteLine();
    AnsiConsole.Write(panel);
    AnsiConsole.WriteLine();
}

void PrintException(Exception exception)
{
    if (hideLogs)
    {
        scriptExecutionManager?.Logger?.LogError(exception, "{error}", exception.Message);
    }
    else
    {
        AnsiConsole.WriteException(exception);
    }
}

void PrintExecutionTime()
{
    AnsiConsole.WriteLine();

    using ValueStringBuilder executionTime = new ValueStringBuilder(80);

    executionTime.Append("[[[lightslategrey]Total Execution Time:");
    executionTime.Append($"{timer.Elapsed.Humanize(2)}");
    executionTime.Append("[/]]]");

    Rule rule = new Rule(executionTime.ToString());
    rule.Justification = Justify.Center;
    rule.RuleStyle(new Style(foreground: Color.Green));
    AnsiConsole.Write(rule);
}

void PrintExecutionResults(ScriptExecutionResults results)
{
    AnsiConsole.WriteLine();

    Table table = new Table();
    table.Title = new TableTitle("Script Manager Execution Results:");
    table.Border(TableBorder.Ascii);

    table.AddColumn(new TableColumn(string.Empty).Centered());
    table.AddColumn(new TableColumn("Total Scripts").Centered());
    table.AddColumn(new TableColumn("Already Ran Scripts").Centered());
    table.AddColumn(new TableColumn("Successful Scripts").Centered());
    table.AddColumn(new TableColumn("Failed Scripts").Centered());

    table.AddRow("Number of Scripts", $"{results.TotalScripts}", $"[lime]{results.AlreadyRanScripts}[/]", $"[greenyellow]{results.SuccessfulExecutedScripts}[/]", $"[gold1]{results.FailedScripts}[/]");

    AnsiConsole.Write(table);
    AnsiConsole.WriteLine();
}

void PrintHelpDocumentation()
{
    Dictionary<IRenderable, IRenderable> argumentDescription = new Dictionary<IRenderable, IRenderable>()
    {
        // Server
        {
            new Markup($"[lime]{ArgumentDefinitions.s}[/] or [lime]{ArgumentDefinitions.server}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify the server name or IP address.").Overflow(Overflow.Ellipsis)
        },

        //Database
        {
            new Markup($"[lime]{ArgumentDefinitions.d}[/] or [lime]{ArgumentDefinitions.database}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify the name of the database to use.").Overflow(Overflow.Ellipsis)
        },

        // Username
        {
            new Markup($"[lime]{ArgumentDefinitions.u}[/] or [lime]{ArgumentDefinitions.user}[/] or [lime]{ArgumentDefinitions.username}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify the username to use for authentication.").Overflow(Overflow.Ellipsis)
        },

        // Password
        {
            new Markup($"[lime]{ArgumentDefinitions.p}[/] or [lime]{ArgumentDefinitions.password}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify the password to use for authentication.").Overflow(Overflow.Ellipsis)
        },

        // Folder
        {
            new Markup($"[lime]{ArgumentDefinitions.f}[/] or [lime]{ArgumentDefinitions.folder}[/] or [lime]{ArgumentDefinitions.directory}[/] or [lime]{ArgumentDefinitions.path}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify the directory/folder to use for locating the script files. Sub-directories are included.").Overflow(Overflow.Ellipsis)
        },

        // Logger
        {
            new Markup($"[lime]{ArgumentDefinitions.o}[/] or [lime]{ArgumentDefinitions.output}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify the directory/folder to use for saving log files. Use '.' for current path.").Overflow(Overflow.Ellipsis)
        },

        // Server type
        {
            new Markup($"[lime]{ArgumentDefinitions.v}[/] or [lime]{ArgumentDefinitions.servertype}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify the type of server.\nPossible inputs: [cyan]sql[/], [cyan]sqlserver[/], [cyan]mssql[/], [cyan]mssqlserver[/].").Overflow(Overflow.Ellipsis)
        },

        // Environment
        {
            new Markup($"[lime]{ArgumentDefinitions.e}[/] or [lime]{ArgumentDefinitions.environment}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify database server environment.Possible inputs:\n*[cyan]local[/]. This is default.\n*[cyan]docker container=<container-name>[/].\n*[cyan]remote local=<local-path>,server=<server-path>,container=<container-name>[/].").Overflow(Overflow.Ellipsis)
        },

        // Data Source
        {
            new Markup($"[lime]{ArgumentDefinitions.c}[/] or [lime]{ArgumentDefinitions.source}[/] or [lime]{ArgumentDefinitions.storage}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify the type of data source to store the execution history. Possible inputs:\n* [cyan]internal[/] to save the history within the database. This is the default.\n* [cyan]json[/] to save the history within a .json file.\n* [cyan]csv[/] to save the history within a .csv file.").Overflow(Overflow.Ellipsis)
        },

        // Sequence Number
        {
            new Markup($"[lime]{ArgumentDefinitions.l}[/] or [lime]{ArgumentDefinitions.latest}[/] or [lime]{ArgumentDefinitions.sequence}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to specify a sequence number.").Overflow(Overflow.Ellipsis)
        },

        // Hide
        {
            new Markup($"[lime]{ArgumentDefinitions.h}[/] or [lime]{ArgumentDefinitions.hide}[/]\n[lime]{ArgumentDefinitions.ha}[/] or [lime]{ArgumentDefinitions.hideall}[/]").Overflow(Overflow.Ellipsis),

            new Markup("Use this to hide script output, except for warning, error and critical messages.\nUse this to hide all script output.").Overflow(Overflow.Ellipsis)
        },

        // Execution
         {
            new Markup($"\n[lime]{ArgumentDefinitions.r}[/] or [lime]{ArgumentDefinitions.run}[/]\n[lime]{ArgumentDefinitions.t}[/] or [lime]{ArgumentDefinitions.test}[/]\n[lime]{ArgumentDefinitions.i}[/] or [lime]{ArgumentDefinitions.inspect}[/] or [lime]{ArgumentDefinitions.scan}[/]\n[lime]{ArgumentDefinitions.qr}[/] or [lime]{ArgumentDefinitions.quickrun}[/]\n[lime]{ArgumentDefinitions.qt}[/] or [lime]{ArgumentDefinitions.quicktest}[/]\n[lime]{ArgumentDefinitions.sr}[/] or [lime]{ArgumentDefinitions.superrun}[/]\n[lime]{ArgumentDefinitions.st}[/] or [lime]{ArgumentDefinitions.supertest}[/]").Overflow(Overflow.Ellipsis),

            new Markup($"Use these arguments to specify the type of execution to use.\n* [lime]{ArgumentDefinitions.r}[/] or [lime]{ArgumentDefinitions.run}[/] specifies scripts to execute.\n* [lime]{ArgumentDefinitions.t}[/] or [lime]-{ArgumentDefinitions.test}[/] specifies changes to be rolled-back after execution.\n* [lime]{ArgumentDefinitions.i}[/] or [lime]{ArgumentDefinitions.inspect}[/] or [lime]{ArgumentDefinitions.scan}[/] specifies to search for scripts but not execute them.\n* [lime]-q[/] or [lime]-quick[/] prefixes specifies running execution process with optimizations.\n* [lime]-s[/] or [lime]-super[/] runs the execution process with maximum optimizations.").Overflow(Overflow.Ellipsis)
        },

         // Backup, Delete, Restore
        {
            new Markup($"[lime]{ArgumentDefinitions.b}[/] or [lime]{ArgumentDefinitions.backup}[/]\n[lime]{ArgumentDefinitions.n}[/] or [lime]{ArgumentDefinitions.snapshot}[/]\n[lime]{ArgumentDefinitions.x}[/] or [lime]{ArgumentDefinitions.delete}[/]\n[lime]{ArgumentDefinitions.xb}[/] or [lime]{ArgumentDefinitions.deletebackup}[/]\n[lime]{ArgumentDefinitions.xn}[/] or [lime]{ArgumentDefinitions.deletesnapshot}[/]\n[lime]{ArgumentDefinitions.z}[/] or [lime]{ArgumentDefinitions.restore}[/]\n[lime]{ArgumentDefinitions.zb}[/] or [lime]{ArgumentDefinitions.restorebackup}[/]\n[lime]{ArgumentDefinitions.zn}[/] or [lime]{ArgumentDefinitions.restoresnapshot}[/]").Overflow(Overflow.Ellipsis),

            new Markup($"* [lime]{ArgumentDefinitions.b}[/] or [lime]{ArgumentDefinitions.backup}[/] specifies creating a backup.\n* [lime]{ArgumentDefinitions.n}[/] or [lime]{ArgumentDefinitions.snapshot}[/] specifies creating a snapshot.\n* [lime]{ArgumentDefinitions.x}[/] or [lime]{ArgumentDefinitions.delete}[/] specifies deleting all data sources, backups and snapshots.\n* [lime]{ArgumentDefinitions.xb}[/] or [lime]{ArgumentDefinitions.deletebackup}[/] specifies deleting backup(s). Use [lime]{ArgumentDefinitions.l}[/] for sequence.\n* [lime]{ArgumentDefinitions.xn}[/] or [lime]{ArgumentDefinitions.deletesnapshot}[/] specifies deleting snapshot(s). Use [lime]{ArgumentDefinitions.l}[/] for sequence.\n* [lime]{ArgumentDefinitions.z}[/] or [lime]{ArgumentDefinitions.restore}[/] specifies restoring a backup using [lime]{ArgumentDefinitions.l}[/] with latest snapshot.\n* [lime]{ArgumentDefinitions.zb}[/] or [lime]{ArgumentDefinitions.restorebackup}[/] specifies restoring a backup. Use [lime]{ArgumentDefinitions.l}[/] for sequence.\n* [lime]{ArgumentDefinitions.zn}[/] or [lime]{ArgumentDefinitions.restoresnapshot}[/] specifies restoring a snapshot. Use [lime]{ArgumentDefinitions.l}[/] for sequence.").Overflow(Overflow.Ellipsis)
        }
    };

    Rule rule = new Rule("Script Manager Help Documentation:").Justify(Justify.Center);

    Grid grid = new Grid();
    grid.Expand();

    grid.AddColumn();
    grid.AddColumn();

    grid.AddRow(new Markup("[underline]Arguments[/]"), new Markup("[underline]Description[/]"));
    grid.AddRow();

    foreach ((IRenderable argument, IRenderable description) in argumentDescription)
    {
        grid.AddRow(argument, description);
        grid.AddRow();
    }

    AnsiConsole.WriteLine();
    AnsiConsole.Write(rule);
    AnsiConsole.WriteLine();
    AnsiConsole.Write(grid);
}



static void OnExecutionStarted(object? _, Script e)
{
    LogScript(string.Intern("Execution Started:"), true, e);
}

static void OnExecutionEnded(object? _, Script e)
{
    LogScript(string.Intern("Execution Ended:"), false, e);
}

static void OnExecutionProgress(ExecutionProgress progress)
{
    ProgressHelper.CurrentProgress = progress;
}

static string? GetExecutionName(ExecutionRunType executionType, ExecutionOptimizationType optimizationType) => (executionType, optimizationType) switch
{
    (ExecutionRunType.DefaultRun, ExecutionOptimizationType.None) => string.Intern("Run: "),
    (ExecutionRunType.DefaultRun, ExecutionOptimizationType.Quick) => string.Intern("QuickRun: "),
    (ExecutionRunType.DefaultRun, ExecutionOptimizationType.Super) => string.Intern("SuperRun: "),
    (ExecutionRunType.TestRun, ExecutionOptimizationType.None) => string.Intern("TestRun: "),
    (ExecutionRunType.TestRun, ExecutionOptimizationType.Quick) => string.Intern("QuickRun: "),
    (ExecutionRunType.TestRun, ExecutionOptimizationType.Super) => string.Intern("SuperRun: "),
    (ExecutionRunType.ScanOnly, ExecutionOptimizationType.None) => string.Intern("Scan: "),
    _ => null
};

static void LogScript(string ruleTitle, bool newline, Script script)
{
    using ValueStringBuilder text = new ValueStringBuilder(120);

    text.Append($"{ruleTitle} - [[[lightslategrey]{script.File?.GetFileName()!}".AsSpan());

    if (script.Status is ScriptStatusType.SUCCESS)
    {
        if (script.IsAlreadyRan) text.Append(string.Intern(" Status: [lime]ALREADY RAN[/]").AsSpan());
        else text.Append(string.Intern(" Status: [greenyellow]SUCCESS[/]").AsSpan());
    }
    else if (script.Status is ScriptStatusType.NONE)
    {
        text.Append(string.Intern(" Status: [gold1]NEVER RAN[/]").AsSpan());
    }
    else if (script.Status is ScriptStatusType.FAIL)
    {
        text.Append(string.Intern(" Status: [white on maroon]FAIL[/]").AsSpan());
    }

    if (script.ExecutionTime is not null && script.ExecutionType is not ExecutionRunType.ScanOnly)
    {
        text.Append($" Execution Time: {script.ExecutionTime.Value.Humanize()}".AsSpan());
    }

    text.Append("[/]]]");

    Rule rule = new Rule(text.ToString());

    rule.Justification = Justify.Center;
    rule.RuleStyle(new Style(foreground: Color.Green));

    if (newline)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine();
    }

    AnsiConsole.Write(rule);
    AnsiConsole.Write('\r');
}



ref struct ValueStringBuilder
{
    private char[]? _arrayToReturnToPool;
    private Span<char> _chars;
    private int _pos;

    public ValueStringBuilder(Span<char> initialBuffer)
    {
        _arrayToReturnToPool = null;
        _chars = initialBuffer;
        _pos = 0;
    }

    public ValueStringBuilder(int initialCapacity)
    {
        _arrayToReturnToPool = ArrayPool<char>.Shared.Rent(initialCapacity);
        _chars = _arrayToReturnToPool;
        _pos = 0;
    }

    public void Append(ReadOnlySpan<char> value)
    {
        int pos = _pos;
        if (pos > _chars.Length - value.Length)
        {
            Grow(value.Length);
        }

        value.CopyTo(_chars.Slice(_pos));
        _pos += value.Length;
    }

    private void Grow(int additionalCapacityBeyondPos)
    {
        // Make sure to let Rent throw an exception if the caller has a bug and the desired capacity is negative
        char[] poolArray = ArrayPool<char>.Shared.Rent((int)Math.Max((uint)(_pos + additionalCapacityBeyondPos), (uint)_chars.Length * 2));

        _chars.Slice(0, _pos).CopyTo(poolArray);

        char[]? toReturn = _arrayToReturnToPool;
        _chars = _arrayToReturnToPool = poolArray;
        if (toReturn != null)
        {
            ArrayPool<char>.Shared.Return(toReturn);
        }
    }

    public override string ToString()
    {
        string s = _chars.Slice(0, _pos).ToString();
        Dispose();
        return s;
    }

    public void Dispose()
    {
        char[]? toReturn = _arrayToReturnToPool;
        this = default; // for safety, to avoid using pooled array if this instance is erroneously appended to again
        if (toReturn != null)
        {
            ArrayPool<char>.Shared.Return(toReturn);
        }
    }
}

ref struct ScriptManagerConfig
{
    public DataProviderType? DatabaseType { get; private set; } = default!;

    public ExecutionRunType? ExecutionType { get; private set; } = default!;

    public DataSourceType? SourceType { get; private set; } = default!;

    public ServerEnvironmentType? EnvironmentType { get; private set; } = default!;

    public ExecutionOptimizationType? OptimizationType { get; private set; } = default!;

    public ExecutionThreadType? ThreadType { get; private set; } = default!;

    public int? SequenceNumber { get; private set; } = default!;

    public string Database { get; private set; } = default!;

    public string Server { get; private set; } = default!;

    public string ScriptExtension { get; private set; } = default!;

    public string Username { get; private set; } = default!;

    public string Password { get; private set; } = default!;

    public PathEnvironmentInfo? DataSourceInfo { get; private set; } = default!;

    public PathEnvironmentInfo? ScriptsLocationInfo { get; private set; } = default!;

    public string? LogsLocation { get; private set; } = default;


    public ScriptManagerConfig(string[] args, ILogger logger)
    {
        try
        {
            ArgumentException.ThrowIfNullOrEmpty(nameof(args));

            Span<string> arguments = args.AsSpan();

            ExtractArguments(arguments);
        }
        catch (Exception)
        {
            logger?.LogError("Failed to create configuration from arguments.");
            throw;
        }
    }


    private void ExtractArguments(Span<string> args)
    {
        Dictionary<string, List<string>> argGroups = GetArgumentGroups(args);

        sbyte executionArgsCount = 0;
        foreach ((string arg, List<string> inputs) in argGroups)
        {
            string? input = inputs.FirstOrDefault();
            if (ArgumentDefinitions.ExecutionArguments.Contains(arg))
            {
                if (executionArgsCount++ > 1) throw new Exception("Input config cannot include multiple execution modes.");
                (ExecutionRunType ExecutionType, ExecutionOptimizationType OptimizationType)? executionArgs = GetExecutionType(arg);
                if (executionArgs is not null)
                {
                    ExecutionType = executionArgs?.ExecutionType;
                    OptimizationType = executionArgs?.OptimizationType;
                }
            }
            else if (ArgumentDefinitions.SequenceNumberArguments.Contains(arg))
            {
                SequenceNumber = GetSequence(input!);
            }
            else if (ArgumentDefinitions.SourceTypeArguments.Contains(arg))
            {
                SourceType = GetDataSourceType(input!);
            }
            else if (ArgumentDefinitions.ServerArguments.Contains(arg))
            {
                Server = input!;
            }
            else if (ArgumentDefinitions.ServerTypeArguments.Contains(arg))
            {
                DatabaseType = GetDatabaseType(input!);
                ScriptExtension = GetScriptExtension(DatabaseType!.Value);
            }
            else if (ArgumentDefinitions.DatabaseArguments.Contains(arg))
            {
                Database = input!;
            }
            else if (ArgumentDefinitions.UserArguments.Contains(arg))
            {
                Username = input!;
            }
            else if (ArgumentDefinitions.PasswordArguments.Contains(arg))
            {
                Password = input!;
            }
            else if (ArgumentDefinitions.EnvironmentArguments.Contains(arg))
            {
                PathEnvironmentInfo? pathEnvironmentInfo = GetPathEnvironmentInfo(input!);
                EnvironmentType = pathEnvironmentInfo?.Environment;
                DataSourceInfo = pathEnvironmentInfo;
            }
            else if (ArgumentDefinitions.ScriptPathArguments.Contains(arg))
            {
                GetDirectory(ref input!);
                ScriptsLocationInfo = GetPathEnvironmentInfo(input!);
            }
            else if (ArgumentDefinitions.LogPathArguments.Contains(arg))
            {
                GetDirectory(ref input!);
                LogsLocation = input!;
            }
        }

        EnvironmentType ??= ServerEnvironmentType.Local;
        OptimizationType ??= ExecutionOptimizationType.None;
        ThreadType ??= ExecutionThreadType.SingleThreaded;
        DatabaseType ??= DataProviderType.MSSQLServer;
        SourceType ??= DataSourceType.Internal;
        if (ExecutionType is null) throw new Exception("Execution type input not specified.");
        if (string.IsNullOrEmpty(Database)) throw new Exception("Database name input not specified.");
        if (string.IsNullOrEmpty(Server)) throw new Exception("Server name input not specified.");
        if (string.IsNullOrEmpty(Username)) throw new Exception("Username input not specified.");
        if (string.IsNullOrEmpty(Password) && Username[0] is '.') throw new Exception("Password input not specified.");
        if (ScriptsLocationInfo is null) throw new Exception("Script directory does not exist.");
    }

    private static Dictionary<string, List<string>> GetArgumentGroups(Span<string> args)
    {
        Dictionary<string, List<string>> argGroups = new Dictionary<string, List<string>>();

        List<string> inputs = default!;

        foreach (string arg in args)
        {
            if (arg.StartsWith('-'))
            {
                inputs = new List<string>();

                argGroups.Add(arg, inputs);
            }
            else
            {
                inputs?.Add(arg);
            }
        }

        return argGroups;
    }

    private static (ExecutionRunType ExecutionType, ExecutionOptimizationType OptimizationType)? GetExecutionType(string arg)
    {
        return arg.ToLowerInvariant() switch
        {
            ArgumentDefinitions.r or ArgumentDefinitions.run => (ExecutionRunType.DefaultRun, ExecutionOptimizationType.None),
            ArgumentDefinitions.t or ArgumentDefinitions.test => (ExecutionRunType.TestRun, ExecutionOptimizationType.None),
            ArgumentDefinitions.i or ArgumentDefinitions.inspect or ArgumentDefinitions.scan => (ExecutionRunType.ScanOnly, ExecutionOptimizationType.None),
            ArgumentDefinitions.qr or ArgumentDefinitions.quickrun => (ExecutionRunType.DefaultRun, ExecutionOptimizationType.Quick),
            ArgumentDefinitions.qt or ArgumentDefinitions.quicktest => (ExecutionRunType.TestRun, ExecutionOptimizationType.Quick),
            ArgumentDefinitions.sr or ArgumentDefinitions.superrun => (ExecutionRunType.DefaultRun, ExecutionOptimizationType.Super),
            ArgumentDefinitions.st or ArgumentDefinitions.supertest => (ExecutionRunType.TestRun, ExecutionOptimizationType.Super),
            ArgumentDefinitions.x or ArgumentDefinitions.delete => (ExecutionRunType.DeleteData, ExecutionOptimizationType.None),
            ArgumentDefinitions.xb or ArgumentDefinitions.deletebackup => (ExecutionRunType.DeleteBackup, ExecutionOptimizationType.None),
            ArgumentDefinitions.xn or ArgumentDefinitions.deletesnapshot => (ExecutionRunType.DeleteSnapshot, ExecutionOptimizationType.None),
            ArgumentDefinitions.z or ArgumentDefinitions.restore => (ExecutionRunType.RestoreData, ExecutionOptimizationType.None),
            ArgumentDefinitions.zb or ArgumentDefinitions.restorebackup => (ExecutionRunType.RestoreBackup, ExecutionOptimizationType.None),
            ArgumentDefinitions.zn or ArgumentDefinitions.restoresnapshot => (ExecutionRunType.RestoreSnapshot, ExecutionOptimizationType.None),
            ArgumentDefinitions.b or ArgumentDefinitions.backup => (ExecutionRunType.CreateBackup, ExecutionOptimizationType.None),
            ArgumentDefinitions.n or ArgumentDefinitions.snapshot => (ExecutionRunType.CreateSnapshot, ExecutionOptimizationType.None),
            _ => null!
        };
    }

    private static int? GetSequence(string input)
    {
        int? sequence = null;

        if (string.IsNullOrEmpty(input) is false)
        {
            if (input.Length is 1 && input[0] == '0') sequence = 0;
            else sequence = int.Parse(input?.TrimStart('0', '#', '-', '_')!);
        }

        return sequence;
    }

    private static DataSourceType? GetDataSourceType(string input)
    {
        return input.ToLowerInvariant() switch
        {
            "internal" => DataSourceType.Internal,
            "json" => DataSourceType.Json,
            "csv" => DataSourceType.Csv,
            _ => DataSourceType.Internal
        };
    }

    private static PathEnvironmentInfo? GetPathEnvironmentInfo(string input)
    {
        if (input.StartsWith("docker"))
        {
            string[] parameters = input.Substring(6).TrimStart().Split(',');

            string? container = Array.Find(parameters, static x => x!.Contains("container", StringComparison.OrdinalIgnoreCase))?.Split('=').LastOrDefault();

            return new PathEnvironmentInfo
            {
                Environment = ServerEnvironmentType.Docker,
                ContainerName = container,
                LocalPath = null,
                ServerPath = input,
            };
        }
        else if (input.StartsWith("remote"))
        {
            string[] parameters = input.Substring(6).TrimStart().Split(',');

            string? localPath = Array.Find(parameters, static x => x!.Contains("local"))?.Split('=').LastOrDefault();
            string? serverPath = Array.Find(parameters, static x => x!.Contains("server"))?.Split('=').LastOrDefault();
            string? container = Array.Find(parameters, static x => x!.Contains("container"))?.Split('=').LastOrDefault();

            return new PathEnvironmentInfo
            {
                Environment = ServerEnvironmentType.Remote,
                ContainerName = container,
                LocalPath = localPath,
                ServerPath = serverPath,
            };
        }
        else if (input.StartsWith("local"))
        {
            string[] parameters = input.Substring(5).TrimStart().Split(',');

            string? localPath = Array.Find(parameters, static x => x!.Contains("local"))?.Split('=').LastOrDefault();

            return new PathEnvironmentInfo
            {
                Environment = ServerEnvironmentType.Local,
                ContainerName = null!,
                LocalPath = localPath,
                ServerPath = null!,
            };
        }
        else
        {
            return new PathEnvironmentInfo
            {
                Environment = ServerEnvironmentType.Local,
                ContainerName = null!,
                LocalPath = input,
                ServerPath = null!,
            };
        }
    }

    private static string GetScriptExtension(DataProviderType databaseServerType)
    {
        return databaseServerType switch
        {
            DataProviderType.MSSQLServer or DataProviderType.SQLLite => ".sql",
            // add more database types when needed
            _ => ".sql"
        };
    }

    private static DataProviderType? GetDatabaseType(string input)
    {
        return input.ToLowerInvariant() switch
        {
            "sql" or "sqlserver" or "mssql" or "mssqlserver" => DataProviderType.MSSQLServer,
            "sqlite" or "lite" => DataProviderType.SQLLite,
            _ => DataProviderType.MSSQLServer
        };
    }

    private static void GetDirectory(ref string input)
    {
        if (input == ".")
        {
            input = Environment.CurrentDirectory;
        }
    }
}

static class ArgumentDefinitions
{
    public const string s = "-s";
    public const string server = "-server";
    public static readonly StringValues ServerArguments = new string[2] { s, server };

    public const string v = "-v";
    public const string servertype = "-servertype";
    public static readonly StringValues ServerTypeArguments = new string[2] { v, servertype };

    public const string d = "-d";
    public const string database = "-database";
    public static readonly StringValues DatabaseArguments = new string[2] { d, database };

    public const string u = "-u";
    public const string user = "-user";
    public const string username = "-username";
    public static readonly StringValues UserArguments = new string[3] { u, user, username };

    public const string p = "-p";
    public const string password = "-password";
    public static readonly StringValues PasswordArguments = new string[2] { p, password };

    public const string e = "-e";
    public const string environment = "-environment";
    public static readonly StringValues EnvironmentArguments = new string[2] { e, environment };

    public const string c = "-c";
    public const string source = "-source";
    public const string storage = "-storage";
    public static readonly StringValues SourceTypeArguments = new string[3] { c, source, storage };

    public const string f = "-f";
    public const string folder = "-folder";
    public const string directory = "-directory";
    public const string path = "-path";
    public static readonly StringValues ScriptPathArguments = new string[4] { f, folder, directory, path };

    public const string o = "-o";
    public const string output = "-output";
    public static readonly StringValues LogPathArguments = new string[2] { o, output };

    public const string l = "-l";
    public const string latest = "-latest";
    public const string sequence = "-sequence";
    public static readonly StringValues SequenceNumberArguments = new string[3] { l, latest, sequence };

    public const string h = "-h";
    public const string hide = "-hide";
    public const string ha = "-ha";
    public const string hideall = "-hideall";
    public static readonly StringValues HideOutputArguments = new string[4] { h, hide, ha, hideall };

    public const string r = "-r";
    public const string run = "-run";
    public const string qr = "-qr";
    public const string quickrun = "-quickrun";
    public const string sr = "-sr";
    public const string superrun = "-superrun";
    public const string t = "-t";
    public const string test = "-test";
    public const string qt = "-qt";
    public const string quicktest = "-quicktest";
    public const string st = "-st";
    public const string supertest = "-supertest";
    public const string i = "-i";
    public const string inspect = "-inspect";
    public const string scan = "-scan";
    public const string x = "-x";
    public const string delete = "-delete";
    public const string xb = "-xb";
    public const string deletebackup = "-deletebackup";
    public const string xn = "-xn";
    public const string deletesnapshot = "-deletesnapshot";
    public const string z = "-z";
    public const string restore = "-restore";
    public const string zb = "-zb";
    public const string restorebackup = "-restorebackup";
    public const string zn = "-zn";
    public const string restoresnapshot = "-restoresnapshot";
    public const string b = "-b";
    public const string backup = "-backup";
    public const string n = "-n";
    public const string snapshot = "-snapshot";
    public static readonly StringValues ExecutionArguments = new string[31] { r, run, qr, quickrun, sr, superrun, t, test, qt, quicktest, st, supertest, i, inspect, scan, x, delete, xb, deletebackup, xn, deletesnapshot, z, restore, zb, restorebackup, zn, restoresnapshot, b, backup, n, snapshot };
}


static class StatusHelper
{
    public const int MAXIMUM_DIGITS_BUFFER_THRESHOLD = 11;

    private static char[] _statusNumberBuffer = new char[MAXIMUM_DIGITS_BUFFER_THRESHOLD];

    private static char[] _statusContentBuffer = null!;

    public static bool IsBusy = false;

    public static Status? Status = default;

    public static string StatusContent = null!;

    public static void StartStatus(string status)
    {
        StatusContent = status;

        _statusContentBuffer = new char[StatusContent.Length + _statusNumberBuffer.Length];

        ReadOnlySpan<char> statusInitialBuffer = StatusContent.AsSpan();

        for (int i = 0; i < statusInitialBuffer.Length; i++)
        {
            _statusContentBuffer[i] = statusInitialBuffer[i];
        }

        _ = Task.Run(() =>
        {
            Status ??= GetStatus();

            IsBusy = true;

            Status.Start(StatusContent, OnStatus);
        });
    }

    public static void StopStatus()
    {
        IsBusy = false;
    }

    private static void OnStatus(StatusContext context)
    {
        while (IsBusy)
        {
            using ValueStringBuilder statusBuilder = new ValueStringBuilder(_statusContentBuffer);

            if (ProgressHelper.CurrentProgress is not null)
            {
                _ = ProgressHelper.CurrentProgress.Current.TryFormat(_statusNumberBuffer, out int charsWritten);

                ReadOnlySpan<char> number = _statusNumberBuffer.AsSpan(0, charsWritten);

                statusBuilder.Append(number);
            }

            _ = context.Status(statusBuilder.ToString());
            context.Refresh();
        }
    }

    private static Status GetStatus()
    {
        return AnsiConsole
               .Status()
               .Spinner(Spinner.Known.BetaWave)
               .SpinnerStyle(Style.Parse("greenyellow"));
    }
}

static class ProgressHelper
{
    public const int PROGRESS_DISPLAY_THRESHOLD = 50;

    public const long TIMEOUT_SECONDS = 30;

    public static Progress? Progress = default;

    public static ExecutionProgress CurrentProgress = default!;

    public static string TaskName = null!;

    public static void StartProgress(string taskName)
    {
        TaskName = taskName;

        _ = Task.Run(() =>
        {
            CurrentProgress = default!;

            Progress ??= GetProgress();

            while (CurrentProgress is null) continue;

            if (CurrentProgress?.Total < PROGRESS_DISPLAY_THRESHOLD) return;

            Progress.Start(OnProgress);
        });
    }

    private static void OnProgress(ProgressContext context)
    {
        ProgressTask task = context.AddTask(TaskName);

        while (CurrentProgress?.Total == 0) continue;

        task.MaxValue = Convert.ToDouble(CurrentProgress!.Total);

        while (context.IsFinished is false && CurrentProgress is not null)
        {
            task.Value = Convert.ToDouble(CurrentProgress!.Current);
            context.Refresh();
        }

        _ = task.Value(task.MaxValue);
    }

    private static Progress GetProgress()
    {
        return AnsiConsole.Progress()
        .AutoRefresh(false)
        .AutoClear(false)
        .HideCompleted(false)
        .Columns(new ProgressColumn[]
        {
            new TaskDescriptionColumn(),
            new ProgressBarColumn()
            {
                CompletedStyle = new Style(Color.Lime,null, Decoration.Bold),
                IndeterminateStyle = new Style(Color.Yellow,null, Decoration.Bold),
                RemainingStyle = new Style(Color.LightSlateGrey,null, Decoration.Bold),
                FinishedStyle = new Style(Color.Lime, null, Decoration.Bold)
            },
            new PercentageColumn(),
        });
    }
}


public sealed class ConsoleFormatterOptions : Microsoft.Extensions.Logging.Console.ConsoleFormatterOptions
{
    public static char ReturnChar = '\r';

    public static char NewlineChar = '\n';

    public static char EmptyChar = ' ';

    public static char OpenSquareBracket = '[';

    public static char CloseSquareBracket = ']';

    public static string? LogLevelPadding = "::";

    public static string LogMessagePadding = new string(' ', 4 + LogLevelPadding.Length);

    public static Dictionary<LogLevel, string> LogLevelStrings = new Dictionary<LogLevel, string>()
    {
        {LogLevel.Trace ,"[bold green on black]trce[/]"},
        {LogLevel.Debug ,"[bold green on black]dbug" },
        {LogLevel.Information ,"[bold lime on black]info[/]"},
        {LogLevel.Warning ,"[bold gold1 on black]warn[/]"},
        {LogLevel.Error ,"[bold white on maroon]fail[/]" },
        {LogLevel.Critical ,"[bold yellow on maroon]crit[/]"},
    };
}

internal sealed class ConsoleFormatter : Microsoft.Extensions.Logging.Console.ConsoleFormatter
{
    public ConsoleFormatter() : base(nameof(ConsoleFormatter))
    {
    }

    public override void Write<TState>(in LogEntry<TState> logEntry, IExternalScopeProvider? scopeProvider, TextWriter textWriter)
    {
        if (logEntry.State is null) return;

        string logLevelString = StringPool.Shared.GetOrAdd(ConsoleFormatterOptions.LogLevelStrings[logEntry.LogLevel]);

        AnsiConsole.Markup(logLevelString);

        LogMessage(textWriter, logEntry, scopeProvider);
    }

    private static void LogMessage<TState>(TextWriter textWriter, in LogEntry<TState> logEntry, IExternalScopeProvider? scopeProvider)
    {
        ReadOnlySpan<char> message = StringPool.Shared.GetOrAdd(logEntry.Formatter(logEntry.State, logEntry.Exception).AsSpan());

        ReadOnlySpan<char> multilineCharacters = stackalloc char[2] { ConsoleFormatterOptions.ReturnChar, ConsoleFormatterOptions.NewlineChar };
        bool multiline = message.Contains(multilineCharacters, StringComparison.Ordinal);

        textWriter.Write(ConsoleFormatterOptions.LogLevelPadding.AsSpan());
        textWriter.Write(ConsoleFormatterOptions.OpenSquareBracket);
        textWriter.Write(StringPool.Shared.GetOrAdd(logEntry.Category).AsSpan());
        textWriter.Write(ConsoleFormatterOptions.CloseSquareBracket);

        textWriter.Write(string.Intern(" => ").AsSpan());

        if (scopeProvider is not null) WriteScopeInformation(textWriter, scopeProvider);

        bool isMessageEmpty = message.IsEmpty is false || !message.IsWhiteSpace() is false;
        if (isMessageEmpty)
        {
            WriteMessage(textWriter, message, multiline);
        }

        if (logEntry.Exception is not null)
        {
            ReadOnlySpan<char> exception = logEntry.Exception.ToString().AsSpan();
            multiline = exception.Contains(multilineCharacters, StringComparison.Ordinal);
            if (isMessageEmpty) textWriter.Write(string.IsInterned(nameof(logEntry.Exception)));
            textWriter.Write(ConsoleFormatterOptions.NewlineChar);
            WriteMessage(textWriter, logEntry.Exception.ToString().AsSpan(), multiline);
        }
        textWriter.Write(ConsoleFormatterOptions.NewlineChar);
    }

    private static void WriteScopeInformation(TextWriter textWriter, IExternalScopeProvider? scopeProvider)
    {
        if (scopeProvider is not null)
        {
            scopeProvider.ForEachScope(static (scope, state) =>
            {
                state.Write(ConsoleFormatterOptions.LogMessagePadding.AsSpan());
                state.Write(string.Intern("=> ").AsSpan());
                state.Write(scope);
            }, textWriter);
        }
    }

    private static void WriteMessage(TextWriter textWriter, ReadOnlySpan<char> message, bool multiline)
    {
        if (message.IsEmpty is false)
        {
            if (multiline is false)
            {
                textWriter.Write(message);
            }
            else
            {
                textWriter.Write(ConsoleFormatterOptions.LogMessagePadding.AsSpan());
                for (int i = 0; i < message.Length; i++)
                {
                    char character = message[i];

                    textWriter.Write(character);

                    if (character == ConsoleFormatterOptions.ReturnChar || character == ConsoleFormatterOptions.NewlineChar)
                    {
                        textWriter.Write(ConsoleFormatterOptions.LogMessagePadding.AsSpan());
                    }
                }
            }
        }
    }
}
