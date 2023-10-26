using System.Reflection;

using ScriptManager.Environments;

namespace ScriptManager.Utilities;

internal static class CompiledDelegates
{
    private static readonly FieldInfo CachedItemsArrayField = typeof(List<string>).GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static readonly MethodInfo CachedGetListItemsArrayMethod = typeof(CompiledDelegates).GetMethod(nameof(GetListItemsArrayMethod), BindingFlags.NonPublic | BindingFlags.Static)!;

    public static readonly Func<List<string>, string[]?> GetListItemsArray = (Func<List<string>, string[]?>)Delegate.CreateDelegate(typeof(Func<List<string>, string[]?>), CachedGetListItemsArrayMethod);

    private static string[] GetListItemsArrayMethod(List<string> list)
    {
        string[] array = (CachedItemsArrayField.GetValue(list) as string[])!;
        return array;
    }



    private static readonly MethodInfo CachedEditLocalJsonFileMethod = typeof(CompiledDelegates).GetMethod(nameof(EditLocalJsonFileMethod), BindingFlags.NonPublic | BindingFlags.Static)!;

    public static readonly Action<string, IPathInfo> EditLocalJsonFile = (Action<string, IPathInfo>)Delegate.CreateDelegate(typeof(Action<string, IPathInfo>), CachedEditLocalJsonFileMethod);

    private static void EditLocalJsonFileMethod(string text, IPathInfo jsonFile)
    {
        using FileStream stream = File.OpenWrite(jsonFile.FullPath);
        using StreamWriter writer = new StreamWriter(stream);
        _ = stream.Seek(-2, SeekOrigin.End);
        if (stream.Length > 4) writer.Write(",\n");
        writer.Write(text.AsSpan());
        writer.Write("\n]");
        writer.Flush();
        writer.Close();
        stream.Close();
    }



    private static readonly MethodInfo CachedEditDockerJsonFileMethod = typeof(CompiledDelegates).GetMethod(nameof(EditDockerJsonFileMethod), BindingFlags.NonPublic | BindingFlags.Static)!;

    public static readonly Action<string, IPathInfo> EditDockerJsonFile = (Action<string, IPathInfo>)Delegate.CreateDelegate(typeof(Action<string, IPathInfo>), CachedEditDockerJsonFileMethod);

    private static void EditDockerJsonFileMethod(string text, IPathInfo jsonFile)
    {
        string container = ((DockerPathInfo)jsonFile).Container;

        text = DockerPathInfo.FormatString(text.AsSpan());
        string command = $"exec {container} sh -c \"wc -l < {jsonFile.FullPath}\"";
        _ = int.TryParse(CommandLineExecutors.RunProcess("docker.exe", command, CancellationToken.None).StandardOutput, out int lineCount);
        if (lineCount > 3)
        {
            command = $"exec {container} sh -c \"sed -i '{lineCount - 2}s/$/,/' {jsonFile.FullPath}\"";
            _ = CommandLineExecutors.RunProcess("docker.exe", command, CancellationToken.None);
        }
        command = $"exec {container} sh -c \"sed -i '{$"{lineCount - 1}"}i\\{text}' {jsonFile.FullPath}\"";
        _ = CommandLineExecutors.RunProcess("docker.exe", command, CancellationToken.None);
    }


    //private static Func<IDbConnection, string, List<string>, ILogger, ScriptStatusType> CreateExecuteScriptTextCompiled()
    //{
    //    var connectionParam = Expression.Parameter(typeof(IDbConnection), "connection");
    //    var scriptTextParam = Expression.Parameter(typeof(string), "scriptText");
    //    var errorsParam = Expression.Parameter(typeof(List<string>), "errors");
    //    var loggerParam = Expression.Parameter(typeof(ILogger), "logger");


    //    var beginTransactionMethod = typeof(IDbConnection).GetMethod("BeginTransaction", BindingFlags.Public | BindingFlags.Instance, Type.EmptyTypes);
    //    var executeMethod = typeof(SqlMapper).GetMethod("Execute", BindingFlags.Public | BindingFlags.Static, new[] { typeof(IDbConnection), typeof(string), typeof(object[]), typeof(IDbTransaction), typeof(int?), typeof(CommandType?) });
    //    var addMethod = typeof(List<string>).GetMethod("Add", BindingFlags.Public | BindingFlags.Instance);
    //    var logExceptionMethod = typeof(LoggerExtensions).GetMethod("LogException", BindingFlags.Public | BindingFlags.Static, new[] { typeof(ILogger), typeof(LogLevel), typeof(string), typeof(Exception) });
    //    var rollbackMethod = typeof(IDbTransaction).GetMethod("Rollback", BindingFlags.Public | BindingFlags.Instance);
    //    var commitMethod = typeof(IDbTransaction).GetMethod("Commit", BindingFlags.Public | BindingFlags.Instance);
    //    var disposeMethod = typeof(IDisposable).GetMethod("Dispose", BindingFlags.Public | BindingFlags.Instance);

    //    var exceptionVariable = Expression.Parameter(typeof(Exception), "ex");

    //    var statusVariable = Expression.Variable(typeof(ScriptStatusType), "status");
    //    var statusSuccessAssign = Expression.Assign(statusVariable, Expression.Constant(ScriptStatusType.SUCCESS));
    //    var statusFailAssign = Expression.Assign(statusVariable, Expression.Constant(ScriptStatusType.FAIL));
    //    LabelTarget returnTarget = Expression.Label(typeof(ScriptStatusType));
    //    GotoExpression returnExpression = Expression.Return(returnTarget,
    //        statusVariable, typeof(ScriptStatusType));
    //    LabelExpression statusReturn = Expression.Label(returnTarget, statusFailAssign);

    //    var transactionVariable = Expression.Variable(typeof(IDbTransaction), "transaction");
    //    var transactionAssign = Expression.Assign(transactionVariable, Expression.Convert(Expression.Call(connectionParam, beginTransactionMethod!), typeof(IDbTransaction)));

    //    var scriptExecuteCall = Expression.Call(null, executeMethod!, connectionParam, scriptTextParam, Expression.Constant(default(object[])), transactionVariable, Expression.Constant(0, typeof(int?)), Expression.Constant(CommandType.Text, typeof(CommandType?)));
    //    var transactionDisposeCall = Expression.Call(transactionVariable, disposeMethod!);
    //    var transactionCommitCall = Expression.Call(transactionVariable, commitMethod!);

    //    var tryExecuteScriptBlock =
    //        Expression.TryCatch(Expression.Block(scriptExecuteCall, statusSuccessAssign, transactionCommitCall),
    //        Expression.Catch(exceptionVariable,
    //        Expression.Block(
    //            Expression.Call(errorsParam, addMethod!, Expression.Property(exceptionVariable, "Message")),
    //            Expression.Call(null, logExceptionMethod!, loggerParam, Expression.Constant(LogLevel.Warning), Expression.Constant("Failed to execute command."), exceptionVariable),
    //            Expression.Call(transactionVariable, rollbackMethod!)
    //        )
    //    ));

    //    var methodBody = Expression.Block(
    //        new[] { statusVariable, transactionVariable, exceptionVariable },
    //        statusFailAssign,
    //        transactionAssign,
    //        tryExecuteScriptBlock,
    //        transactionDisposeCall,
    //        statusReturn
    //    );

    //    var lambda = Expression.Lambda<Func<IDbConnection, string, List<string>, ILogger, ScriptStatusType>>(
    //        methodBody,
    //        connectionParam,
    //        scriptTextParam,
    //        errorsParam,
    //        loggerParam
    //    );

    //    return lambda.Compile();
    //}
}
