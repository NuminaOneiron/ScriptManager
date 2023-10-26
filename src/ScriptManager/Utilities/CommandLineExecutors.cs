using System.Diagnostics;
using System.Text;

namespace ScriptManager.Utilities;

internal static class CommandLineExecutors
{
    [ThreadStatic]
    public static CommandLineResult CommandLineResult = new CommandLineResult();

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static CommandLineResult RunProcess(string exe, string args, CancellationToken cancelToken)
    {
        Process? process = null;

        try
        {
            process = new Process();
            process.StartInfo.FileName = exe;
            process.StartInfo.Arguments = args;
            process.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            process.StartInfo.CreateNoWindow = true;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.UseShellExecute = false;

            _ = process.Start();
            _ = process.WaitForExitAsync(cancelToken);

            CommandLineResult.InsertStandardOutput(process!.StandardOutput);
            CommandLineResult.InsertErrorOutput(process!.StandardError);
            CommandLineResult.ExecutionTime = process!.TotalProcessorTime;
        }
        catch (Exception ex)
        {
            CommandLineResult.ErrorOutput = ex.Message;
        }
        finally
        {
            process?.Dispose();
        }

        return CommandLineResult;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static CommandLineResult RunProcess(string exe, StringBuilder args, CancellationToken cancelToken)
    {
        Process? process = null;

        try
        {
            process = new Process();
            process.StartInfo.FileName = exe;
            process.StartInfo.Arguments = args.ToString();
            process.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            process.StartInfo.CreateNoWindow = true;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.UseShellExecute = false;

            _ = process.Start();
            _ = process.WaitForExitAsync(cancelToken);

            CommandLineResult.InsertStandardOutput(process!.StandardOutput);
            CommandLineResult.InsertErrorOutput(process!.StandardError);
            CommandLineResult.ExecutionTime = process!.TotalProcessorTime;
        }
        catch (Exception ex)
        {
            CommandLineResult.ErrorOutput = ex.Message;
        }
        finally
        {
            process?.Dispose();
        }

        return CommandLineResult;
    }
}