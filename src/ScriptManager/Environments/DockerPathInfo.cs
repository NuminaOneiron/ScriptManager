using System.Text;

using CommunityToolkit.HighPerformance;

using ScriptManager.Enums;
using ScriptManager.Extensions;
using ScriptManager.Utilities;

namespace ScriptManager.Environments;

public readonly struct DockerPathInfo : IPathInfo
{
    private const string EXE = "docker.exe";

    public ServerEnvironmentType Environment { get; } = ServerEnvironmentType.Docker;

    public readonly string Container { get; init; }

    public ref readonly string Directory { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref GetDirectoryName()!; }

    public readonly string FullPath { get; init; }

    public ref readonly bool Exists { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref FileExists(); }

    public ref readonly int Length { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref GetLength(); }

    public ref readonly string Parent { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref GetDirectoryName()!; }


    public DockerPathInfo(string path, string container)
    {
        FullPath = path;
        Container = container;
        CheckFileAccess();
    }

    public DockerPathInfo(string container, Span<string> pathTokens)
    {
        Container = container;

        StringBuilder path = StringBuilderCache.Acquire();
        for (int i = 0; i < pathTokens.Length; i++)
        {
            if (i > 0) _ = path.Append(Constants.ForwardSlash);
            _ = path.Append(pathTokens[i]);
        }

        FullPath = path.ToString();
        CheckFileAccess();
    }
    

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ref int GetLength()
    {
        string command = $"exec {Container} sh -c \"wc -m {FullPath}\"";
        string? result = RunCommand(command).StandardOutput;
        _ = int.TryParse(result?.Split(' ')?.FirstOrDefault(), out int length);
        return ref Unsafe.AsRef(length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ref string GetDirectoryName()
    {
        string command = $"exec {Container} sh -c \"dirname {FullPath}\"";
        string? result = RunCommand(command).StandardOutput;
        return ref Unsafe.AsRef(result)!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string[] GetLines(string content)
    {
        if (string.IsNullOrEmpty(content)) return null!;

        string? line;
        List<string> lines = new List<string>();

        StringReader reader = new StringReader(content);
        while ((line = reader.ReadLine()) is not null)
        {
            if (string.IsNullOrEmpty(line) is false) lines.Add(line);
        }

        return lines.ToArray();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ref bool FileExists()
    {
        string command = $"exec {Container} sh -c \"if [ -f {FullPath} ]; then echo 'true'; else echo 'false'; fi\"";
        string? result = RunCommand(command).StandardOutput;
        return ref Unsafe.AsRef(result is "true");
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private readonly void CheckFileAccess()
    {
        if (Path.HasExtension(FullPath) is false) return;

        string command = $"exec {Container} sh -c \"ls -l {FullPath}";
        if (RunCommand(command).StandardOutput?.FirstOrDefault()?.StartsWith(Constants.Dash) is true)
        {
            command = $"exec {Container} sh -c \"whoami\"";
            string user = RunCommand(command).StandardOutput!;

            command = $"exec {Container} sh -c \"chown {user} {FullPath}\"";
            _ = RunCommand(command);

            command = $"exec {Container} sh -c \"chmod ugo+wrx {FullPath}\"";
            _ = RunCommand(command);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void FormatString(string text)
    {
        _ = text.Replace("\"", "\\\"").Replace(@"\r", @"\\r").Replace(@"\n", @"\\n");
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal static string FormatString(ReadOnlySpan<char> textSpan)
    {
        StringBuilder stringBuilder = StringBuilderCache.Acquire();
        for (int i = 0; i < textSpan.Length; i++)
        {
            ref char character = ref Unsafe.AsRef(textSpan[i]);

            switch (character)
            {
                case Constants.DoubleQuotes:
                    _ = stringBuilder!.Append(string.Intern("\\\""));
                    break;
                case char.MinValue or Constants.NonUnicode:
                    break;
                default:
                    _ = stringBuilder!.Append(character);
                    break;
            }
        }
        return stringBuilder.ToString();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CommandLineResult RunCommand(string command)
    {
        return CommandLineExecutors.RunProcess(EXE, command, CancellationToken.None);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Append(string text)
    {
        text = FormatString(text.AsSpan());
        string command = $"exec {Container} sh -c \"echo -n '{text}' >> {FullPath}\"";
        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void AppendLine(string text)
    {
        text = FormatString(text.AsSpan());
        string command = $"exec {Container} sh -c \"echo '{text}' >> {FullPath}\"";
        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void CopyFrom(string sourceFilePath, bool copyToRemoteLocation = false)
    {
        string command;

        if (copyToRemoteLocation)
        {
            command = $"cp {sourceFilePath} {Container}:{FullPath}";
        }
        else
        {
            command = $"exec {Container} sh -c \"cp {sourceFilePath} {FullPath}\"";
        }

        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void CopyTo(string destinationFilePath, bool copyToRemoteLocation = false)
    {
        string command;

        if (copyToRemoteLocation)
        {
            command = $"cp {FullPath} {Container}:{destinationFilePath}";
        }
        else
        {
            command = $"exec {Container} sh -c \"cp {FullPath} {destinationFilePath}\"";
        }

        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void CreateDirectory(string directory)
    {
        string command = $"exec {Container} sh -c \"mkdir {directory}\"";
        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Delete()
    {
        string command = $"exec {Container} sh -c \"rm -f {FullPath}\"";
        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Delete(string filePath)
    {
        string command = $"exec {Container} sh -c \"rm -f {filePath}\"";
        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void DeleteDirectory(string directory)
    {
        string command = $"exec {Container} sh -c \"rm -rf {directory}\"";
        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly bool DirectoryExists(string directory)
    {
        string command = $"exec {Container} sh -c \"if [ -d {directory} ]; then echo 'true'; else echo 'false'; fi\"";
        string? result = RunCommand(command).StandardOutput;
        return result is "true";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Edit(string text, Action<string, IPathInfo> editMethod)
    {
        editMethod?.Invoke(text, this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string[] EnumerateDirectoryFiles(string directory, string? searchPattern = null)
    {
        string? command;

        if (string.IsNullOrEmpty(searchPattern))
        {
            command = $"exec {Container} sh -c \"ls {directory}\"";
        }
        else
        {
            command = $"exec {Container} sh -c \"find {directory} -name \"{searchPattern}\"\"";
        }

        string? result = RunCommand(command).StandardOutput;

        return GetLines(result!);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly IPathInfo[] GetDirectoryFiles(string directory, string? searchPattern = null)
    {
        Span<string> fileNames = EnumerateDirectoryFiles(directory, searchPattern);

        IPathInfo[] files = new IPathInfo[fileNames.Length];

        for (int i = 0; i < fileNames.Length; i++)
        {
            files[i] = new DockerPathInfo(fileNames[i], Container);
        }

        return files;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly DateTimeOffset GetCreationDate()
    {
        string command = $"exec {Container} sh -c \"stat -c '%w' {FullPath}\"";
        string date = RunCommand(command).StandardOutput!;

        _ = DateTimeOffset.TryParse(date, out DateTimeOffset dateUtc);

        return dateUtc;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string GetExtension(bool includePeriod = true)
    {
        string extension = Path.GetExtension(GetFileName(true));

        if (includePeriod)
        {
            return extension;
        }
        else
        {
            return new string(extension.AsSpan().Slice(1)).AsCached();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string GetFileName(bool withExtension = false)
    {
        if (withExtension)
        {
            return Path.GetFileName(FullPath);
        }
        else
        {
            return Path.GetFileNameWithoutExtension(FullPath);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string GetParentDirectory(string path)
    {
        string command = $"exec {Container} sh -c \"dirname {path}\"";
        string? parent = RunCommand(command).StandardOutput;
        return parent!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string GetPathFromDirectory(params string[] pathTokens)
    {
        StringBuilder path = StringBuilderCache.Acquire();

        _ = path.Append(Directory);

        for (int i = 0; i < pathTokens.Length; i++)
        {
            _ = path.Append(Constants.ForwardSlash);
            _ = path.Append(pathTokens[i]);
        }

        return path.ToString();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void MoveTo(string destinationFilePath, bool moveToRemoteLocation = false)
    {
        CopyTo(destinationFilePath, moveToRemoteLocation);
        Delete();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string[] ReadAllLines()
    {
        string command = $"exec {Container} sh -c \"cat {FullPath}\"";
        string? result = RunCommand(command).StandardOutput;
        return GetLines(result!);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string ReadAllText()
    {
        string command = $"exec {Container} sh -c \"cat {FullPath}\"";
        return RunCommand(command).StandardOutput!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Replace(string content)
    {
        content = FormatString(content.AsSpan());
        string command = $"exec {Container} sh -c \"echo '{content}' > {FullPath}\"";
        _ = RunCommand(command);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void WriteAllLines(string[] lines)
    {
        string tempFile = Path.Combine(Path.GetTempPath(), GetFileName(true));
        File.WriteAllLines(tempFile, lines);
        CopyFrom(tempFile, true);
        File.Delete(tempFile);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void WriteAllText(string content)
    {
        content = FormatString(content.AsSpan());
        string command = $"exec {Container} sh -c \"echo '{content}' > {FullPath}\"";
        _ = RunCommand(command);
    }
}