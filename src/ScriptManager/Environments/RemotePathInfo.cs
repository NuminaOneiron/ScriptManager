using System.Buffers;
using System.Text;
using System.Text.RegularExpressions;

using ScriptManager.Enums;
using ScriptManager.Extensions;
using ScriptManager.Utilities;

namespace ScriptManager.Environments;

public readonly partial struct RemotePathInfo : IPathInfo
{
    private const string EXE = "net.exe";

    private readonly string _remoteFullPath = default!;

    public ServerEnvironmentType Environment { get; } = ServerEnvironmentType.Remote;

    public ref readonly string Directory { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref GetDirectoryName()!; }

    public readonly char DriveLetter { get; init; }

    public ref readonly bool Exists { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref FileExists(); }

    public readonly string FullPath { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => GetFullName(); init { } }

    public ref readonly int Length { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref GetLength(); }

    public readonly string LocalPath { get; init; }

    public readonly IPathInfo? ServerPath { get; init; }


    public RemotePathInfo(string path, in PathEnvironmentInfo? environmentInfo)
    {
        LocalPath = environmentInfo?.LocalPath!.Trim()!;

        DriveLetter = LocalPath[0];

        if (path.StartsWith(@"\\".AsCached(), StringComparison.Ordinal))
        {
            _remoteFullPath = path;

            (string Filename, string Folder) = GetRelativePath(path);

            ServerPath = string.IsNullOrEmpty(environmentInfo?.ContainerName) is false ? new DockerPathInfo($"{environmentInfo?.ServerPath}/{Folder}/{Filename}", environmentInfo?.ContainerName!) : null!;
        }
        else
        {
            string uncPath = GetUNCPath();

            DirectoryInfo directory = new DirectoryInfo(uncPath);

            string filename = Path.GetFileName(path);

            FileInfo? file = directory.EnumerateFiles($"*{Path.GetExtension(filename)}", SearchOption.AllDirectories).FirstOrDefault(x => x.Name == filename);

            _remoteFullPath = file?.FullName!;

            ServerPath = string.IsNullOrEmpty(environmentInfo?.ContainerName) is false ? new DockerPathInfo(path, environmentInfo?.ContainerName!) : null!;
        }
    }

    public RemotePathInfo(in PathEnvironmentInfo? environmentInfo, Span<string> pathTokens)
    {
        LocalPath = environmentInfo?.LocalPath!.Trim()!;

        DriveLetter = LocalPath[0];

        Span<string> serverPathTokens = GetServerTokens(in environmentInfo, pathTokens);

        ServerPath = string.IsNullOrEmpty(environmentInfo?.ContainerName) is false ? new DockerPathInfo(environmentInfo?.ContainerName!, serverPathTokens) : null!;

        StringBuilder path = StringBuilderCache.Acquire();
        for (int i = 0; i < pathTokens.Length; i++)
        {
            if (i > 0) _ = path.Append(Constants.BackSlash);
            _ = path.Append(pathTokens[i]);
        }

        _remoteFullPath = path.ToString();
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string GetUNCPath()
    {
        if (OperatingSystem.IsWindows())
        {
            using CommandLineResult result = CommandLineExecutors.RunProcess(EXE, $"use {DriveLetter}:", CancellationToken.None);

            string? remotePath = GetUNCPathPattern().Match(result.StandardOutput!).Captures.FirstOrDefault()?.Value;

            string? relativePath = Path.GetRelativePath(Path.GetPathRoot(LocalPath)!, LocalPath!);

            return Path.Combine(remotePath!, relativePath);
        }

        return string.Empty;
    }

    [GeneratedRegex("[^\\s][\\\\].+[^\\s]", RegexOptions.Compiled)]
    private static partial Regex GetUNCPathPattern();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (string Filename, string Folder) GetRelativePath(string path)
    {
        string[] pathTokens = path.AsSplit(Constants.BackSlash);

        (string Filename, string Folder) relativePath = (Path.GetFileName(path), pathTokens[pathTokens.Length - 2]);

        ArrayPool<string>.Shared.Return(pathTokens);

        return relativePath;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static Span<string> GetServerTokens(in PathEnvironmentInfo? environmentInfo, Span<string> pathTokens)
    {
        Span<string> serverPathTokens = new string[3];
        if (pathTokens[0]?.StartsWith(@"\\", StringComparison.Ordinal) is true)
        {
            StringBuilder path = StringBuilderCache.Acquire();
            _ = path.Append(pathTokens[0]);
            for (int i = 1; i < pathTokens.Length; i++)
            {
                _ = path.Append(Constants.BackSlash);
                string value = pathTokens[i];
                if (string.IsNullOrEmpty(value))
                {
                    _ = path.Append(value);
                }
            }

            (string Filename, string Folder) = GetRelativePath(path.ToString());
            serverPathTokens[0] = environmentInfo?.ServerPath!;
            serverPathTokens[1] = Folder;
            serverPathTokens[2] = Filename;
        }

        return serverPathTokens;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ref string GetDirectoryName()
    {
        return ref Unsafe.AsRef(Path.GetDirectoryName(_remoteFullPath))!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly string GetFullName()
    {
        if (ServerPath is not null)
        {
            return ServerPath.FullPath!;
        }
        else
        {
            return _remoteFullPath;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ref int GetLength()
    {
        if (ServerPath is not null)
        {
            return ref Unsafe.AsRef(ServerPath.Length)!;
        }
        else
        {
            return ref Unsafe.AsRef(File.ReadAllText(_remoteFullPath).Length);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ref bool FileExists()
    {
        return ref Unsafe.AsRef(File.Exists(_remoteFullPath));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Append(string text)
    {
        if (ServerPath is not null)
        {
            ServerPath.Append(text);
        }
        else
        {
            using StreamWriter writer = File.AppendText(_remoteFullPath);
            writer.Write(text);
            writer?.Flush();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void AppendLine(string text)
    {
        if (ServerPath is not null)
        {
            ServerPath.AppendLine(text);
        }
        else
        {
            using StreamWriter writer = File.AppendText(_remoteFullPath);
            writer.WriteLine(text);
            writer?.Flush();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void CopyFrom(string sourceFilePath, bool copyToRemoteLocation = false)
    {
        if (ServerPath is not null)
        {
            ServerPath.CopyFrom(sourceFilePath, copyToRemoteLocation);
        }
        else
        {
            if (copyToRemoteLocation)
            {
                File.Copy(sourceFilePath, _remoteFullPath);
            }
            else
            {
                File.Copy(sourceFilePath, _remoteFullPath);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void CopyTo(string destinationFilePath, bool copyToRemoteLocation = false)
    {
        if (ServerPath is not null)
        {
            ServerPath.CopyTo(destinationFilePath, copyToRemoteLocation);
        }
        else
        {
            if (copyToRemoteLocation)
            {
                File.Copy(_remoteFullPath, destinationFilePath);
            }
            else
            {
                File.Copy(_remoteFullPath, destinationFilePath);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void CreateDirectory(string directory)
    {
        _ = System.IO.Directory.CreateDirectory(directory);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Delete()
    {
        File.Delete(_remoteFullPath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Delete(string filePath)
    {
        File.Delete(filePath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void DeleteDirectory(string directory)
    {
        System.IO.Directory.Delete(directory);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly bool DirectoryExists(string directory)
    {
        return System.IO.Directory.Exists(directory);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Edit(string text, Action<string, IPathInfo> editMethod)
    {
        editMethod?.Invoke(text, this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string[] EnumerateDirectoryFiles(string directory, string? searchPattern = null)
    {
        if (string.IsNullOrEmpty(searchPattern))
        {
            return System.IO.Directory.GetFiles(directory);
        }
        else
        {
            return System.IO.Directory.GetFiles(directory, searchPattern);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly IPathInfo[] GetDirectoryFiles(string directory, string? searchPattern = null)
    {
        Span<string> fileNames = EnumerateDirectoryFiles(directory, searchPattern);

        IPathInfo[] files = new IPathInfo[fileNames.Length];

        for (int i = 0; i < fileNames.Length; i++)
        {
            files[i] = new RemotePathInfo(fileNames[i], this.GetPathEnvironmentInfo());
        }

        return files;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly DateTimeOffset GetCreationDate()
    {
        return File.GetCreationTimeUtc(_remoteFullPath);
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
            return extension.Substring(1);
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
        return Path.GetDirectoryName(path)!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string GetPathFromDirectory(params string[] pathTokens)
    {
        StringBuilder path = StringBuilderCache.Acquire();

        _ = path.Append(Directory);

        for (int i = 0; i < pathTokens.Length; i++)
        {
            _ = path.Append(Constants.BackSlash);
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
        return File.ReadAllLines(_remoteFullPath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string ReadAllText()
    {
        return File.ReadAllText(_remoteFullPath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Replace(string content)
    {
        File.WriteAllText(_remoteFullPath, content);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void WriteAllLines(string[] lines)
    {
        File.WriteAllLines(_remoteFullPath, lines);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void WriteAllText(string content)
    {
        File.WriteAllText(_remoteFullPath, content);
    }
}