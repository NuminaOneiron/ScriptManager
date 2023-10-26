using System.Text;

using ScriptManager.Enums;
using ScriptManager.Utilities;

namespace ScriptManager.Environments;

public readonly struct LocalPathInfo : IPathInfo
{
    public ServerEnvironmentType Environment { get; } = ServerEnvironmentType.Local;

    public ref readonly string Directory { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref Unsafe.AsRef(Path.GetDirectoryName(FullPath))!; }

    public readonly string FullPath { get; init; }

    public ref readonly bool Exists { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref FileExists(); }

    public ref readonly int Length { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ref GetLength(); }


    public LocalPathInfo(string path)
    {
        FullPath = path;
    }

    public LocalPathInfo(Span<string> pathTokens)
    {
        StringBuilder path = StringBuilderCache.Acquire();
        for (int i = 0; i < pathTokens.Length; i++)
        {
            if (i > 0) _ = path.Append(Constants.BackSlash);
            _ = path.Append(pathTokens[i]);
        }

        FullPath = path.ToString();
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ref int GetLength()
    {
        return ref Unsafe.AsRef(File.ReadAllText(FullPath).Length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly ref bool FileExists()
    {
        return ref Unsafe.AsRef(File.Exists(FullPath));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Append(string text)
    {
        using StreamWriter writer = File.AppendText(FullPath);
        writer.Write(text);
        writer?.Flush();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void AppendLine(string text)
    {
        using StreamWriter writer = File.AppendText(FullPath!);
        writer.WriteLine(text);
        writer?.Flush();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void CopyFrom(string sourceFilePath, bool copyToRemoteLocation = false)
    {
        if (copyToRemoteLocation)
        {
            File.Copy(sourceFilePath, FullPath!);
        }
        else
        {
            File.Copy(sourceFilePath, FullPath!);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void CopyTo(string destinationFilePath, bool copyToRemoteLocation = false)
    {
        if (copyToRemoteLocation)
        {
            File.Copy(FullPath, destinationFilePath);
        }
        else
        {
            File.Copy(FullPath, destinationFilePath);
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
        File.Delete(FullPath);
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
            files[i] = new LocalPathInfo(fileNames[i]);
        }

        return files;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly DateTimeOffset GetCreationDate()
    {
        return File.GetCreationTimeUtc(FullPath);
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
        return File.ReadAllLines(FullPath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly string ReadAllText()
    {
        return File.ReadAllText(FullPath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Replace(string content)
    {
        File.WriteAllText(FullPath, content);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void WriteAllLines(string[] lines)
    {
        File.WriteAllLines(FullPath, lines);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void WriteAllText(string content)
    {
        File.WriteAllText(FullPath, content);
    }
}