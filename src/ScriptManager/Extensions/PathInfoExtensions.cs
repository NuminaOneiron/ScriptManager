using System.Runtime.InteropServices;

using Microsoft.Win32.SafeHandles;

using ScriptManager.Enums;
using ScriptManager.Environments;
using ScriptManager.Utilities;

namespace ScriptManager.Extensions;

internal static class PathInfoExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryGetSequenceNumber(this IPathInfo file, out int number)
    {
        ReadOnlySpan<char> span = file.GetFileName().AsSpan();

        ReadOnlySpan<char> trimChars = stackalloc char[4] { Constants.Zero, Constants.Hash, Constants.Dash, Constants.Underscore };

        int endIndex = span.IndexOf(Constants.Underscore);

        ReadOnlySpan<char> numberSpan = span.TrimStart(trimChars).Slice(0, endIndex);

        return int.TryParse(numberSpan, out number);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IPathInfo CreatePathInfo(this in PathEnvironmentInfo environmentInfo)
    {
        return environmentInfo.Environment switch
        {
            ServerEnvironmentType.Local => new LocalPathInfo(environmentInfo.LocalPath!),
            ServerEnvironmentType.Docker => new DockerPathInfo(environmentInfo.ServerPath!, environmentInfo.ContainerName!),
            ServerEnvironmentType.Remote => new RemotePathInfo(environmentInfo.LocalPath!, environmentInfo),
            _ => null!
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PathEnvironmentInfo? GetPathEnvironmentInfo(this IPathInfo pathInfo)
    {
        switch (pathInfo.Environment)
        {
            case ServerEnvironmentType.Local:
                return new PathEnvironmentInfo { Environment = pathInfo.Environment, LocalPath = pathInfo.FullPath, ContainerName = null, ServerPath = null };
            case ServerEnvironmentType.Docker:
                ref DockerPathInfo dockerPath = ref Unsafe.As<IPathInfo, DockerPathInfo>(ref Unsafe.AsRef(pathInfo));
                return new PathEnvironmentInfo { Environment = pathInfo.Environment, LocalPath = null, ContainerName = dockerPath.Container, ServerPath = null };
            case ServerEnvironmentType.Remote:
                ref RemotePathInfo remotePath = ref Unsafe.As<IPathInfo, RemotePathInfo>(ref Unsafe.AsRef(pathInfo));
                return new PathEnvironmentInfo { Environment = pathInfo.Environment, LocalPath = remotePath.LocalPath, ContainerName = null, ServerPath = remotePath.ServerPath?.FullPath };
            default: return null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteTextAtPosition(this IPathInfo pathInfo, ReadOnlySpan<char> text, int start)
    {
        if (pathInfo.Exists is false) return;

        using SafeFileHandle handle = File.OpenHandle(pathInfo.FullPath, FileMode.Open, FileAccess.Write, options: FileOptions.RandomAccess);

        RandomAccess.Write(handle, MemoryMarshal.AsBytes(text), start);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public unsafe static string ReadTextAtPosition(this IPathInfo pathInfo, int start)
    {
        if (pathInfo.Exists is false) return string.Empty;

        using SafeFileHandle handle = File.OpenHandle(pathInfo.FullPath, FileMode.Open, FileAccess.Read, options: FileOptions.RandomAccess);

        int length = (int)RandomAccess.GetLength(handle);

        Span<byte> buffer = stackalloc byte[length];

        _ = RandomAccess.Read(handle, buffer, start);

        return new string(MemoryMarshal.Cast<byte, char>(buffer));
    }
}