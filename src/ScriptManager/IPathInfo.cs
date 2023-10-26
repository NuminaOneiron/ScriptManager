using ScriptManager.Enums;

namespace ScriptManager;

public interface IPathInfo
{
    ServerEnvironmentType Environment { get; }

    ref readonly string Directory { get; }

    string FullPath { get; init; }

    ref readonly bool Exists { get; }

    ref readonly int Length { get; }

    void Append(string text);

    void AppendLine(string text);

    void CopyFrom(string sourceFilePath, bool copyToRemoteLocation = false);

    void CopyTo(string destinationFilePath, bool copyToRemoteLocation = false);

    void CreateDirectory(string directory);

    void Delete();

    void Delete(string filePath);

    void DeleteDirectory(string directory);

    bool DirectoryExists(string directory);

    void Edit(string text, Action<string, IPathInfo> editMethod)
    {
        editMethod?.Invoke(text, this);
    }

    string[] EnumerateDirectoryFiles(string directory, string? searchPattern = null);

    IPathInfo[] GetDirectoryFiles(string directory, string? searchPattern = null);

    DateTimeOffset GetCreationDate();

    string GetExtension(bool includePeriod = true)
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

    string GetFileName(bool withExtension = false)
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

    string GetParentDirectory(string path);

    string GetPathFromDirectory(params string[] pathTokens);

    void MoveTo(string destinationFilePath, bool moveToRemoteLocation = false)
    {
        CopyTo(destinationFilePath, moveToRemoteLocation);
        Delete();
    }

    string[] ReadAllLines();

    string ReadAllText();

    void Replace(string content);

    void WriteAllLines(string[] lines);

    void WriteAllText(string content);
}
