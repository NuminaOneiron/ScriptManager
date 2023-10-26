using System.Globalization;
using System.Text.RegularExpressions;

using ScriptManager.Extensions;
using ScriptManager.Utilities;

namespace ScriptManager;

public readonly partial struct ScriptCreator
{
    private readonly Func<IPathInfo, Script>? _scriptCreator;

    public ScriptCreator()
    {
    }

    public ScriptCreator(Func<IPathInfo, Script> scriptCreator)
    {
        _scriptCreator = scriptCreator;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal readonly Script Create(IPathInfo file)
    {
        if (file is null) return default!;

        if (_scriptCreator is not null)
        {
            return _scriptCreator.Invoke(file);
        }
        else
        {
            return DefaultScriptCreator(file);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal static Script DefaultScriptCreator(IPathInfo file)
    {
        Script script = new Script();
        string fileName = file.GetFileName(true);

        script.File = file;
        script.CreatedDate = file.GetCreationDate();
        script.Description = fileName.Trim(Constants.Zero, Constants.Hash, Constants.Dash, Constants.Underscore).AsCached();


        if (file.TryGetSequenceNumber(out int number))
        {
            script.SequenceNumber = number;
        }

        if (TryGetScriptDate(fileName, out DateTime dateTime))
        {
            script.CreatedDate = dateTime;
            StringPoolCache.Add(dateTime.ToShortTimeString());
        }

        if (TryGetScriptAuthor(fileName, out string author))
        {
            script.Author = author.AsCached();
        }
        else
        {
            script.Author = Environment.UserName.AsCached();
        }

        return script;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryGetScriptDate(string text, out DateTime dateTime)
    {
        _ = DateTime.UtcNow;

        ReadOnlySpan<char> values = GetScriptDatePattern().Match(text).ValueSpan;

        return DateTime.TryParse(values, CultureInfo.InvariantCulture, DateTimeStyles.None, out dateTime);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryGetScriptAuthor(string text, out string name)
    {
        name = string.Empty;

        ReadOnlySpan<char> trimChars = stackalloc char[4] { Constants.Zero, Constants.Hash, Constants.Dash, Constants.Underscore };

        ReadOnlySpan<char> span = GetScriptAuthorPattern().Match(text).ValueSpan.Trim(trimChars);

        if (span.IsEmpty is false || span.IsWhiteSpace() is false)
        {
            name = span.ToString();

            return true;
        }

        return false;
    }

    [GeneratedRegex("(\\d{1,4})([-_/\\s])(\\d{1,4}|\\w{3,9})\\2(\\d{1,4})", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, 200)]
    private static partial Regex GetScriptDatePattern();

    [GeneratedRegex("[\\p{L}][\\p{L} \\\\.'\\-()]+(?=\\.)", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, 200)]
    private static partial Regex GetScriptAuthorPattern();
}
