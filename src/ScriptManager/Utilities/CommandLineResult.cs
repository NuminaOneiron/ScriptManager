using System.Text;

namespace ScriptManager.Utilities;

public sealed record CommandLineResult : IDisposable
{
    [ThreadStatic]
    private static List<string> _cachedErrorOutputLines = new List<string>();

    [ThreadStatic]
    private static List<string> _cachedStandardOutputLines = new List<string>();

    public StringValues? StandardOutput { get; set; } = default!;

    public StringValues? ErrorOutput { get; set; } = default!;

    public TimeSpan? ExecutionTime { get; set; } = default!;


    public CommandLineResult()
    {
    }

    public CommandLineResult(StringValues? standardOutput, StringValues? errorOutput, TimeSpan? executionTime)
    {
        StandardOutput = standardOutput;
        ErrorOutput = errorOutput;
        ExecutionTime = executionTime;
    }


    public void InsertStandardOutput(StreamReader reader)
    {
        StringBuilder stringBuilder = StringBuilderCache.Acquire();

        ReadOnlySpan<char> chars = reader.ReadToEnd();

        StringSegment output = stringBuilder.Append(chars).ToString();

        if (StringSegment.IsNullOrEmpty(output) is false)
        {
            StandardOutput = output.Trim().Value;
        }
        else
        {
            StandardOutput = null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendStandardOutput(string text)
    {
        _cachedStandardOutputLines.Add(text);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ConsolidateStandardOutput()
    {
        StandardOutput = _cachedStandardOutputLines.ToArray();

        _cachedStandardOutputLines.Clear();
    }


    public void InsertErrorOutput(StreamReader reader)
    {
        StringBuilder stringBuilder = StringBuilderCache.Acquire();

        ReadOnlySpan<char> chars = reader.ReadToEnd();

        StringSegment output = stringBuilder.Append(chars).ToString();

        if (StringSegment.IsNullOrEmpty(output) is false)
        {
            ErrorOutput = output.Trim().Value;
        }
        else
        {
            ErrorOutput = null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendErrorOutput(string text)
    {
        _cachedErrorOutputLines.Add(text);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ConsolidateErrorOutput()
    {
        ErrorOutput = _cachedErrorOutputLines.ToArray();

        _cachedErrorOutputLines.Clear();
    }


    public void Dispose()
    {
        _cachedStandardOutputLines.Clear();
        _cachedErrorOutputLines.Clear();
    }
}