using System.Text;

using Humanizer;

using ScriptManager.Enums;
using ScriptManager.Extensions;
using ScriptManager.Utilities;

namespace ScriptManager;

public sealed class Script : ScriptHistory
{
    public int SequenceNumber { get; set; } = default!;

    public bool IsAlreadyRan { get; set; } = false;

    public ScriptStatusType Status { get; set; } = ScriptStatusType.NONE;

    public ExecutionRunType ExecutionType { get; set; } = ExecutionRunType.DefaultRun;

    public DateTimeOffset CreatedDate { get; set; } = default!;

    public TimeSpan? ExecutionTime { get; set; } = default!;

    public string Author { get; set; } = default!;

    public string Description { get; set; } = default!;

    public string FilePath { get => File?.FullPath!; }

    public IPathInfo File { get; set; } = default!;

    public List<string>? ExceptionErrors { get; set; } = default;

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public override string ToString()
    {
        StringBuilder outputBuilder = StringBuilderCache.Acquire();

        _ = outputBuilder.AppendCached(nameof(ScriptHistory.SequenceNumber)).AppendCached(": ").AppendCached(SequenceNumber.AsString()).Append(Constants.SemiColon);

        _ = outputBuilder.Append(Constants.Whitespace).AppendCached(nameof(ScriptHistory.Author)).AppendCached(": ").AppendCached(Author).Append(Constants.SemiColon);

        _ = outputBuilder.Append(Constants.Whitespace).AppendCached(nameof(ScriptHistory.CreatedDate)).Append(": ").AppendCached(CreatedDate.Date.ToShortDateString()).Append(Constants.SemiColon);

        if (IsAlreadyRan)
        {
            _ = outputBuilder.Append(Constants.Whitespace).AppendCached(nameof(ScriptHistory.Status)).Append(": ").Append("ALREADY RAN").Append(Constants.SemiColon);
        }
        else if (Status is ScriptStatusType.NONE)
        {
            _ = outputBuilder.Append(Constants.Whitespace).AppendCached(nameof(ScriptHistory.Status)).AppendCached(": ").AppendCached("NEVER RAN").Append(Constants.SemiColon);
        }
        else
        {
            _ = outputBuilder.Append(Constants.Whitespace).AppendCached(nameof(ScriptHistory.Status)).AppendCached(": ").AppendCached(Status.AsString()).Append(Constants.SemiColon);
        }

        if (ExecutionTime is not null && ExecutionType is not ExecutionRunType.ScanOnly) _ = outputBuilder.Append(Constants.Whitespace).AppendCached("Execution Time: ").Append(ExecutionTime.Value.Humanize(3)).Append(Constants.SemiColon);

        if (ExceptionErrors?.Count > 0) _ = outputBuilder.Append(Constants.Whitespace).AppendCached("Exception Errors: ").AppendCached(ExceptionErrors.Count.AsString()).Append(Constants.SemiColon);

        return outputBuilder.ToString();
    }
}