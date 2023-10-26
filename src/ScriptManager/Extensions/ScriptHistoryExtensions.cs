using System.Text;

using ScriptManager.Enums;
using ScriptManager.Utilities;

namespace ScriptManager.Extensions;

internal static class ScriptHistoryExtensions
{
    public static void Validate(this ScriptHistory script, DataSourceType dataSource, DataProviderType dataProvider)
    {
        if (script.Author?.Length > 30)
        {
            script.Author = string.Create(30, script.Author, (chars, state) =>
            {
                for (int i = 0; i < chars.Length; i++)
                {
                    chars[i] = state[i];
                }
            });
        }

        if (script.Description?.Length > 50)
        {
            script.Description = string.Create(50, script.Description, (chars, state) =>
            {
                for (int i = 0; i < chars.Length; i++)
                {
                    chars[i] = state[i];
                }
            });
        }

        if (dataSource is DataSourceType.Csv)
        {
            script.Author!.ReplaceChar(Constants.Comma, char.MinValue);
            script.Description!.ReplaceChar(Constants.Comma, char.MinValue);
        }
        else if (dataSource is DataSourceType.Json)
        {
            script.Author!.ReplaceChar(Constants.DoubleQuotes, Constants.SingleQuote);
            script.Description!.ReplaceChar(Constants.DoubleQuotes, Constants.SingleQuote);
        }
        else if (dataSource is DataSourceType.Internal)
        {
            if (dataProvider is DataProviderType.MSSQLServer)
            {
                script.Author!.ReplaceChar(Constants.SingleQuote, Constants.Backtick);
                script.Description!.ReplaceChar(Constants.SingleQuote, Constants.Backtick);
            }
        }
    }

    public static string ToCsvEntry(this ScriptHistory script)
    {
        StringBuilder scriptEntry = StringBuilderCache.Acquire();

        _ = scriptEntry.AppendCached(script.SequenceNumber.AsString());
        _ = scriptEntry.Append(Constants.Comma);

        _ = scriptEntry.AppendCached(script.Author);
        _ = scriptEntry.Append(Constants.Comma);

        _ = scriptEntry.AppendCached(script.Description);
        _ = scriptEntry.Append(Constants.Comma);

        _ = scriptEntry.AppendCached(script.Status.AsString());
        _ = scriptEntry.Append(Constants.Comma);

        _ = scriptEntry.AppendCached(script.CreatedDate.AsString());

        return scriptEntry.ToString();
    }

    public static string ToJsonEntry(this ScriptHistory script, bool appendComma = false)
    {
        StringBuilder scriptEntry = StringBuilderCache.Acquire();
        _ = scriptEntry.Append(Constants.OpenCurlyBracket);

        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(nameof(ScriptHistory.SequenceNumber));
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.Append(Constants.Colon);
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(script.SequenceNumber.AsString());
        _ = scriptEntry.Append(Constants.DoubleQuotes);

        _ = scriptEntry.Append(Constants.Comma);

        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(nameof(ScriptHistory.Author));
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.Append(Constants.Colon);
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(script.Author);
        _ = scriptEntry.Append(Constants.DoubleQuotes);

        _ = scriptEntry.Append(Constants.Comma);

        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(nameof(ScriptHistory.Description));
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.Append(Constants.Colon);
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(script.Description);
        _ = scriptEntry.Append(Constants.DoubleQuotes);

        _ = scriptEntry.Append(Constants.Comma);

        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(nameof(ScriptHistory.Status));
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.Append(Constants.Colon);
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(script.Status.AsString());
        _ = scriptEntry.Append(Constants.DoubleQuotes);

        _ = scriptEntry.Append(Constants.Comma);

        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(nameof(ScriptHistory.CreatedDate));
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.Append(Constants.Colon);
        _ = scriptEntry.Append(Constants.DoubleQuotes);
        _ = scriptEntry.AppendCached(script.CreatedDate.AsString());
        _ = scriptEntry.Append(Constants.DoubleQuotes);

        _ = scriptEntry.Append(Constants.CloseCurlyBracket);

        if (appendComma)
        {
            _ = scriptEntry.Append(Constants.Comma);
        }

        return scriptEntry.ToString();
    }
}