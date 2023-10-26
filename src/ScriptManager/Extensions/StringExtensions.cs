using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Text;

using ScriptManager.Enums;
using ScriptManager.Utilities;

namespace ScriptManager.Extensions;

internal static class StringExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static StringBuilder AppendCached(this StringBuilder stringBuilder, ReadOnlySpan<char> value)
    {
        return stringBuilder.Append(value.AsCachedSpan());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static StringBuilder AppendLineCached(this StringBuilder stringBuilder, ReadOnlySpan<char> value)
    {
        return stringBuilder.Append(Environment.NewLine).Append(value.AsCachedSpan());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string AsCached<T>(this T number) where T : INumber<T>
    {
        return StringPoolCache.GetOrAdd(number.AsString());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string AsCached(this string value)
    {
        return StringPoolCache.GetOrAdd(value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string AsCached(this ReadOnlySpan<char> value)
    {
        return StringPoolCache.GetOrAdd(value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<char> AsCachedSpan(this ReadOnlySpan<char> value)
    {
        return StringPoolCache.GetOrAdd(value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string AsInterned(this string value)
    {
        return string.Intern(value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string[] AsSplit(this string text, char separator)
    {
        char[] token = ArrayPool<char>.Shared.Rent(1);
        token[0] = separator;

        StringTokenizer stringTokenizer = new StringTokenizer(text, token);

        ArrayPool<char>.Shared.Return(token);

        int count;
        if (stringTokenizer.TryGetNonEnumeratedCount(out count)) count = stringTokenizer.Count();

        string[] split = ArrayPool<string>.Shared.Rent(count);

        StringTokenizer.Enumerator stringTokens = stringTokenizer.GetEnumerator();

        int i = 0;
        while (stringTokens.MoveNext())
        {
            split[i] = stringTokens.Current.Value!;
            i++;
        }

        return split;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string AsString<T>(this T number) where T : INumber<T>
    {
        Span<char> numberBuffer = stackalloc char[20];

        Span<char> format = stackalloc char[1] { 'G' };

        _ = number.TryFormat(numberBuffer, out int charsWritten, format, CultureInfo.InvariantCulture);

        return new string(numberBuffer.Slice(0, charsWritten));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string AsString(this DateTime dateTime)
    {
        Span<char> dateTimeBuffer = stackalloc char[29];

        Span<char> format = stackalloc char[1] { 'G' };

        _ = dateTime.TryFormat(dateTimeBuffer, out int charsWritten, format, CultureInfo.InvariantCulture);

        return new string(dateTimeBuffer.Slice(0, charsWritten));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string AsString(this DateTimeOffset dateTime)
    {
        Span<char> dateTimeBuffer = stackalloc char[29];

        Span<char> format = stackalloc char[1] { 'G' };

        _ = dateTime.TryFormat(dateTimeBuffer, out int charsWritten, format, CultureInfo.InvariantCulture);

        return new string(dateTimeBuffer.Slice(0, charsWritten));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string AsString(this ScriptStatusType status)
    {
        return status switch
        {
            ScriptStatusType.SUCCESS => Constants.SUCCESS,
            ScriptStatusType.FAIL => Constants.FAIL,
            ScriptStatusType.IGNORE => Constants.IGNORE,
            _ => Constants.NONE
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public unsafe static void ReplaceChar(this string text, char oldValue, char newValue)
    {
        fixed (char* charPtr = text)
        {
            for (int i = 0; i < text.Length; i++)
            {
                if (*(charPtr + i) == oldValue)
                {
                    *(charPtr + i) = newValue;
                }
            }
        }
    }
}