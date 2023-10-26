using System.Text;

namespace ScriptManager.Utilities;

/// <summary>
/// Provides a synchronized string pool for caching and reusing string instances.
/// </summary>
public static class StringPoolCache
{
    private static StringPool _stringPool = StringPool.Shared;

    /// <summary>
    /// Adds a <see cref="ReadOnlySpan{T}"/> of <see cref="char"/> to the string pool.
    /// </summary>
    /// <param name="span">The <see cref="ReadOnlySpan{T}"/> of <see cref="char"/> to be added to the pool.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.Synchronized)]
    public static void Add(ReadOnlySpan<char> span)
    {
        _ = _stringPool.GetOrAdd(span);
    }

    /// <summary>
    /// Retrieves a string from the pool based on the provided <see cref="ReadOnlySpan{T}"/> of <see cref="char"/> if available; otherwise, adds the span to the pool and returns the corresponding string.
    /// </summary>
    /// <param name="span">The <see cref="ReadOnlySpan{T}"/> of <see cref="char"/> to retrieve or add to the pool.</param>
    /// <returns>The retrieved or added string from the pool.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.Synchronized)]
    public static string GetOrAdd(ReadOnlySpan<char> span)
    {
        return _stringPool.GetOrAdd(span);
    }

    /// <summary>
    /// Retrieves a string from the pool based on the provided <see cref="ReadOnlySpan{T}"/> of <see cref="byte"/> and <see cref="Encoding"/> if available; otherwise, adds the span to the pool and returns the corresponding string.
    /// </summary>
    /// <param name="span">The <see cref="ReadOnlySpan{T}"/> of <see cref="byte"/> to retrieve or add to the pool.</param>
    /// <param name="encoding">The <see cref="Encoding"/> used to convert the byte span to a string.</param>
    /// <returns>The retrieved or added string from the pool.</returns>
    [MethodImpl(MethodImplOptions.Synchronized | MethodImplOptions.AggressiveInlining)]
    public static string GetOrAdd(ReadOnlySpan<byte> span, Encoding encoding)
    {
        return _stringPool.GetOrAdd(span, encoding);
    }

    /// <summary>
    /// Resets the string pool, clearing all cached strings.
    /// </summary>
    [MethodImpl(MethodImplOptions.Synchronized | MethodImplOptions.AggressiveInlining)]
    public static void Reset()
    {
        _stringPool.Reset();
    }

    /// <summary>
    /// Tries to retrieve a string from the pool based on the provided <see cref="ReadOnlySpan{T}"/> of <see cref="char"/> if available.
    /// </summary>
    /// <param name="span">The <see cref="ReadOnlySpan{T}"/> of <see cref="char"/> to retrieve from the pool.</param>
    /// <param name="value">When this method returns, contains the retrieved string if available; otherwise, null.</param>
    /// <returns>True if the string was retrieved from the pool; otherwise, false.</returns>
    [MethodImpl(MethodImplOptions.Synchronized | MethodImplOptions.AggressiveInlining)]
    public static bool TryGet(ReadOnlySpan<char> span, out string? value)
    {
        return _stringPool.TryGet(span, out value);
    }
}