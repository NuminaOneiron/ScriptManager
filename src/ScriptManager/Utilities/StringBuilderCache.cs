using System.Text;

namespace ScriptManager.Utilities;

internal static class StringBuilderCache
{
    internal const int MaxBuilderSize = 360;

    [ThreadStatic]
    private static StringBuilder? _cachedInstance;

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static StringBuilder Acquire(int capacity = MaxBuilderSize)
    {
        if (_cachedInstance is not null)
        {
            _ = _cachedInstance.Clear();

            _ = _cachedInstance.EnsureCapacity(capacity);

            return _cachedInstance;
        }

        _cachedInstance = new StringBuilder(capacity);

        return _cachedInstance;
    }

    public static StringBuilder AcquireInstance()
    {
        if (_cachedInstance is not null)
        {
            return _cachedInstance;
        }

        return Acquire();
    }
}