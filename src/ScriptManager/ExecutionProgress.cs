namespace ScriptManager;

public sealed class ExecutionProgress
{
    public int Current { get; set; }

    public int Total { get; set; }

    public void Reset()
    {
        Current = 0;
        Total = 0;
    }
}
