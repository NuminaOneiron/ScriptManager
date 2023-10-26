using ScriptManager.Enums;

namespace ScriptManager;

public interface ScriptHistory
{
    int SequenceNumber { get; set; }

    string Author { get; set; }

    string Description { get; set; }

    ScriptStatusType Status { get; set; }

    DateTimeOffset CreatedDate { get; set; }
}
