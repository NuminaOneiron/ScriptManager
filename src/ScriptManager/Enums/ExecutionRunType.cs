namespace ScriptManager.Enums;
public enum ExecutionRunType : sbyte
{
    DefaultRun = 0,
    TestRun = 1,
    ScanOnly = 2,

    CreateBackup = 3,
    CreateSnapshot = 4,

    DeleteData = 5,
    DeleteBackup = 6,
    DeleteSnapshot = 7,

    RestoreData = 8,
    RestoreBackup = 9,
    RestoreSnapshot = 10
}