namespace ScriptManager;

public readonly struct ConnectionStringInfo
{
    public required readonly string ConnectionString { get; init; }

    public required readonly string DataSource { get; init; }

    public required readonly string Database { get; init; }

    public required readonly string Username { get; init; }

    public required readonly string Password { get; init; }

    public ConnectionStringInfo(string connectionString, string dataSource, string database, string username, string password)
    {
        ConnectionString = connectionString;
        DataSource = dataSource;
        Database = database;
        Username = username;
        Password = password;
    }
}