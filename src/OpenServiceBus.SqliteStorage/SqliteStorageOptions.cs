namespace OpenServiceBus.SqliteStorage;

/// <summary>
/// Options for <see cref="SqliteMessageStore"/>. Use a file path for persistence across
/// restarts, or <c>:memory:</c> for tests. With a file path the store opens the database
/// in WAL mode so reads don't block writes - the AMQP listener can stamp deliveries while
/// the lock-expiration sweeper writes back.
/// </summary>
public sealed class SqliteStorageOptions
{
    /// <summary>
    /// Either an absolute path to a <c>.db</c> file or the special string <c>:memory:</c>.
    /// When using <c>:memory:</c> the database lives for the lifetime of the process and a
    /// shared cache name is auto-generated so multiple connections see the same data.
    /// </summary>
    public string DataSource { get; set; } = ":memory:";

    /// <summary>
    /// Polling interval used by <see cref="SqliteMessageStore.TryDequeueAsync"/> when no
    /// message is available - we wake on a per-queue notification when an enqueue happens
    /// in-process, but the timer guards against missed cross-process signals (e.g. another
    /// host writing to the same shared file). Default 250 ms.
    /// </summary>
    public TimeSpan DequeuePollInterval { get; set; } = TimeSpan.FromMilliseconds(250);
}
