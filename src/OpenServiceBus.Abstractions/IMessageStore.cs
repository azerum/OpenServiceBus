namespace OpenServiceBus.Abstractions;

/// <summary>
/// Per-entity message persistence boundary. The in-memory implementation lives in
/// <c>OpenServiceBus.Broker</c>; a SQLite-backed implementation arrives post-MVP without
/// breaking call sites.
/// </summary>
public interface IMessageStore
{
    /// <summary>Allocate the storage for a queue. Called once at queue creation.</summary>
    Task CreateQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Remove a queue and discard its messages. Called once at queue deletion.</summary>
    Task DeleteQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Enqueue an encoded message. The returned stored record carries the assigned sequence number.</summary>
    Task<StoredMessage> EnqueueAsync(
        string queueName,
        byte[] encodedMessage,
        CancellationToken cancellationToken = default);

    /// <summary>The number of currently enqueued (active) messages on the queue.</summary>
    Task<long> CountAsync(string queueName, CancellationToken cancellationToken = default);
}
