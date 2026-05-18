namespace OpenServiceBus.Abstractions;

/// <summary>
/// Per-entity message persistence and peek-lock boundary. The in-memory implementation
/// lives in <c>OpenServiceBus.Broker</c>; a SQLite-backed implementation arrives post-MVP
/// without breaking call sites.
/// </summary>
public interface IMessageStore
{
    /// <summary>Allocate the storage for a queue. Called once at queue creation.</summary>
    Task CreateQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Remove a queue and discard its messages. Called once at queue deletion.</summary>
    Task DeleteQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueue an encoded message. The returned record carries the assigned sequence number.
    /// </summary>
    /// <param name="expiresAt">
    /// Absolute UTC deadline after which the message is considered expired (M6). Null = no TTL.
    /// The caller is responsible for computing this from per-message TTL and queue-default TTL.
    /// </param>
    Task<StoredMessage> EnqueueAsync(
        string queueName,
        byte[] encodedMessage,
        DateTimeOffset? expiresAt = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Remove and return any non-locked messages whose <see cref="StoredMessage.ExpiresAt"/>
    /// has passed. Locked messages are not touched; they re-enter expiration eligibility when
    /// abandoned or when their lock expires.
    /// </summary>
    IReadOnlyList<StoredMessage> ExpireMessages(string queueName, DateTimeOffset now);

    /// <summary>The number of currently enqueued messages on the queue (active + locked, but not yet completed).</summary>
    Task<long> CountAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Wait for and lock the next available message under peek-lock for <paramref name="lockDuration"/>.
    /// Returns <c>null</c> only on cancellation.
    /// </summary>
    Task<LockedMessage?> TryDequeueAsync(
        string queueName,
        TimeSpan lockDuration,
        CancellationToken cancellationToken = default);

    /// <summary>Complete a locked message. Returns false if the lock token is unknown (e.g. already expired or completed).</summary>
    Task<bool> TryCompleteAsync(
        string queueName,
        Guid lockToken,
        CancellationToken cancellationToken = default);

    /// <summary>Abandon a locked message — release the lock and make it available for redelivery.</summary>
    Task<bool> TryAbandonAsync(
        string queueName,
        Guid lockToken,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Expire any locks whose deadlines have passed. Returns the number of locks released.
    /// Called periodically by <c>LockManager</c>; tests can call this directly to drive expiration deterministically.
    /// </summary>
    int ExpireLocks(string queueName, DateTimeOffset now);

    /// <summary>
    /// Extend a peek-lock by another <paramref name="lockDuration"/> from now.
    /// Returns the new locked-until timestamp, or <c>null</c> if the lock token is unknown
    /// (e.g. already completed or expired).
    /// </summary>
    Task<DateTimeOffset?> TryRenewLockAsync(
        string queueName,
        Guid lockToken,
        TimeSpan lockDuration,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically remove a locked message — releases the lock AND removes the stored message,
    /// returning what was stored so the caller can re-enqueue it elsewhere (e.g. to the DLQ).
    /// Returns <c>null</c> if the lock token is unknown.
    /// </summary>
    Task<StoredMessage?> TryRemoveLockedAsync(
        string queueName,
        Guid lockToken,
        CancellationToken cancellationToken = default);
}
