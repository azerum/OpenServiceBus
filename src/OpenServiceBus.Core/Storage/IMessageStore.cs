using OpenServiceBus.Core.Messaging;

namespace OpenServiceBus.Core.Storage;

/// <summary>
/// Per-entity message persistence and peek-lock boundary. The in-memory implementation
/// lives in <c>OpenServiceBus.InMemoryStorage</c>; a SQLite-backed implementation arrives post-MVP
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
    /// <param name="scheduledEnqueueTime">
    /// Absolute UTC time at which the message becomes available for delivery (M7). Null or
    /// past = available immediately. The store assigns a sequence number even for scheduled
    /// messages so callers (and the SDK) can reference them for cancellation.
    /// </param>
    Task<StoredMessage> EnqueueAsync(
        string queueName,
        byte[] encodedMessage,
        DateTimeOffset? expiresAt = null,
        DateTimeOffset? scheduledEnqueueTime = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Move any scheduled messages whose <see cref="StoredMessage.ScheduledEnqueueTime"/>
    /// has arrived into the available pool. Returns the number activated.
    /// </summary>
    int ActivateScheduled(string queueName, DateTimeOffset now);

    /// <summary>
    /// Cancel a scheduled message before it activates. Returns true if a scheduled message
    /// with that sequence number was found and removed. Returns false for unknown sequence
    /// numbers OR for messages that have already activated (the caller should use the normal
    /// disposition path to remove them).
    /// </summary>
    Task<bool> TryCancelScheduledAsync(
        string queueName,
        long sequenceNumber,
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
    /// <param name="associatedLinkName">
    /// Optional: the receiver link name that's taking the lock. When set, renew-lock requests
    /// for this token must declare the same link name in <c>associated-link-name</c> — matches
    /// Service Bus's lock-link affinity.
    /// </param>
    Task<LockedMessage?> TryDequeueAsync(
        string queueName,
        TimeSpan lockDuration,
        string? associatedLinkName = null,
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
    /// (e.g. already completed or expired) — or if <paramref name="requestingLinkName"/>
    /// doesn't match the link that originally took the lock.
    /// </summary>
    Task<DateTimeOffset?> TryRenewLockAsync(
        string queueName,
        Guid lockToken,
        TimeSpan lockDuration,
        string? requestingLinkName = null,
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

    /// <summary>
    /// Read messages without locking or removing them. Returns up to <paramref name="maxCount"/>
    /// messages with <c>SequenceNumber &gt;= fromSequenceNumber</c>, ordered by sequence number.
    /// Includes both Active and Scheduled messages — callers (the peek handler) flag the state
    /// to consumers. Locked messages ARE visible to Peek.
    /// </summary>
    IReadOnlyList<StoredMessage> Peek(string queueName, long fromSequenceNumber, int maxCount);
}
