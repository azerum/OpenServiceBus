namespace OpenServiceBus.Core.Messaging;

/// <summary>
/// A session lock held by a receiver against a session-enabled entity. While the lock is
/// alive the broker only delivers messages whose <see cref="StoredMessage.SessionId"/>
/// matches <see cref="SessionId"/> to that receiver, and other session receivers cannot
/// take ownership of the same session.
/// </summary>
public sealed record SessionLock
{
    /// <summary>The session id the receiver has claimed.</summary>
    public required string SessionId { get; init; }

    /// <summary>UTC deadline after which the lock expires and another receiver can take the session.</summary>
    public required DateTimeOffset LockedUntil { get; init; }

    /// <summary>Receiver-link name that holds the lock. Used to enforce lock-link affinity on $management ops.</summary>
    public required string? LinkName { get; init; }
}
