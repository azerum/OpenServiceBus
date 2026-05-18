namespace OpenServiceBus.Core.Entities;

/// <summary>
/// Configuration for a single topic entity. Topics themselves don't store messages — they're
/// fan-out endpoints. Each subscription on the topic owns its own message store, peek-lock
/// lifecycle, and DLQ (see <see cref="SubscriptionDescriptor"/>).
/// </summary>
public sealed record TopicDescriptor
{
    public required string Name { get; init; }

    /// <summary>Per-topic default message TTL; applies to messages enqueued without their own TTL.</summary>
    public TimeSpan? DefaultMessageTimeToLive { get; init; }
}
