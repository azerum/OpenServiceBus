namespace OpenServiceBus.Core.Entities;

/// <summary>
/// Configuration for a single queue entity.
/// Mirrors the relevant subset of <c>Microsoft.ServiceBus.Messaging.QueueDescription</c>
/// so that the official Service Bus emulator's <c>config.json</c> can be loaded directly.
/// Additional fields land milestone by milestone (lock duration in M3, max delivery count in M5,
/// TTL in M6, scheduled-enqueue handling in M7, etc.).
/// </summary>
public sealed record QueueDescriptor
{
    public required string Name { get; init; }

    /// <summary>
    /// Maximum delivery attempts before a message is dead-lettered. Enforced in M5.
    /// Default matches Azure Service Bus.
    /// </summary>
    public int MaxDeliveryCount { get; init; } = 10;

    /// <summary>
    /// Peek-lock duration handed to consumers when a message is delivered. Enforced in M3.
    /// Default matches Azure Service Bus.
    /// </summary>
    public TimeSpan LockDuration { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Whether expired messages are dead-lettered or dropped. Enforced in M6.
    /// </summary>
    public bool DeadLetteringOnMessageExpiration { get; init; }

    /// <summary>
    /// Per-queue default message TTL. Per-message TTL still wins when shorter. Enforced in M6.
    /// </summary>
    public TimeSpan? DefaultMessageTimeToLive { get; init; }
}
