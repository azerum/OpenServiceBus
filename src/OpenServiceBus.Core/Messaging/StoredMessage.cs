namespace OpenServiceBus.Core.Messaging;

/// <summary>
/// A message persisted in the broker. The body is the raw, encoded AMQP message bytes
/// so the wire data round-trips exactly. Decoding happens only at delivery time
/// when the broker stamps system properties (M4) or applies disposition logic (M5+).
/// </summary>
public sealed record StoredMessage
{
    public required long SequenceNumber { get; init; }

    public required DateTimeOffset EnqueuedAt { get; init; }

    /// <summary>The raw AMQP-encoded message bytes (an opaque <c>Message.Encode()</c> payload).</summary>
    public required byte[] EncodedMessage { get; init; }

    /// <summary>
    /// Number of unsuccessful previous delivery attempts. Stamped onto the outgoing AMQP
    /// <c>header.delivery-count</c> on each delivery. Starts at 0, incremented on abandon / lock expiry.
    /// </summary>
    public int DeliveryCount { get; init; }

    /// <summary>
    /// Absolute UTC deadline after which the message is considered expired (M6). Null = no TTL.
    /// Computed at enqueue time as <c>now + min(perMessageTtl, queueDefaultTtl)</c>.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>True when this message has passed its TTL deadline.</summary>
    public bool IsExpired(DateTimeOffset now) => ExpiresAt is not null && ExpiresAt.Value <= now;

    /// <summary>
    /// Absolute UTC time at which this scheduled message becomes available for delivery (M7).
    /// Null means the message is not scheduled (available immediately on enqueue).
    /// </summary>
    public DateTimeOffset? ScheduledEnqueueTime { get; init; }

    /// <summary>True when this message is scheduled and its activation time has not yet arrived.</summary>
    public bool IsScheduled(DateTimeOffset now) => ScheduledEnqueueTime is not null && ScheduledEnqueueTime.Value > now;

    /// <summary>
    /// True when this message has been deferred via <c>DeferAsync</c> (M8). Deferred messages
    /// are invisible to the normal receive path and can only be retrieved by sequence number
    /// via <c>ReceiveDeferredMessagesAsync</c>.
    /// </summary>
    public bool IsDeferred { get; init; }
}
