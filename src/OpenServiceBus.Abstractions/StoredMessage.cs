namespace OpenServiceBus.Abstractions;

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
}
