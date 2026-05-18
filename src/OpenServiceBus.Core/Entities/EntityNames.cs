namespace OpenServiceBus.Core.Entities;

/// <summary>
/// Service Bus entity name conventions — the suffixes and helpers used to reason about
/// sub-entities of a queue or topic (dead-letter queues, management endpoints, etc.).
/// Lives in Core because every adapter — AMQP routing, REST CRUD, future config loader —
/// needs to recognise these patterns.
/// </summary>
public static class EntityNames
{
    /// <summary>The suffix Service Bus uses for the dead-letter sub-entity of a queue.</summary>
    public const string DeadLetterSuffix = "/$DeadLetterQueue";

    /// <summary>The suffix for the per-entity AMQP <c>$management</c> request/response node.</summary>
    public const string ManagementSuffix = "/$management";

    /// <summary>The address of the broker-wide Claims-Based Security node.</summary>
    public const string CbsAddress = "$cbs";

    /// <summary>True when the given name identifies a dead-letter sub-entity.</summary>
    public static bool IsDeadLetterQueue(string name) =>
        name.EndsWith(DeadLetterSuffix, StringComparison.Ordinal);
}
