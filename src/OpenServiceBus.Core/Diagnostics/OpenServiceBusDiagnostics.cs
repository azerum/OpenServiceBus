using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace OpenServiceBus.Core.Diagnostics;

/// <summary>
/// Single source of truth for OpenServiceBus telemetry. All instrumentation hangs off
/// one <see cref="ActivitySource"/> and one <see cref="Meter"/> so consumers point their OTel
/// pipeline at exactly one name each:
///
/// <code>
/// services.AddOpenTelemetry()
///     .WithTracing(b =&gt; b.AddSource(OpenServiceBusDiagnostics.SourceName))
///     .WithMetrics(b =&gt; b.AddMeter(OpenServiceBusDiagnostics.SourceName));
/// </code>
///
/// Span names and tag keys follow the OpenTelemetry messaging semantic conventions where they
/// apply (<c>messaging.system</c>, <c>messaging.destination.name</c>, <c>messaging.message.id</c>),
/// with Service Bus-specific extensions in the <c>messaging.servicebus.*</c> namespace.
/// </summary>
public static class OpenServiceBusDiagnostics
{
    /// <summary>Name registered for both the <see cref="ActivitySource"/> and the <see cref="Meter"/>.</summary>
    public const string SourceName = "OpenServiceBus";

    /// <summary>Version stamp on the ActivitySource and Meter.</summary>
    public const string Version = "1.0.0";

    public static readonly ActivitySource ActivitySource = new(SourceName, Version);

    public static readonly Meter Meter = new(SourceName, Version);

    // ── Counters ──────────────────────────────────────────────────────

    public static readonly Counter<long> MessagesSent = Meter.CreateCounter<long>(
        "osb.messages.sent",
        unit: "{message}",
        description: "Messages accepted by the broker from senders.");

    public static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
        "osb.messages.received",
        unit: "{message}",
        description: "Messages handed to consumers under peek-lock.");

    public static readonly Counter<long> MessagesDispositioned = Meter.CreateCounter<long>(
        "osb.messages.dispositioned",
        unit: "{message}",
        description: "Disposition outcomes (complete, abandon, defer, dead-letter).");

    public static readonly Counter<long> MessagesDeadLettered = Meter.CreateCounter<long>(
        "osb.messages.deadlettered",
        unit: "{message}",
        description: "Messages routed to a dead-letter queue (explicit or via max-delivery / TTL).");

    public static readonly Counter<long> MessagesExpired = Meter.CreateCounter<long>(
        "osb.messages.expired",
        unit: "{message}",
        description: "Messages dropped or DLQ'd by the TTL sweeper.");

    // ── Histograms ────────────────────────────────────────────────────

    public static readonly Histogram<int> DeliveryCountAtDisposition = Meter.CreateHistogram<int>(
        "osb.message.delivery_count",
        unit: "{attempt}",
        description: "Delivery attempt count observed at disposition time. Useful for spotting redelivery storms.");

    // ── Tag keys ──────────────────────────────────────────────────────

    /// <summary>The standardized value for <see cref="TagSystem"/>.</summary>
    public const string SystemValue = "servicebus";

    public const string TagSystem = "messaging.system";
    public const string TagDestination = "messaging.destination.name";
    public const string TagMessageId = "messaging.message.id";
    public const string TagConversationId = "messaging.message.conversation_id";
    public const string TagOperation = "messaging.operation.type";
    public const string TagDeliveryCount = "messaging.servicebus.delivery_count";
    public const string TagSessionId = "messaging.servicebus.session_id";
    public const string TagDisposition = "messaging.servicebus.disposition_status";
    public const string TagDeadLetterReason = "messaging.servicebus.dead_letter_reason";
    public const string TagDeadLetterSource = "messaging.servicebus.dead_letter_source";

    // ── Span names ────────────────────────────────────────────────────

    public const string SpanSend = "osb.send";
    public const string SpanReceive = "osb.receive";
    public const string SpanSettle = "osb.settle";
}
