using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Abstractions;
using OpenServiceBus.Broker;

namespace OpenServiceBus.Amqp;

/// <summary>
/// Implements the broker-side of a receive link: pulls the next available message from
/// the store under peek-lock, stamps Service Bus system properties (M4), translates client
/// dispositions back into store operations, and moves messages to the DLQ when an explicit
/// Rejected disposition arrives or when the delivery-count budget is exhausted (M5).
/// One instance per (queue, sub-resource) — multiple client receivers can share it safely.
/// </summary>
public sealed class QueueReceiverSource : IMessageSource
{
    // Annotation symbols match Azure.Messaging.ServiceBus's AmqpMessageConstants exactly.
    private static readonly Symbol EnqueuedTimeUtcSymbol = new("x-opt-enqueued-time");
    private static readonly Symbol SequenceNumberSymbol = new("x-opt-sequence-number");
    private static readonly Symbol LockedUntilSymbol = new("x-opt-locked-until");
    private static readonly Symbol DeadLetterSourceSymbol = new("x-opt-deadletter-source");

    // Dead-letter reason/description live on the message as application-properties so consumers
    // (incl. the DLQ receiver) can read them via ServiceBusReceivedMessage.DeadLetterReason etc.
    private const string DeadLetterReasonHeader = "DeadLetterReason";
    private const string DeadLetterErrorDescriptionHeader = "DeadLetterErrorDescription";

    // Same names appear as Symbol keys on Rejected.Error.Info (the SDK's preferred shape).
    private static readonly Symbol DeadLetterReasonSymbol = new(DeadLetterReasonHeader);
    private static readonly Symbol DeadLetterErrorDescriptionSymbol = new(DeadLetterErrorDescriptionHeader);

    // Default reason when the SDK calls DeadLetterMessageAsync(msg) with no reason supplied.
    private const string MaxDeliveryReason = "MaxDeliveryCountExceeded";

    private readonly string _entityName;
    private readonly QueueDescriptor _descriptor;
    private readonly IMessageStore _store;
    private readonly ILogger<QueueReceiverSource> _logger;
    private readonly bool _isDlq;

    public QueueReceiverSource(
        string entityName,
        QueueDescriptor descriptor,
        IMessageStore store,
        ILogger<QueueReceiverSource> logger)
    {
        _entityName = entityName;
        _descriptor = descriptor;
        _store = store;
        _logger = logger;
        _isDlq = QueueManager.IsDeadLetterQueue(entityName);
    }

    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        while (true)
        {
            var locked = await _store.TryDequeueAsync(_entityName, _descriptor.LockDuration).ConfigureAwait(false);
            if (locked is null) return null!;

            // If the wire delivery-count has reached MaxDeliveryCount, skip delivery and move to DLQ.
            // The DLQ itself never auto-dead-letters (its MaxDeliveryCount is int.MaxValue and _isDlq guards anyway).
            if (!_isDlq && locked.Message.DeliveryCount >= _descriptor.MaxDeliveryCount)
            {
                await DeadLetterAsync(
                    locked.LockToken,
                    MaxDeliveryReason,
                    $"Message could not be consumed within {_descriptor.MaxDeliveryCount} delivery attempts.")
                    .ConfigureAwait(false);
                continue;
            }

            var amqp = DecodeMessage(locked.Message.EncodedMessage);
            StampSystemProperties(amqp, locked);

            var ctx = new ReceiveContext(link, amqp) { UserToken = locked.LockToken };
            return ctx;
        }
    }

    public void DisposeMessage(ReceiveContext receiveContext, DispositionContext dispositionContext)
    {
        if (receiveContext.UserToken is not Guid lockToken)
        {
            dispositionContext.Complete();
            return;
        }

        try
        {
            switch (dispositionContext.DeliveryState)
            {
                case Accepted:
                    _store.TryCompleteAsync(_entityName, lockToken).GetAwaiter().GetResult();
                    break;

                case Released:
                case Modified:
                    _store.TryAbandonAsync(_entityName, lockToken).GetAwaiter().GetResult();
                    break;

                case Rejected rejected:
                    var (reason, description) = ExtractDeadLetterInfo(rejected);
                    DeadLetterAsync(lockToken, reason, description).GetAwaiter().GetResult();
                    break;

                default:
                    _logger.LogWarning(
                        "Unexpected delivery state {State} for lock {Lock} on {Entity}",
                        dispositionContext.DeliveryState?.GetType().Name ?? "<null>", lockToken, _entityName);
                    _store.TryAbandonAsync(_entityName, lockToken).GetAwaiter().GetResult();
                    break;
            }
        }
        finally
        {
            dispositionContext.Complete();
        }
    }

    private async Task DeadLetterAsync(Guid lockToken, string? reason, string? description, CancellationToken cancellationToken = default)
    {
        if (_isDlq)
        {
            // No DLQ for the DLQ - just release the lock without enqueue elsewhere.
            await _store.TryAbandonAsync(_entityName, lockToken, cancellationToken).ConfigureAwait(false);
            return;
        }

        var removed = await _store.TryRemoveLockedAsync(_entityName, lockToken, cancellationToken).ConfigureAwait(false);
        if (removed is null) return;

        var amqp = DecodeMessage(removed.EncodedMessage);
        amqp.ApplicationProperties ??= new ApplicationProperties();
        if (reason is not null) amqp.ApplicationProperties[DeadLetterReasonHeader] = reason;
        if (description is not null) amqp.ApplicationProperties[DeadLetterErrorDescriptionHeader] = description;

        amqp.MessageAnnotations ??= new MessageAnnotations();
        amqp.MessageAnnotations.Map[DeadLetterSourceSymbol] = _entityName;

        var dlqBytes = EncodeMessage(amqp);
        var dlqName = _entityName + QueueManager.DeadLetterSuffix;
        await _store.EnqueueAsync(dlqName, dlqBytes, cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Dead-lettered seq#{Seq} from {Entity} to {Dlq} (reason={Reason})",
            removed.SequenceNumber, _entityName, dlqName, reason ?? "(unspecified)");
    }

    private static (string? Reason, string? Description) ExtractDeadLetterInfo(Rejected rejected)
    {
        if (rejected.Error?.Info is null) return (null, null);
        rejected.Error.Info.TryGetValue(DeadLetterReasonSymbol, out var reason);
        rejected.Error.Info.TryGetValue(DeadLetterErrorDescriptionSymbol, out var description);
        return (reason as string, description as string);
    }

    /// <summary>
    /// Stamp broker-authoritative fields onto the outgoing message:
    ///   <list type="bullet">
    ///     <item><c>header.delivery-count</c> — incremented per redelivery, read by the SDK as <c>ServiceBusReceivedMessage.DeliveryCount</c>.</item>
    ///     <item><c>x-opt-sequence-number</c> — long, the broker-assigned monotonic sequence.</item>
    ///     <item><c>x-opt-enqueued-time</c> — UTC DateTime of when the broker accepted the message.</item>
    ///     <item><c>x-opt-locked-until</c> — UTC DateTime of when this specific lock expires.</item>
    ///   </list>
    /// Existing client-set fields on Header/MessageAnnotations are preserved.
    /// </summary>
    private static void StampSystemProperties(Message amqp, LockedMessage locked)
    {
        amqp.Header ??= new Header();
        amqp.Header.DeliveryCount = (uint)locked.Message.DeliveryCount;

        amqp.MessageAnnotations ??= new MessageAnnotations();
        amqp.MessageAnnotations.Map[SequenceNumberSymbol] = locked.Message.SequenceNumber;
        amqp.MessageAnnotations.Map[EnqueuedTimeUtcSymbol] = locked.Message.EnqueuedAt.UtcDateTime;
        amqp.MessageAnnotations.Map[LockedUntilSymbol] = locked.LockedUntil.UtcDateTime;
    }

    private static Message DecodeMessage(byte[] encoded)
    {
        var buffer = new ByteBuffer(encoded, 0, encoded.Length, encoded.Length);
        return Message.Decode(buffer);
    }

    private static byte[] EncodeMessage(Message msg)
    {
        var buffer = msg.Encode();
        var copy = new byte[buffer.Length];
        Array.Copy(buffer.Buffer, buffer.Offset, copy, 0, buffer.Length);
        return copy;
    }
}
