using OpenServiceBus.Amqp.DeadLettering;

using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Routing;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.Queues;

/// <summary>
/// Implements the broker-side of a receive link: pulls the next available message from
/// the store under peek-lock, stamps Service Bus system properties (M4), translates client
/// dispositions back into store operations, moves messages to the DLQ when an explicit
/// Rejected disposition arrives or when the delivery-count budget is exhausted (M5), and
/// dead-letters / drops expired messages on dequeue (M6).
/// One instance per (queue, sub-resource) — multiple client receivers can share it safely.
/// </summary>
public sealed class QueueReceiverSource : IMessageSource
{
    // Annotation symbols match Azure.Messaging.ServiceBus's AmqpMessageConstants exactly.
    private static readonly Symbol EnqueuedTimeUtcSymbol = new("x-opt-enqueued-time");
    private static readonly Symbol SequenceNumberSymbol = new("x-opt-sequence-number");
    private static readonly Symbol LockedUntilSymbol = new("x-opt-locked-until");

    // Same names appear as Symbol keys on Rejected.Error.Info (the SDK's preferred shape).
    private static readonly Symbol DeadLetterReasonSymbol = new(DeadLetterEncoder.DeadLetterReasonHeader);
    private static readonly Symbol DeadLetterErrorDescriptionSymbol = new(DeadLetterEncoder.DeadLetterErrorDescriptionHeader);

    // Default reason when the SDK calls DeadLetterMessageAsync(msg) with no reason supplied.
    private const string MaxDeliveryReason = "MaxDeliveryCountExceeded";

    // Standard Service Bus reasons for TTL expiration.
    public const string TtlExpiredReason = "TTLExpiredException";
    public const string TtlExpiredDescription = "The message expired and was moved to the dead-letter queue.";

    private readonly string _entityName;
    private readonly QueueDescriptor _descriptor;
    private readonly IMessageStore _store;
    private readonly IMessageRouter _router;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<QueueReceiverSource> _logger;
    private readonly bool _isDlq;

    public QueueReceiverSource(
        string entityName,
        QueueDescriptor descriptor,
        IMessageStore store,
        IMessageRouter router,
        TimeProvider timeProvider,
        ILogger<QueueReceiverSource> logger)
    {
        _entityName = entityName;
        _descriptor = descriptor;
        _store = store;
        _router = router;
        _timeProvider = timeProvider;
        _logger = logger;
        _isDlq = EntityNames.IsDeadLetterQueue(entityName);
    }

    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        while (true)
        {
            // Pass the receiver link name so the lock is scoped to this link — only this
            // link's $management session can renew it (matches Service Bus's lock-link affinity).
            var locked = await _store.TryDequeueAsync(_entityName, _descriptor.LockDuration, link.Name).ConfigureAwait(false);
            if (locked is null) return null!;

            // M6: messages that crossed their TTL deadline while waiting in the queue get
            // dropped (or moved to DLQ when DeadLetteringOnMessageExpiration is set on the queue).
            if (locked.Message.IsExpired(_timeProvider.GetUtcNow()))
            {
                await HandleExpiredOnDequeueAsync(locked.LockToken).ConfigureAwait(false);
                continue;
            }

            // M5: if the wire delivery-count has reached MaxDeliveryCount, skip delivery and move to DLQ.
            // The DLQ itself never auto-dead-letters (MaxDeliveryCount = int.MaxValue and _isDlq guards anyway).
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

            // ReceiveAndDelete: the client opened the link with snd-settle-mode=settled, meaning
            // we send the message as settled and NO disposition will arrive. The peek-lock we just
            // took would otherwise expire → ghost redelivery. Settle (delete) it now so the message
            // is gone the moment we hand it to the framework. This is at-most-once semantics — a
            // wire-level send failure loses the message, matching real Service Bus.
            if (link.SettleOnSend)
            {
                await _store.TryCompleteAsync(_entityName, locked.LockToken).ConfigureAwait(false);
            }

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

                // M8: Modified with UndeliverableHere=true is the SDK's DeferAsync wire signal —
                // park the message in Deferred state instead of returning it to the active pool.
                case Modified modified when modified.UndeliverableHere:
                    _store.TryDeferAsync(_entityName, lockToken).GetAwaiter().GetResult();
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

    /// <summary>
    /// A message was dequeued under peek-lock but its TTL has already passed. Either dead-letter it
    /// (when the queue is configured to) or drop it. Either way the message is gone from the source queue.
    /// </summary>
    private async Task HandleExpiredOnDequeueAsync(Guid lockToken, CancellationToken cancellationToken = default)
    {
        if (!_isDlq && _descriptor.DeadLetteringOnMessageExpiration)
        {
            await DeadLetterAsync(lockToken, TtlExpiredReason, TtlExpiredDescription, cancellationToken).ConfigureAwait(false);
            return;
        }

        // Drop: just remove the locked message; no DLQ enqueue.
        await _store.TryRemoveLockedAsync(_entityName, lockToken, cancellationToken).ConfigureAwait(false);
        _logger.LogDebug("TTL-dropped expired message from {Entity}", _entityName);
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

        var dlqBytes = DeadLetterEncoder.AppendDeadLetterHeaders(removed.EncodedMessage, _entityName, reason, description);
        // M16: if the entity is configured to forward dead-lettered messages elsewhere, route there;
        // otherwise the message lands on the local <entity>/$DeadLetterQueue as before.
        var dlqTarget = string.IsNullOrEmpty(_descriptor.ForwardDeadLetteredMessagesTo)
            ? _entityName + EntityNames.DeadLetterSuffix
            : _descriptor.ForwardDeadLetteredMessagesTo!;
        await _router.RouteAsync(dlqTarget, dlqBytes, expiresAt: null, cancellationToken: cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Dead-lettered seq#{Seq} from {Entity} to {Dlq} (reason={Reason})",
            removed.SequenceNumber, _entityName, dlqTarget, reason ?? "(unspecified)");
    }

    private static (string? Reason, string? Description) ExtractDeadLetterInfo(Rejected rejected)
    {
        if (rejected.Error?.Info is null) return (null, null);
        rejected.Error.Info.TryGetValue(DeadLetterReasonSymbol, out var reason);
        rejected.Error.Info.TryGetValue(DeadLetterErrorDescriptionSymbol, out var description);
        return (reason as string, description as string);
    }

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
}
