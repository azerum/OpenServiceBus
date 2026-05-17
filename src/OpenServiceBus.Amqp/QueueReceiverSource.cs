using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.Amqp;

/// <summary>
/// Implements the broker-side of a receive link: pulls the next available message from
/// the store under peek-lock, stamps Service Bus system properties (M4), and translates
/// client dispositions back into store operations.
/// One instance per (queue, sub-resource) — multiple client receivers can share it safely.
/// </summary>
public sealed class QueueReceiverSource : IMessageSource
{
    // Annotation symbols match Azure.Messaging.ServiceBus's AmqpMessageConstants exactly.
    private static readonly Symbol EnqueuedTimeUtcSymbol = new("x-opt-enqueued-time");
    private static readonly Symbol SequenceNumberSymbol = new("x-opt-sequence-number");
    private static readonly Symbol LockedUntilSymbol = new("x-opt-locked-until");

    private readonly string _entityName;
    private readonly QueueDescriptor _descriptor;
    private readonly IMessageStore _store;
    private readonly ILogger<QueueReceiverSource> _logger;

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
    }

    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        // The AMQPNetLite contract: this method blocks until a message is available,
        // and the listener drives it once per client-granted credit.
        var locked = await _store.TryDequeueAsync(_entityName, _descriptor.LockDuration).ConfigureAwait(false);
        if (locked is null)
        {
            // Cancellation / channel closure -> propagate up so the listener stops asking.
            return null!;
        }

        var amqp = DecodeMessage(locked.Message.EncodedMessage);
        StampSystemProperties(amqp, locked);

        // For M3 the AMQP disposition flow (Accept/Release/Modify) settles via delivery-id,
        // not lock-token, so the auto-generated delivery-tag is fine and the framework maps
        // dispositions back to us via ReceiveContext.UserToken. M5 will revisit this when we
        // wire $management RenewLock, which requires lock-token == delivery-tag bytes.
        var ctx = new ReceiveContext(link, amqp) { UserToken = locked.LockToken };
        return ctx;
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

                case Rejected:
                    // For M3 treat as abandon (true reject + DLQ lands in M5).
                    _store.TryAbandonAsync(_entityName, lockToken).GetAwaiter().GetResult();
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
}
