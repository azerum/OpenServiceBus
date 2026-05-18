using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Amqp.DeadLettering;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Routing;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.Queues;

/// <summary>
/// Session-aware variant of <see cref="QueueReceiverSource"/>. One instance per receiver link,
/// scoped to a specific <see cref="SessionId"/>. Delivers only messages belonging to that
/// session and assumes the link's session lock has already been acquired by the
/// <c>EntityLinkProcessor</c> at attach time (so the SDK's <c>AcceptSessionAsync</c> /
/// <c>AcceptNextSessionAsync</c> calls resolve before any first delivery).
/// </summary>
public sealed class SessionReceiverSource : IMessageSource
{
    private static readonly Symbol EnqueuedTimeUtcSymbol = new("x-opt-enqueued-time");
    private static readonly Symbol SequenceNumberSymbol = new("x-opt-sequence-number");
    private static readonly Symbol LockedUntilSymbol = new("x-opt-locked-until");
    private static readonly Symbol DeadLetterReasonSymbol = new(DeadLetterEncoder.DeadLetterReasonHeader);
    private static readonly Symbol DeadLetterErrorDescriptionSymbol = new(DeadLetterEncoder.DeadLetterErrorDescriptionHeader);

    public const string TtlExpiredReason = "TTLExpiredException";
    public const string TtlExpiredDescription = "The message expired and was moved to the dead-letter queue.";
    private const string MaxDeliveryReason = "MaxDeliveryCountExceeded";

    private readonly string _entityName;
    private readonly QueueDescriptor _descriptor;
    private readonly IMessageStore _store;
    private readonly IMessageRouter _router;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<SessionReceiverSource> _logger;
    private readonly bool _isDlq;

    public string SessionId { get; }
    public string? LinkName { get; }

    public SessionReceiverSource(
        string entityName,
        QueueDescriptor descriptor,
        IMessageStore store,
        IMessageRouter router,
        TimeProvider timeProvider,
        ILogger<SessionReceiverSource> logger,
        string sessionId,
        string? linkName)
    {
        _entityName = entityName;
        _descriptor = descriptor;
        _store = store;
        _router = router;
        _timeProvider = timeProvider;
        _logger = logger;
        _isDlq = EntityNames.IsDeadLetterQueue(entityName);
        SessionId = sessionId;
        LinkName = linkName;
    }

    public async Task<ReceiveContext> GetMessageAsync(ListenerLink link)
    {
        while (true)
        {
            // The SDK's session receiver calls DrainAsync after every receive (see decompiled
            // AmqpReceiver.ReceiveMessagesAsyncInternal line 2762). For drain to complete,
            // GetMessageAsync MUST return null so SourceLinkEndpoint can call link.CompleteDrain().
            // Poll the channel on a short timeout so we periodically observe link.IsDraining /
            // IsDetaching and yield null when either is set.
            LockedMessage? locked = null;
            while (locked is null)
            {
                if (link.IsDraining) return null!;
                using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
                try
                {
                    locked = await _store.TryDequeueFromSessionAsync(
                        _entityName, SessionId, _descriptor.LockDuration, link.Name, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Poll timeout — loop and re-check drain state.
                }
            }

            if (locked.Message.IsExpired(_timeProvider.GetUtcNow()))
            {
                await HandleExpiredOnDequeueAsync(locked.LockToken).ConfigureAwait(false);
                continue;
            }

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

            if (link.SettleOnSend)
            {
                await _store.TryCompleteAsync(_entityName, locked.LockToken).ConfigureAwait(false);
            }

            return new ReceiveContext(link, amqp) { UserToken = locked.LockToken };
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
                        "Unexpected delivery state {State} for lock {Lock} on session {Session}",
                        dispositionContext.DeliveryState?.GetType().Name ?? "<null>", lockToken, SessionId);
                    _store.TryAbandonAsync(_entityName, lockToken).GetAwaiter().GetResult();
                    break;
            }
        }
        finally
        {
            dispositionContext.Complete();
        }
    }

    private async Task HandleExpiredOnDequeueAsync(Guid lockToken, CancellationToken cancellationToken = default)
    {
        if (!_isDlq && _descriptor.DeadLetteringOnMessageExpiration)
        {
            await DeadLetterAsync(lockToken, TtlExpiredReason, TtlExpiredDescription, cancellationToken).ConfigureAwait(false);
            return;
        }
        await _store.TryRemoveLockedAsync(_entityName, lockToken, cancellationToken).ConfigureAwait(false);
    }

    private async Task DeadLetterAsync(Guid lockToken, string? reason, string? description, CancellationToken cancellationToken = default)
    {
        if (_isDlq)
        {
            await _store.TryAbandonAsync(_entityName, lockToken, cancellationToken).ConfigureAwait(false);
            return;
        }
        var removed = await _store.TryRemoveLockedAsync(_entityName, lockToken, cancellationToken).ConfigureAwait(false);
        if (removed is null) return;
        var dlqBytes = DeadLetterEncoder.AppendDeadLetterHeaders(removed.EncodedMessage, _entityName, reason, description);
        // M16: honor ForwardDeadLetteredMessagesTo, falling back to the local DLQ.
        var dlqTarget = string.IsNullOrEmpty(_descriptor.ForwardDeadLetteredMessagesTo)
            ? _entityName + EntityNames.DeadLetterSuffix
            : _descriptor.ForwardDeadLetteredMessagesTo!;
        await _router.RouteAsync(dlqTarget, dlqBytes, expiresAt: null, cancellationToken: cancellationToken).ConfigureAwait(false);
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
