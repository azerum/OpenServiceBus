using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Filters;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Routing;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.Queues;

/// <summary>
/// Handles incoming AMQP sender links from clients sending messages into a queue.
/// One processor instance per attach (created by <see cref="EntityLinkProcessor"/>).
/// </summary>
public sealed class QueueSenderProcessor : IMessageProcessor
{
    private static readonly Symbol ScheduledEnqueueTimeSymbol = new("x-opt-scheduled-enqueue-time");

    /// <summary>
    /// Microsoft.Azure.Amqp's batched-message-format marker. When this is set on the outer
    /// transfer, the body is one or more <c>Data</c> sections (a <see cref="DataList"/> in
    /// AMQPNetLite terms) each holding a fully-encoded inner message. Real Service Bus splits
    /// these on the broker side so each inner message gets its own sequence number — we do the same.
    /// </summary>
    private const uint AmqpBatchedMessageFormat = 0x80013700u;

    private readonly string _queueName;
    private readonly QueueDescriptor _descriptor;
    private readonly IMessageStore _store;
    private readonly IMessageRouter _router;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<QueueSenderProcessor> _logger;

    public QueueSenderProcessor(
        string queueName,
        QueueDescriptor descriptor,
        IMessageStore store,
        IMessageRouter router,
        TimeProvider timeProvider,
        ILogger<QueueSenderProcessor> logger)
    {
        _queueName = queueName;
        _descriptor = descriptor;
        _store = store;
        _router = router;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        try
        {
            var msg = messageContext.Message;

            // Batched envelope from SendMessagesAsync / ServiceBusMessageBatch — split into N individual
            // enqueues so each inner message gets its own sequence number and lifecycle.
            if (msg.Format == AmqpBatchedMessageFormat && msg.BodySection is DataList dataList)
            {
                _ = EnqueueBatchAsync(messageContext, dataList);
                return;
            }

            var encoded = CopyEncoded(msg);
            var expiresAt = ComputeExpiresAt(msg);
            // M7: clients can schedule via SendMessageAsync (this path) by stamping the
            // x-opt-scheduled-enqueue-time annotation; or via the dedicated ScheduleMessageAsync
            // which goes through $management. Both end up at IMessageStore.EnqueueAsync(..., scheduledEnqueueTime).
            var scheduledFor = ReadScheduledEnqueueTime(msg);
            // M14: AMQP properties.group-id IS Service Bus's SessionId — but only route by it
            // when the queue is session-enabled; on a plain queue the GroupId is preserved as
            // metadata via the encoded bytes and the message stays on the regular delivery path.
            var sessionId = _descriptor.RequiresSession ? msg.Properties?.GroupId : null;
            // M15: when dedup is required, pass messageId + window to the store so repeats are dropped.
            var messageId = _descriptor.RequiresDuplicateDetection ? msg.Properties?.MessageId?.ToString() : null;
            var dedupWindow = _descriptor.RequiresDuplicateDetection
                ? _descriptor.DuplicateDetectionHistoryTimeWindow ?? TimeSpan.FromMinutes(10)
                : (TimeSpan?)null;
            // M16: forward target may be a topic — build a filter context unconditionally so
            // the router can fan-out if the chain hits one. Cheap when not forwarded.
            var filterContext = BuildFilterContext(msg, _timeProvider.GetUtcNow());
            _ = EnqueueAndCompleteAsync(messageContext, encoded, expiresAt, scheduledFor, sessionId, messageId, dedupWindow, filterContext);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to encode incoming message on queue {Queue}", _queueName);
            messageContext.Complete(new Error(new Symbol(ErrorCode.InternalError))
            {
                Description = "Failed to accept message",
            });
        }
    }

    private async Task EnqueueBatchAsync(MessageContext context, DataList dataList)
    {
        try
        {
            for (var i = 0; i < dataList.Count; i++)
            {
                var innerBinary = dataList[i].Binary;
                // Copy: AMQPNetLite may pool the underlying buffer; we need an independent byte[].
                var innerBytes = new byte[innerBinary.Length];
                Array.Copy(innerBinary, innerBytes, innerBinary.Length);

                // Each inner message has its own header (TTL) and annotations (scheduled-enqueue-time).
                var inner = DecodeMessage(innerBytes);
                var expiresAt = ComputeExpiresAt(inner);
                var scheduledFor = ReadScheduledEnqueueTime(inner);
                var sessionId = _descriptor.RequiresSession ? inner.Properties?.GroupId : null;
                var messageId = _descriptor.RequiresDuplicateDetection ? inner.Properties?.MessageId?.ToString() : null;
                var dedupWindow = _descriptor.RequiresDuplicateDetection
                    ? _descriptor.DuplicateDetectionHistoryTimeWindow ?? TimeSpan.FromMinutes(10)
                    : (TimeSpan?)null;
                var filterContext = BuildFilterContext(inner, _timeProvider.GetUtcNow());

                await RouteOrEnqueueAsync(innerBytes, expiresAt, scheduledFor, sessionId, messageId, dedupWindow, filterContext).ConfigureAwait(false);
            }
            _logger.LogDebug("Split batched envelope into {Count} message(s) on {Queue}", dataList.Count, _queueName);
            context.Complete();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to enqueue batched messages on queue {Queue}", _queueName);
            context.Complete(new Error(new Symbol(ErrorCode.InternalError)) { Description = ex.Message });
        }
    }

    private static Message DecodeMessage(byte[] bytes)
    {
        var buf = new ByteBuffer(bytes, 0, bytes.Length, bytes.Length);
        return Message.Decode(buf);
    }

    private async Task EnqueueAndCompleteAsync(MessageContext context, byte[] encoded, DateTimeOffset? expiresAt, DateTimeOffset? scheduledFor, string? sessionId, string? messageId, TimeSpan? duplicateDetectionWindow, MessageFilterContext filterContext)
    {
        try
        {
            await RouteOrEnqueueAsync(encoded, expiresAt, scheduledFor, sessionId, messageId, duplicateDetectionWindow, filterContext).ConfigureAwait(false);
            _logger.LogDebug("Accepted send on {Queue} ({Bytes} bytes, expiresAt={Expires}, scheduledFor={Scheduled})",
                _queueName, encoded.Length,
                expiresAt?.ToString("O") ?? "never",
                scheduledFor?.ToString("O") ?? "immediate");
            context.Complete();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to enqueue message on queue {Queue}", _queueName);
            context.Complete(new Error(new Symbol(ErrorCode.InternalError))
            {
                Description = ex.Message,
            });
        }
    }

    /// <summary>
    /// Hook for auto-forwarding (M16). When the queue has <c>ForwardTo</c>, the message is
    /// routed to the configured destination via <see cref="IMessageRouter"/> — which transparently
    /// handles topic fan-out and chained queues up to the 4-hop cap. Otherwise it's a direct
    /// store enqueue, identical to pre-M16 behavior.
    /// </summary>
    private Task RouteOrEnqueueAsync(byte[] encoded, DateTimeOffset? expiresAt, DateTimeOffset? scheduledFor, string? sessionId, string? messageId, TimeSpan? dedupWindow, MessageFilterContext filterContext)
    {
        if (!string.IsNullOrEmpty(_descriptor.ForwardTo))
        {
            return _router.RouteAsync(
                _descriptor.ForwardTo, encoded, expiresAt, scheduledFor,
                sessionId, messageId, dedupWindow, filterContext);
        }

        return _store.EnqueueAsync(_queueName, encoded, expiresAt, scheduledFor, sessionId, messageId, dedupWindow);
    }

    private static MessageFilterContext BuildFilterContext(Message msg, DateTimeOffset enqueuedAt)
    {
        var appProps = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (msg.ApplicationProperties is not null)
        {
            foreach (var key in msg.ApplicationProperties.Map.Keys)
            {
                if (key is null) continue;
                appProps[key.ToString()!] = msg.ApplicationProperties.Map[key];
            }
        }
        return new MessageFilterContext
        {
            MessageId = msg.Properties?.MessageId,
            CorrelationId = msg.Properties?.CorrelationId,
            Subject = msg.Properties?.Subject,
            To = msg.Properties?.To,
            ReplyTo = msg.Properties?.ReplyTo,
            ReplyToSessionId = msg.Properties?.ReplyToGroupId,
            SessionId = msg.Properties?.GroupId,
            ContentType = msg.Properties?.ContentType,
            EnqueuedTimeUtc = enqueuedAt,
            ApplicationProperties = appProps,
        };
    }

    private static DateTimeOffset? ReadScheduledEnqueueTime(Message msg)
    {
        if (msg.MessageAnnotations is null) return null;
        if (!msg.MessageAnnotations.Map.TryGetValue(ScheduledEnqueueTimeSymbol, out var value)) return null;
        return value switch
        {
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, dt.Kind == DateTimeKind.Unspecified ? DateTimeKind.Utc : dt.Kind).ToUniversalTime()),
            DateTimeOffset dto => dto,
            _ => null,
        };
    }

    /// <summary>
    /// Compute the effective TTL deadline: <c>now + min(perMessageTtl, queueDefaultTtl)</c>.
    /// Returns null when neither side specifies a TTL.
    /// </summary>
    private DateTimeOffset? ComputeExpiresAt(Message msg)
    {
        TimeSpan? perMessage = msg.Header?.Ttl is uint ms and > 0
            ? TimeSpan.FromMilliseconds(ms)
            : null;
        var queueDefault = _descriptor.DefaultMessageTimeToLive;

        var effective = MinTtl(perMessage, queueDefault);
        return effective is null ? null : _timeProvider.GetUtcNow() + effective.Value;
    }

    private static TimeSpan? MinTtl(TimeSpan? a, TimeSpan? b)
    {
        if (a is null) return b;
        if (b is null) return a;
        return a < b ? a : b;
    }

    private static byte[] CopyEncoded(Message message)
    {
        var buffer = message.Encode();
        var copy = new byte[buffer.Length];
        Array.Copy(buffer.Buffer, buffer.Offset, copy, 0, buffer.Length);
        return copy;
    }
}
