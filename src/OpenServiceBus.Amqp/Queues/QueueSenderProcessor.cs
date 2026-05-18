using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
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
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<QueueSenderProcessor> _logger;

    public QueueSenderProcessor(
        string queueName,
        QueueDescriptor descriptor,
        IMessageStore store,
        TimeProvider timeProvider,
        ILogger<QueueSenderProcessor> logger)
    {
        _queueName = queueName;
        _descriptor = descriptor;
        _store = store;
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
            _ = EnqueueAndCompleteAsync(messageContext, encoded, expiresAt, scheduledFor, sessionId);
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

                await _store.EnqueueAsync(_queueName, innerBytes, expiresAt, scheduledFor, sessionId).ConfigureAwait(false);
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

    private async Task EnqueueAndCompleteAsync(MessageContext context, byte[] encoded, DateTimeOffset? expiresAt, DateTimeOffset? scheduledFor, string? sessionId)
    {
        try
        {
            var stored = await _store.EnqueueAsync(_queueName, encoded, expiresAt, scheduledFor, sessionId).ConfigureAwait(false);
            _logger.LogDebug("Enqueued message #{Seq} ({Bytes} bytes) to {Queue} (expiresAt={Expires}, scheduledFor={Scheduled})",
                stored.SequenceNumber, encoded.Length, _queueName,
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
