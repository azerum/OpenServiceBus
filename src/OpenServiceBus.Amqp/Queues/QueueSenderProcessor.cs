using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.Amqp;

/// <summary>
/// Handles incoming AMQP sender links from clients sending messages into a queue.
/// One processor instance per attach (created by <see cref="EntityLinkProcessor"/>).
/// </summary>
public sealed class QueueSenderProcessor : IMessageProcessor
{
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
            var encoded = CopyEncoded(messageContext.Message);
            var expiresAt = ComputeExpiresAt(messageContext.Message);
            // Fire-and-forget the enqueue; the in-memory store completes synchronously
            // but the IMessageStore contract is async so we don't block the listener thread.
            _ = EnqueueAndCompleteAsync(messageContext, encoded, expiresAt);
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

    private async Task EnqueueAndCompleteAsync(MessageContext context, byte[] encoded, DateTimeOffset? expiresAt)
    {
        try
        {
            var stored = await _store.EnqueueAsync(_queueName, encoded, expiresAt).ConfigureAwait(false);
            _logger.LogDebug("Enqueued message #{Seq} ({Bytes} bytes) to {Queue} (expiresAt={Expires})",
                stored.SequenceNumber, encoded.Length, _queueName, expiresAt?.ToString("O") ?? "never");
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
