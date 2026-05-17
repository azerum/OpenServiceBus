using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.Amqp;

/// <summary>
/// Handles incoming AMQP sender links from clients sending messages into a queue.
/// One processor instance per queue (the AMQPNetLite <c>ContainerHost</c> requires
/// per-address registration; M2.5 will replace this with a single dynamic <c>ILinkProcessor</c>).
/// </summary>
public sealed class QueueSenderProcessor : IMessageProcessor
{
    private readonly string _queueName;
    private readonly IMessageStore _store;
    private readonly ILogger<QueueSenderProcessor> _logger;

    public QueueSenderProcessor(string queueName, IMessageStore store, ILogger<QueueSenderProcessor> logger)
    {
        _queueName = queueName;
        _store = store;
        _logger = logger;
    }

    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        try
        {
            var encoded = CopyEncoded(messageContext.Message);
            // Fire-and-forget the enqueue; the in-memory store completes synchronously
            // but the IMessageStore contract is async so we don't block the listener thread.
            _ = EnqueueAndCompleteAsync(messageContext, encoded);
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

    private async Task EnqueueAndCompleteAsync(MessageContext context, byte[] encoded)
    {
        try
        {
            var stored = await _store.EnqueueAsync(_queueName, encoded).ConfigureAwait(false);
            _logger.LogDebug("Enqueued message #{Seq} ({Bytes} bytes) to {Queue}",
                stored.SequenceNumber, encoded.Length, _queueName);
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

    private static byte[] CopyEncoded(Message message)
    {
        var buffer = message.Encode();
        var copy = new byte[buffer.Length];
        Array.Copy(buffer.Buffer, buffer.Offset, copy, 0, buffer.Length);
        return copy;
    }
}
