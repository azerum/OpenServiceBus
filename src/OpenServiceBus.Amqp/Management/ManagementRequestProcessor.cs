using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.Management;

/// <summary>
/// Handles AMQP <c>$management</c> request/response operations for a single queue entity.
/// Registered per entity by <see cref="AmqpListenerHost"/> on <c>QueueCreated</c>.
///
/// Operations implemented:
/// <list type="bullet">
///   <item><c>com.microsoft:renew-lock</c> (M5) — extends one or more peek-lock deadlines.</item>
///   <item><c>com.microsoft:schedule-message</c> (M7) — enqueue messages for future delivery.</item>
///   <item><c>com.microsoft:cancel-scheduled-message</c> (M7) — cancel scheduled messages by sequence number.</item>
/// </list>
///
/// Wire contract verified against <c>Azure.Messaging.ServiceBus.Amqp.ManagementConstants</c>:
/// response <c>application-properties</c> keys are <b>camelCase</b> (<c>statusCode</c>, <c>statusDescription</c>) —
/// this differs from <c>$cbs</c> which is kebab-case.
/// </summary>
public sealed class ManagementRequestProcessor : IRequestProcessor
{
    private const string OperationKey = "operation";
    private const string AssociatedLinkNameKey = "associated-link-name";
    private const string StatusCodeKey = "statusCode";
    private const string StatusDescriptionKey = "statusDescription";

    private const string RenewLockOperation = "com.microsoft:renew-lock";
    private const string ScheduleMessageOperation = "com.microsoft:schedule-message";
    private const string CancelScheduledMessageOperation = "com.microsoft:cancel-scheduled-message";
    private const string PeekMessageOperation = "com.microsoft:peek-message";

    private const string LockTokensBodyKey = "lock-tokens";
    private const string ExpirationsBodyKey = "expirations";
    private const string MessagesBodyKey = "messages";
    private const string MessageBodyKey = "message";
    private const string SequenceNumbersBodyKey = "sequence-numbers";
    private const string FromSequenceNumberBodyKey = "from-sequence-number";
    private const string MessageCountBodyKey = "message-count";

    private static readonly Symbol ScheduledEnqueueTimeSymbol = new("x-opt-scheduled-enqueue-time");
    private static readonly Symbol MessageStateSymbol = new("x-opt-message-state");
    private static readonly Symbol EnqueuedTimeUtcSymbol = new("x-opt-enqueued-time");
    private static readonly Symbol SequenceNumberSymbol = new("x-opt-sequence-number");

    // Service Bus message states the SDK reports via ServiceBusReceivedMessage.State.
    private const int MessageStateActive = 0;
    private const int MessageStateDeferred = 1;
    private const int MessageStateScheduled = 2;

    private readonly string _entityName;
    private readonly QueueDescriptor _descriptor;
    private readonly IMessageStore _store;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ManagementRequestProcessor> _logger;

    public ManagementRequestProcessor(
        string entityName,
        QueueDescriptor descriptor,
        IMessageStore store,
        TimeProvider timeProvider,
        ILogger<ManagementRequestProcessor> logger)
    {
        _entityName = entityName;
        _descriptor = descriptor;
        _store = store;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public int Credit => 100;

    public void Process(RequestContext requestContext)
    {
        var request = requestContext.Message;
        var operation = GetOperation(request);

        Message response;
        try
        {
            response = operation switch
            {
                RenewLockOperation => HandleRenewLock(request),
                ScheduleMessageOperation => HandleScheduleMessage(request),
                CancelScheduledMessageOperation => HandleCancelScheduledMessage(request),
                PeekMessageOperation => HandlePeekMessage(request),
                _ => BuildResponse(request, statusCode: 501, statusDescription: $"NotImplemented: {operation}"),
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "$management {Op} failed on {Entity}", operation, _entityName);
            response = BuildResponse(request, statusCode: 500, statusDescription: "InternalError: " + ex.Message);
        }

        requestContext.Complete(response);
    }

    private Message HandleRenewLock(Message request)
    {
        if (request.Body is not Map body || !body.TryGetValue(LockTokensBodyKey, out var lockTokensObj))
        {
            return BuildResponse(request, 400, "BadRequest: missing 'lock-tokens' in body");
        }

        var lockTokens = ExtractGuidArray(lockTokensObj);
        if (lockTokens.Length == 0)
        {
            return BuildResponse(request, 400, "BadRequest: empty 'lock-tokens'");
        }

        // The SDK includes `associated-link-name` so the broker can verify the renew is being
        // issued by the same receiver that took the lock.
        var requestingLink = request.ApplicationProperties?[AssociatedLinkNameKey] as string;

        var expirations = new DateTime[lockTokens.Length];
        for (var i = 0; i < lockTokens.Length; i++)
        {
            var newUntil = _store.TryRenewLockAsync(_entityName, lockTokens[i], _descriptor.LockDuration, requestingLink)
                .GetAwaiter().GetResult();
            if (newUntil is null)
            {
                // Service Bus surfaces "lock lost" as 410 Gone — for unknown lock OR cross-link renew.
                return BuildResponse(request, 410, $"Gone: lock token {lockTokens[i]} is unknown, expired, or held by a different link");
            }
            expirations[i] = newUntil.Value.UtcDateTime;
        }

        var responseBody = new Map { [ExpirationsBodyKey] = expirations };
        var response = BuildResponse(request, 200, "OK");
        response.BodySection = new AmqpValue { Value = responseBody };
        return response;
    }

    /// <summary>
    /// Schedule one or more messages. The body's <c>messages</c> array contains entries where
    /// <c>message</c> is the encoded AMQP payload; the scheduled delivery time lives inside that
    /// payload as the <c>x-opt-scheduled-enqueue-time</c> annotation. Returns the assigned
    /// sequence numbers so the SDK can later cancel them.
    /// </summary>
    private Message HandleScheduleMessage(Message request)
    {
        if (request.Body is not Map body || !body.TryGetValue(MessagesBodyKey, out var messagesObj))
        {
            return BuildResponse(request, 400, "BadRequest: missing 'messages' in body");
        }

        // Microsoft.Azure.Amqp sends List<AmqpMap> which arrives as Amqp.Types.List (= List<object>) of Map.
        var entries = messagesObj switch
        {
            Map[] m => m,
            object[] a => a.OfType<Map>().ToArray(),
            System.Collections.IList list => list.OfType<Map>().ToArray(),
            _ => Array.Empty<Map>(),
        };
        if (entries.Length == 0)
        {
            return BuildResponse(request, 400, "BadRequest: empty 'messages' list");
        }

        var sequenceNumbers = new long[entries.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            if (!entries[i].TryGetValue(MessageBodyKey, out var bytesObj))
            {
                return BuildResponse(request, 400, $"BadRequest: missing 'message' in entry {i}");
            }
            var encoded = ExtractMessageBytes(bytesObj);
            if (encoded is null)
            {
                return BuildResponse(request, 400, $"BadRequest: 'message' in entry {i} must be binary");
            }

            var amqp = DecodeMessage(encoded);
            var scheduledTime = ReadScheduledEnqueueTime(amqp) ?? _timeProvider.GetUtcNow();
            var expiresAt = ComputeExpiresAt(amqp);

            var stored = _store.EnqueueAsync(_entityName, encoded, expiresAt, scheduledTime).GetAwaiter().GetResult();
            sequenceNumbers[i] = stored.SequenceNumber;
        }

        var responseBody = new Map { [SequenceNumbersBodyKey] = sequenceNumbers };
        var response = BuildResponse(request, 200, "OK");
        response.BodySection = new AmqpValue { Value = responseBody };
        return response;
    }

    /// <summary>
    /// Read messages without locking. Request body has <c>from-sequence-number</c> (long) and
    /// <c>message-count</c> (int). Response body has a <c>messages</c> list, each entry being
    /// a map with <c>message</c> → encoded bytes. Each outgoing message has fresh stamps for
    /// <c>x-opt-sequence-number</c>, <c>x-opt-enqueued-time</c>, and <c>x-opt-message-state</c>
    /// so the SDK can surface <see cref="ServiceBusMessageState"/> correctly.
    /// </summary>
    private Message HandlePeekMessage(Message request)
    {
        if (request.Body is not Map body)
        {
            return BuildResponse(request, 400, "BadRequest: missing peek body");
        }
        var fromSeq = body.TryGetValue(FromSequenceNumberBodyKey, out var fObj) ? Convert.ToInt64(fObj) : 0L;
        var maxCount = body.TryGetValue(MessageCountBodyKey, out var cObj) ? Convert.ToInt32(cObj) : 1;
        if (maxCount <= 0) maxCount = 1;

        var peeked = _store.Peek(_entityName, fromSeq, maxCount);
        if (peeked.Count == 0)
        {
            // Service Bus returns 204 No Content for an empty peek window.
            return BuildResponse(request, 204, "NoContent");
        }

        var entries = new List<Map>(peeked.Count);
        foreach (var stored in peeked)
        {
            var amqp = DecodeMessage(stored.EncodedMessage);
            // Stamp the same broker-authoritative fields a normal delivery would carry.
            amqp.MessageAnnotations ??= new MessageAnnotations();
            amqp.MessageAnnotations.Map[SequenceNumberSymbol] = stored.SequenceNumber;
            amqp.MessageAnnotations.Map[EnqueuedTimeUtcSymbol] = stored.EnqueuedAt.UtcDateTime;
            amqp.MessageAnnotations.Map[MessageStateSymbol] = stored.ScheduledEnqueueTime is not null
                ? MessageStateScheduled
                : MessageStateActive;

            var rebytes = ReEncode(amqp);
            entries.Add(new Map { [MessageBodyKey] = rebytes });
        }

        var responseBody = new Map { [MessagesBodyKey] = entries };
        var response = BuildResponse(request, 200, "OK");
        response.BodySection = new AmqpValue { Value = responseBody };
        return response;
    }

    private static byte[] ReEncode(Message msg)
    {
        var buf = msg.Encode();
        var copy = new byte[buf.Length];
        Array.Copy(buf.Buffer, buf.Offset, copy, 0, buf.Length);
        return copy;
    }

    private Message HandleCancelScheduledMessage(Message request)
    {
        if (request.Body is not Map body || !body.TryGetValue(SequenceNumbersBodyKey, out var seqsObj))
        {
            return BuildResponse(request, 400, "BadRequest: missing 'sequence-numbers' in body");
        }
        var seqs = ExtractLongArray(seqsObj);
        if (seqs.Length == 0)
        {
            return BuildResponse(request, 400, "BadRequest: empty 'sequence-numbers'");
        }
        foreach (var seq in seqs)
        {
            // Cancel is idempotent — silent no-op for unknown seq or already-activated messages.
            _store.TryCancelScheduledAsync(_entityName, seq).GetAwaiter().GetResult();
        }
        return BuildResponse(request, 200, "OK");
    }

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

    private static byte[]? ExtractMessageBytes(object value) => value switch
    {
        byte[] bytes => bytes,
        ArraySegment<byte> seg => seg.ToArray(),
        _ => null,
    };

    private static Message DecodeMessage(byte[] encoded)
    {
        var buffer = new ByteBuffer(encoded, 0, encoded.Length, encoded.Length);
        return Message.Decode(buffer);
    }

    private static string GetOperation(Message request)
    {
        if (request.ApplicationProperties is null) return string.Empty;
        return request.ApplicationProperties[OperationKey] as string ?? string.Empty;
    }

    private static Guid[] ExtractGuidArray(object value) => value switch
    {
        Guid[] guids => guids,
        object[] objs => objs.OfType<Guid>().ToArray(),
        Guid single => [single],
        _ => Array.Empty<Guid>(),
    };

    private static long[] ExtractLongArray(object value) => value switch
    {
        long[] longs => longs,
        object[] objs => objs.Select(Convert.ToInt64).ToArray(),
        System.Collections.IList list => list.Cast<object>().Select(Convert.ToInt64).ToArray(),
        long single => [single],
        _ => Array.Empty<long>(),
    };

    private static Message BuildResponse(Message request, int statusCode, string statusDescription)
    {
        var response = new Message
        {
            Properties = new Properties(),
            ApplicationProperties = new ApplicationProperties(),
        };

        var requestId = request.Properties?.GetMessageId();
        if (requestId is not null) response.Properties.SetCorrelationId(requestId);

        response.ApplicationProperties[StatusCodeKey] = statusCode;
        response.ApplicationProperties[StatusDescriptionKey] = statusDescription;
        return response;
    }
}
