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
/// Operations implemented in M5:
/// <list type="bullet">
///   <item><c>com.microsoft:renew-lock</c> — extends one or more peek-lock deadlines.</item>
/// </list>
///
/// Wire contract verified against <c>Azure.Messaging.ServiceBus.Amqp.ManagementConstants</c>:
/// response <c>application-properties</c> keys are <b>camelCase</b> (<c>statusCode</c>, <c>statusDescription</c>) —
/// this differs from <c>$cbs</c> which is kebab-case.
/// </summary>
public sealed class ManagementRequestProcessor : IRequestProcessor
{
    private const string OperationKey = "operation";
    private const string StatusCodeKey = "statusCode";
    private const string StatusDescriptionKey = "statusDescription";

    private const string RenewLockOperation = "com.microsoft:renew-lock";
    private const string LockTokensBodyKey = "lock-tokens";
    private const string ExpirationsBodyKey = "expirations";

    private readonly string _entityName;
    private readonly QueueDescriptor _descriptor;
    private readonly IMessageStore _store;
    private readonly ILogger<ManagementRequestProcessor> _logger;

    public ManagementRequestProcessor(
        string entityName,
        QueueDescriptor descriptor,
        IMessageStore store,
        ILogger<ManagementRequestProcessor> logger)
    {
        _entityName = entityName;
        _descriptor = descriptor;
        _store = store;
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

        var expirations = new DateTime[lockTokens.Length];
        for (var i = 0; i < lockTokens.Length; i++)
        {
            var newUntil = _store.TryRenewLockAsync(_entityName, lockTokens[i], _descriptor.LockDuration)
                .GetAwaiter().GetResult();
            if (newUntil is null)
            {
                // Service Bus surfaces "lock lost" as 410 Gone.
                return BuildResponse(request, 410, $"Gone: lock token {lockTokens[i]} is unknown or expired");
            }
            expirations[i] = newUntil.Value.UtcDateTime;
        }

        var responseBody = new Map { [ExpirationsBodyKey] = expirations };
        var response = BuildResponse(request, 200, "OK");
        response.BodySection = new AmqpValue { Value = responseBody };
        return response;
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
