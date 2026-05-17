using System.Collections.Concurrent;
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.Amqp;

/// <summary>
/// Single <see cref="ILinkProcessor"/> registered with the <see cref="ContainerHost"/>.
/// Resolves the entity name from the incoming attach and wires the link to the right endpoint:
///
///   sender attach (client → broker) on <c>&lt;queue&gt;</c>        → TargetLinkEndpoint over <see cref="QueueSenderProcessor"/>
///   receiver attach (broker → client) on <c>&lt;queue&gt;</c>      → SourceLinkEndpoint over <see cref="QueueReceiverSource"/>
///   <c>&lt;queue&gt;/$DeadLetterQueue</c>                          → reserved for M5
///   <c>&lt;queue&gt;/$management</c>                               → reserved for M5
///   unknown entity                                                → refused with amqp:not-found
/// </summary>
public sealed class EntityLinkProcessor : ILinkProcessor
{
    private readonly IQueueRegistry _registry;
    private readonly IMessageStore _store;
    private readonly AmqpListenerOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<EntityLinkProcessor> _logger;

    // QueueReceiverSource caches one instance per entity address so multiple clients share state.
    private readonly ConcurrentDictionary<string, QueueReceiverSource> _receiverSources = new(StringComparer.OrdinalIgnoreCase);

    public EntityLinkProcessor(
        IQueueRegistry registry,
        IMessageStore store,
        IOptions<AmqpListenerOptions> options,
        ILoggerFactory loggerFactory)
    {
        _registry = registry;
        _store = store;
        _options = options.Value;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<EntityLinkProcessor>();
    }

    public void Process(AttachContext attachContext)
    {
        try
        {
            var attach = attachContext.Attach;
            var isReceiverFromClient = !attach.Role; // role=false: client is sender ⇒ broker receives

            var rawAddress = isReceiverFromClient
                ? (attach.Target as Target)?.Address
                : (attach.Source as Source)?.Address;

            if (!EntityAddress.TryParse(rawAddress, out var entityAddress))
            {
                Reject(attachContext, ErrorCode.InvalidField, "Link attach has no resolvable address.");
                return;
            }

            // $management is served via per-queue IRequestProcessor registrations done by AmqpListenerHost
            // when each queue is created. ContainerHost routes those attaches before reaching this LinkProcessor,
            // so seeing $management here means an attach to a queue that doesn't have a management endpoint yet.
            if (entityAddress.SubResource == EntitySubResource.Management)
            {
                Reject(attachContext, ErrorCode.NotFound,
                    $"No $management endpoint for entity '{entityAddress.Entity}'.");
                return;
            }

            var routingEntity = entityAddress.SubResource switch
            {
                EntitySubResource.Main => entityAddress.Entity,
                EntitySubResource.DeadLetterQueue => entityAddress.Entity + "/$DeadLetterQueue",
                _ => null,
            };
            if (routingEntity is null)
            {
                Reject(attachContext, ErrorCode.NotImplemented,
                    $"Address sub-resource '{entityAddress.SubResource}' is not implemented yet.");
                return;
            }

            var descriptor = _registry.GetAsync(routingEntity).GetAwaiter().GetResult();
            if (descriptor is null)
            {
                Reject(attachContext, ErrorCode.NotFound, $"Entity '{routingEntity}' does not exist.");
                return;
            }

            // AttachContext.Complete echoes the incoming attach as the response. The Azure SDK
            // reads max-message-size from the attach reply and rejects sends if it's missing
            // (interprets as -1 bytes). The client doesn't set it, so we stamp it here.
            attach.MaxMessageSize = _options.MaxMessageSize;

            if (isReceiverFromClient)
            {
                var processor = new QueueSenderProcessor(
                    descriptor.Name, _store, _loggerFactory.CreateLogger<QueueSenderProcessor>());
                var endpoint = new TargetLinkEndpoint(processor, attachContext.Link);
                attachContext.Complete(endpoint, processor.Credit);
                _logger.LogDebug("Wired sender attach to {Entity} (credit={Credit})", descriptor.Name, processor.Credit);
            }
            else
            {
                var source = _receiverSources.GetOrAdd(descriptor.Name, name => new QueueReceiverSource(
                    name, descriptor, _store, _loggerFactory.CreateLogger<QueueReceiverSource>()));
                var endpoint = new SourceLinkEndpoint(source, attachContext.Link);
                // initialCredit=0: broker is the sender on this link; client grants credit via Flow.
                attachContext.Complete(endpoint, 0);
                _logger.LogDebug("Wired receiver attach to {Entity}", descriptor.Name);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process link attach");
            Reject(attachContext, ErrorCode.InternalError, ex.Message);
        }
    }

    private static void Reject(AttachContext attachContext, string code, string description)
    {
        attachContext.Complete(new Error(new Symbol(code)) { Description = description });
    }
}
