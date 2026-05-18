using OpenServiceBus.Amqp.Hosting;
using OpenServiceBus.Amqp.Queues;
using OpenServiceBus.Amqp.Topics;

using System.Collections.Concurrent;
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.Routing;

/// <summary>
/// Single <see cref="ILinkProcessor"/> registered with the <see cref="ContainerHost"/>.
/// Resolves the entity name from the incoming attach and wires the link to the right endpoint:
///
///   sender attach on <c>&lt;queue&gt;</c>                        → QueueSenderProcessor
///   receiver attach on <c>&lt;queue&gt;</c>                      → QueueReceiverSource
///   any attach on <c>&lt;queue&gt;/$DeadLetterQueue</c>          → routes to the DLQ backing queue
///   sender attach on <c>&lt;topic&gt;</c>                        → TopicSenderProcessor (M13)
///   receiver attach on <c>&lt;topic&gt;/Subscriptions/&lt;s&gt;</c> → QueueReceiverSource on the sub backing queue (M13)
///   <c>$management</c> attaches                                  → per-entity IRequestProcessor (registered by AmqpListenerHost)
///   unknown entity                                               → refused with amqp:not-found
/// </summary>
public sealed class EntityLinkProcessor : ILinkProcessor
{
    private readonly IQueueRegistry _registry;
    private readonly ITopicRegistry? _topics;
    private readonly IMessageStore _store;
    private readonly AmqpListenerOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<EntityLinkProcessor> _logger;

    private readonly ConcurrentDictionary<string, QueueReceiverSource> _receiverSources = new(StringComparer.OrdinalIgnoreCase);

    public EntityLinkProcessor(
        IQueueRegistry registry,
        IMessageStore store,
        IOptions<AmqpListenerOptions> options,
        TimeProvider timeProvider,
        ILoggerFactory loggerFactory,
        ITopicRegistry? topics = null)
    {
        _registry = registry;
        _topics = topics;
        _store = store;
        _options = options.Value;
        _timeProvider = timeProvider;
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

            if (entityAddress.SubResource == EntitySubResource.Management)
            {
                Reject(attachContext, ErrorCode.NotFound,
                    $"No $management endpoint for entity '{entityAddress.Entity}'.");
                return;
            }

            attach.MaxMessageSize = _options.MaxMessageSize;

            if (entityAddress.Kind == EntityKind.Subscription)
            {
                if (!RouteSubscriptionAttach(attachContext, entityAddress, isReceiverFromClient))
                {
                    return;
                }
                return;
            }

            // Topic vs queue is ambiguous from the attach alone — the address looks the same
            // ("orders" could be a queue or a topic). Resolve preferring topics IF this is a
            // sender attach and the topic exists; otherwise fall through to queue lookup.
            // Receiver attaches with a bare topic name (no /Subscriptions/) aren't valid in
            // Service Bus; we let them fall through to queue handling which will fail correctly
            // if no queue exists.
            if (isReceiverFromClient && _topics is not null && entityAddress.SubResource == EntitySubResource.Main)
            {
                var topic = _topics.GetTopicAsync(entityAddress.Entity).GetAwaiter().GetResult();
                if (topic is not null)
                {
                    WireTopicSender(attachContext, topic);
                    return;
                }
            }

            RouteQueueAttach(attachContext, entityAddress, isReceiverFromClient);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process link attach");
            Reject(attachContext, ErrorCode.InternalError, ex.Message);
        }
    }

    private bool RouteSubscriptionAttach(AttachContext attachContext, EntityAddress entityAddress, bool isReceiverFromClient)
    {
        if (_topics is null)
        {
            Reject(attachContext, ErrorCode.NotImplemented, "Topic registry not configured on this broker.");
            return false;
        }

        var sub = _topics.GetSubscriptionAsync(entityAddress.Entity, entityAddress.Subscription!).GetAwaiter().GetResult();
        if (sub is null)
        {
            Reject(attachContext, ErrorCode.NotFound,
                $"Subscription '{entityAddress.Subscription}' on topic '{entityAddress.Entity}' does not exist.");
            return false;
        }

        // Subscriptions are backed by regular queues — the receiver path works as-is. Sender
        // attaches to a subscription don't have a SB semantic (you publish to the topic, not the
        // subscription); refuse them to avoid silent misuse.
        if (isReceiverFromClient)
        {
            Reject(attachContext, ErrorCode.NotAllowed,
                "Senders must attach to the topic itself, not to a subscription.");
            return false;
        }

        var backingQueue = entityAddress.BackingQueueName;
        var descriptor = _registry.GetAsync(backingQueue).GetAwaiter().GetResult();
        if (descriptor is null)
        {
            Reject(attachContext, ErrorCode.NotFound, $"Backing queue '{backingQueue}' for subscription is missing.");
            return false;
        }

        var source = _receiverSources.GetOrAdd(backingQueue, name => new QueueReceiverSource(
            name, descriptor, _store, _timeProvider, _loggerFactory.CreateLogger<QueueReceiverSource>()));
        var endpoint = new SourceLinkEndpoint(source, attachContext.Link);
        attachContext.Complete(endpoint, 0);
        _logger.LogDebug("Wired subscription receiver attach to {Entity}", backingQueue);
        return true;
    }

    private void WireTopicSender(AttachContext attachContext, TopicDescriptor topic)
    {
        var processor = new TopicSenderProcessor(
            topic,
            _topics!,
            _store,
            _timeProvider,
            _loggerFactory.CreateLogger<TopicSenderProcessor>());
        var endpoint = new TargetLinkEndpoint(processor, attachContext.Link);
        attachContext.Complete(endpoint, processor.Credit);
        _logger.LogDebug("Wired topic sender attach to {Topic} (credit={Credit})", topic.Name, processor.Credit);
    }

    private void RouteQueueAttach(AttachContext attachContext, EntityAddress entityAddress, bool isReceiverFromClient)
    {
        var routingEntity = entityAddress.BackingQueueName;
        var descriptor = _registry.GetAsync(routingEntity).GetAwaiter().GetResult();
        if (descriptor is null)
        {
            Reject(attachContext, ErrorCode.NotFound, $"Entity '{routingEntity}' does not exist.");
            return;
        }

        if (isReceiverFromClient)
        {
            var processor = new QueueSenderProcessor(
                descriptor.Name,
                descriptor,
                _store,
                _timeProvider,
                _loggerFactory.CreateLogger<QueueSenderProcessor>());
            var endpoint = new TargetLinkEndpoint(processor, attachContext.Link);
            attachContext.Complete(endpoint, processor.Credit);
            _logger.LogDebug("Wired sender attach to {Entity} (credit={Credit})", descriptor.Name, processor.Credit);
        }
        else
        {
            var source = _receiverSources.GetOrAdd(descriptor.Name, name => new QueueReceiverSource(
                name, descriptor, _store, _timeProvider, _loggerFactory.CreateLogger<QueueReceiverSource>()));
            var endpoint = new SourceLinkEndpoint(source, attachContext.Link);
            attachContext.Complete(endpoint, 0);
            _logger.LogDebug("Wired receiver attach to {Entity}", descriptor.Name);
        }
    }

    private static void Reject(AttachContext attachContext, string code, string description)
    {
        attachContext.Complete(new Error(new Symbol(code)) { Description = description });
    }
}
