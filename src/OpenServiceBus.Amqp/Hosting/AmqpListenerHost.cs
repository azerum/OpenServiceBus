using Amqp;
using Amqp.Listener;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenServiceBus.Amqp.Management;
using OpenServiceBus.Amqp.Routing;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace OpenServiceBus.Amqp.Hosting;

public sealed class AmqpListenerHost : IHostedService, IAsyncDisposable
{
    private readonly AmqpListenerOptions _options;
    private readonly ILogger<AmqpListenerHost> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IQueueRegistry _queueRegistry;
    private readonly IMessageStore _messageStore;
    private readonly TimeProvider _timeProvider;
    private ContainerHost? _host;

    public AmqpListenerHost(
        IOptions<AmqpListenerOptions> options,
        IQueueRegistry queueRegistry,
        IMessageStore messageStore,
        TimeProvider timeProvider,
        ILoggerFactory loggerFactory)
    {
        _options = options.Value;
        _queueRegistry = queueRegistry;
        _messageStore = messageStore;
        _timeProvider = timeProvider;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<AmqpListenerHost>();
    }

    /// <summary>The address the listener was actually opened on.</summary>
    public Address? Address { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_options.EnableFrameTracing)
        {
            Trace.TraceLevel = TraceLevel.Frame;
            Trace.TraceListener = (level, fmt, args) =>
                _logger.LogDebug("[amqp] " + fmt, args ?? Array.Empty<object?>());
        }

        var address = new Address(_options.Host, _options.Port, user: null, password: null, path: "/", scheme: "amqp");
        var host = new ContainerHost(address);

        foreach (var listener in host.Listeners)
        {
            ServiceBusSasl.ConfigureListenerMechanisms(listener);

            var handler = new ListenerEventHandler(_options.ContainerId, _options.IdleTimeoutMs, _options.MaxFrameSize);
            listener.HandlerFactory = _ => handler;
        }

        host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());

        var linkProcessor = new EntityLinkProcessor(_queueRegistry, _messageStore, Options.Create(_options), _timeProvider, _loggerFactory);
        host.RegisterLinkProcessor(linkProcessor);

        host.Open();
        _host = host;
        Address = address;

        // Per-queue $management endpoint registration. Subscribe BEFORE listing so we don't
        // miss any queues created concurrently with startup.
        _queueRegistry.QueueCreated += OnQueueCreated;
        _queueRegistry.QueueDeleted += OnQueueDeleted;
        foreach (var existing in await _queueRegistry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            RegisterManagementEndpoint(existing);
        }

        _logger.LogInformation(
            "AMQP listener opened on amqp://{Host}:{Port} (containerId={ContainerId}, idleTimeoutMs={Idle}, maxFrameSize={Frame})",
            _options.Host, _options.Port, _options.ContainerId, _options.IdleTimeoutMs, _options.MaxFrameSize);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _queueRegistry.QueueCreated -= OnQueueCreated;
        _queueRegistry.QueueDeleted -= OnQueueDeleted;

        if (_host is null)
        {
            return Task.CompletedTask;
        }

        _logger.LogInformation("AMQP listener shutting down");
        try
        {
            _host.Close();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error while closing AMQP listener");
        }
        finally
        {
            _host = null;
        }

        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => new(StopAsync(CancellationToken.None));

    private void OnQueueCreated(object? sender, QueueDescriptor descriptor) => RegisterManagementEndpoint(descriptor);

    private void OnQueueDeleted(object? sender, QueueDescriptor descriptor)
    {
        if (_host is null) return;
        if (EntityNames.IsDeadLetterQueue(descriptor.Name)) return;

        try
        {
            _host.UnregisterRequestProcessor(descriptor.Name + "/$management");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to unregister $management for queue {Queue}", descriptor.Name);
        }
    }

    private void RegisterManagementEndpoint(QueueDescriptor descriptor)
    {
        if (_host is null) return;
        // DLQ siblings don't get their own $management endpoint - all management ops target the parent queue.
        if (EntityNames.IsDeadLetterQueue(descriptor.Name)) return;

        var processor = new ManagementRequestProcessor(
            descriptor.Name,
            descriptor,
            _messageStore,
            _timeProvider,
            _loggerFactory.CreateLogger<ManagementRequestProcessor>());

        _host.RegisterRequestProcessor(descriptor.Name + "/$management", processor);
        _logger.LogDebug("Registered $management endpoint for queue {Queue}", descriptor.Name);
    }
}
