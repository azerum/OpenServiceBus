using Amqp;
using Amqp.Listener;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenServiceBus.Abstractions;
using OpenServiceBus.Amqp.Management;

namespace OpenServiceBus.Amqp;

public sealed class AmqpListenerHost : IHostedService, IAsyncDisposable
{
    private readonly AmqpListenerOptions _options;
    private readonly ILogger<AmqpListenerHost> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IQueueRegistry _queueRegistry;
    private readonly IMessageStore _messageStore;
    private ContainerHost? _host;

    public AmqpListenerHost(
        IOptions<AmqpListenerOptions> options,
        IQueueRegistry queueRegistry,
        IMessageStore messageStore,
        ILoggerFactory loggerFactory)
    {
        _options = options.Value;
        _queueRegistry = queueRegistry;
        _messageStore = messageStore;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<AmqpListenerHost>();
    }

    /// <summary>The address the listener was actually opened on.</summary>
    public Address? Address { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var address = new Address(_options.Host, _options.Port, user: null, password: null, path: "/", scheme: "amqp");
        var host = new ContainerHost(address);

        foreach (var listener in host.Listeners)
        {
            listener.SASL.EnableAnonymousMechanism = true;

            var handler = new ListenerOpenHandler(_options.ContainerId, _options.IdleTimeoutMs, _options.MaxFrameSize);
            listener.HandlerFactory = _ => handler;
        }

        host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());

        host.Open();

        _host = host;
        Address = address;

        // Hook queue lifecycle BEFORE listing existing queues so we don't miss concurrent creates.
        _queueRegistry.QueueCreated += OnQueueCreated;
        _queueRegistry.QueueDeleted += OnQueueDeleted;

        // Register processors for queues that already exist (e.g. when listener starts after the broker is warm).
        foreach (var existing in await _queueRegistry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            RegisterQueueProcessors(existing);
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

    private void OnQueueCreated(object? sender, QueueDescriptor descriptor) => RegisterQueueProcessors(descriptor);

    private void OnQueueDeleted(object? sender, QueueDescriptor descriptor)
    {
        try
        {
            _host?.UnregisterMessageProcessor(descriptor.Name);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error while unregistering processor for queue {Queue}", descriptor.Name);
        }
    }

    private void RegisterQueueProcessors(QueueDescriptor descriptor)
    {
        if (_host is null) return;

        var processor = new QueueSenderProcessor(
            descriptor.Name,
            _messageStore,
            _loggerFactory.CreateLogger<QueueSenderProcessor>());

        _host.RegisterMessageProcessor(descriptor.Name, processor);
        _logger.LogInformation("Registered AMQP sender processor for queue {Queue}", descriptor.Name);
    }
}
