using Amqp;
using Amqp.Listener;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenServiceBus.Abstractions;
using OpenServiceBus.Amqp.Management;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

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

    public Task StartAsync(CancellationToken cancellationToken)
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

            var handler = new ListenerOpenHandler(_options.ContainerId, _options.IdleTimeoutMs, _options.MaxFrameSize);
            listener.HandlerFactory = _ => handler;
        }

        host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());

        var linkProcessor = new EntityLinkProcessor(_queueRegistry, _messageStore, Options.Create(_options), _loggerFactory);
        host.RegisterLinkProcessor(linkProcessor);

        host.Open();
        _host = host;
        Address = address;

        _logger.LogInformation(
            "AMQP listener opened on amqp://{Host}:{Port} (containerId={ContainerId}, idleTimeoutMs={Idle}, maxFrameSize={Frame})",
            _options.Host, _options.Port, _options.ContainerId, _options.IdleTimeoutMs, _options.MaxFrameSize);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
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
}
