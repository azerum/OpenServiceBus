using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using OpenServiceBus.Amqp.Hosting;
using OpenServiceBus.Amqp.Lifecycle;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Storage;
using OpenServiceBus.InMemoryStorage;
using OpenServiceBus.InMemoryStorage.Lifecycle;
using OpenServiceBus.InMemoryStorage.Queues;
using OpenServiceBus.InMemoryStorage.Routing;
using OpenServiceBus.InMemoryStorage.Topics;
using OpenServiceBus.InMemoryStorage.Transactions;

namespace OpenServiceBus.Testing;

/// <summary>
/// Embeddable, zero-dependency Service Bus broker for use inside unit/integration test fixtures.
/// One instance binds an in-memory broker to a free loopback port and exposes a connection string
/// the Azure SDK can use unmodified. Dispose to release the port.
/// </summary>
/// <example>
/// <code>
/// await using var host = await OpenServiceBusTestHost.StartAsync();
/// await host.CreateQueueAsync("orders");
/// await using var client = new ServiceBusClient(host.ConnectionString);
/// // send / receive as usual
/// </code>
/// </example>
public sealed class OpenServiceBusTestHost : IAsyncDisposable
{
    private readonly AmqpListenerHost _listener;
    private readonly TtlExpirationService _ttlSweeper;
    private readonly ScheduledMessageActivator _scheduledActivator;
    private bool _disposed;

    private OpenServiceBusTestHost(
        AmqpListenerHost listener,
        TtlExpirationService ttlSweeper,
        ScheduledMessageActivator scheduledActivator,
        IQueueRegistry queues,
        ITopicRegistry topics,
        IMessageStore store,
        TimeProvider timeProvider,
        int port,
        string connectionString)
    {
        _listener = listener;
        _ttlSweeper = ttlSweeper;
        _scheduledActivator = scheduledActivator;
        Queues = queues;
        Topics = topics;
        Store = store;
        TimeProvider = timeProvider;
        Port = port;
        ConnectionString = connectionString;
    }

    /// <summary>Service Bus SDK connection string with <c>UseDevelopmentEmulator=true</c>.</summary>
    public string ConnectionString { get; }

    /// <summary>Raw AMQP URI (<c>amqp://host:port</c>) for AMQPNetLite or low-level clients.</summary>
    public string AmqpUri => $"amqp://127.0.0.1:{Port}";

    /// <summary>Port the broker is listening on.</summary>
    public int Port { get; }

    /// <summary>Queue registry — use to create/list/delete queues from inside tests.</summary>
    public IQueueRegistry Queues { get; }

    /// <summary>Topic registry — use to create/list/delete topics, subscriptions, and rules.</summary>
    public ITopicRegistry Topics { get; }

    /// <summary>In-memory message store — exposed for direct test inspection.</summary>
    public IMessageStore Store { get; }

    /// <summary><see cref="System.TimeProvider"/> the broker is driven by.</summary>
    public TimeProvider TimeProvider { get; }

    /// <summary>Start a broker on a free port (or the port specified in options) and return a ready-to-use host.</summary>
    public static async Task<OpenServiceBusTestHost> StartAsync(Action<OpenServiceBusTestHostOptions>? configure = null)
    {
        var opts = new OpenServiceBusTestHostOptions();
        configure?.Invoke(opts);

        var port = opts.Port ?? GetFreePort();

        var listenerOptions = new AmqpListenerOptions
        {
            Host = opts.Host,
            Port = port,
            ContainerId = opts.ContainerId,
            IdleTimeoutMs = opts.IdleTimeoutMs,
            MaxMessageSize = opts.MaxMessageSize,
            EnableFrameTracing = opts.EnableFrameTracing,
            RequireSasAuth = opts.RequireSasAuth,
        };
        if (opts.RequireSasAuth)
        {
            listenerOptions.SasKeys[opts.SasKeyName] = opts.SasKey;
            foreach (var (name, key) in opts.AdditionalSasKeys)
            {
                listenerOptions.SasKeys[name] = key;
            }
        }

        // M18: callers can swap in a different backing store (e.g. SQLite) via opts.StoreFactory.
        // The rest of the stack (registries, router, transactions, listener) is identical and
        // talks to whatever the factory hands back through the IMessageStore interface.
        IMessageStore storeAsIface = opts.StoreFactory is not null
            ? opts.StoreFactory(opts.TimeProvider)
            : new InMemoryMessageStore(opts.TimeProvider);
        var queues = new QueueManager(storeAsIface);
        var topics = new TopicManager(queues);
        var router = new MessageRouter(queues, storeAsIface, NullLogger<MessageRouter>.Instance, topics);
        var transactions = new TransactionManager(NullLogger<TransactionManager>.Instance);

        var listener = new AmqpListenerHost(
            Options.Create(listenerOptions),
            queues,
            storeAsIface,
            router,
            transactions,
            opts.TimeProvider,
            NullLoggerFactory.Instance,
            topics);

        var ttlSweeper = new TtlExpirationService(
            storeAsIface,
            queues,
            router,
            opts.TimeProvider,
            NullLogger<TtlExpirationService>.Instance);

        var scheduledActivator = new ScheduledMessageActivator(
            storeAsIface,
            queues,
            opts.TimeProvider,
            NullLogger<ScheduledMessageActivator>.Instance);

        await listener.StartAsync(CancellationToken.None);
        await ttlSweeper.StartAsync(CancellationToken.None);
        await scheduledActivator.StartAsync(CancellationToken.None);

        var connectionString =
            $"Endpoint=sb://{opts.Host}:{port};SharedAccessKeyName={opts.SasKeyName};SharedAccessKey={opts.SasKey};UseDevelopmentEmulator=true";

        return new OpenServiceBusTestHost(
            listener,
            ttlSweeper,
            scheduledActivator,
            queues,
            topics,
            storeAsIface,
            opts.TimeProvider,
            port,
            connectionString);
    }

    /// <summary>Create a queue with default settings. Returns the resulting descriptor.</summary>
    public Task<QueueDescriptor> CreateQueueAsync(string name) =>
        Queues.CreateAsync(new QueueDescriptor { Name = name });

    /// <summary>Create a queue from a pre-built descriptor. Returns the resulting descriptor.</summary>
    public Task<QueueDescriptor> CreateQueueAsync(QueueDescriptor descriptor) =>
        Queues.CreateAsync(descriptor);

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await _scheduledActivator.StopAsync(CancellationToken.None);
        _scheduledActivator.Dispose();
        await _ttlSweeper.StopAsync(CancellationToken.None);
        _ttlSweeper.Dispose();
        await _listener.StopAsync(CancellationToken.None);
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }
}
