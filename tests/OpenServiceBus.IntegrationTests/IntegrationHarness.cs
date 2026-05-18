using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;
using OpenServiceBus.Amqp.DependencyInjection;
using OpenServiceBus.Amqp.Hosting;
using OpenServiceBus.Amqp.Lifecycle;
using OpenServiceBus.Amqp.Queues;
using OpenServiceBus.Amqp.Routing;
using OpenServiceBus.InMemoryStorage;
using OpenServiceBus.InMemoryStorage.DependencyInjection;
using OpenServiceBus.InMemoryStorage.Lifecycle;
using OpenServiceBus.InMemoryStorage.Queues;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Minimal in-process broker for Azure SDK integration tests. Provides an
/// SDK-ready connection string with <c>UseDevelopmentEmulator=true</c>.
/// Will be replaced by the public <c>OpenServiceBusTestHost</c> in M10.
/// </summary>
internal sealed class IntegrationHarness : IAsyncDisposable
{
    public AmqpListenerHost Host { get; }
    public TtlExpirationService TtlSweeper { get; }
    public IQueueRegistry Queues { get; }
    public IMessageStore Store { get; }
    public int Port { get; }
    public string AmqpUri => $"amqp://127.0.0.1:{Port}";

    /// <summary>Service Bus SDK connection string targeting this broker in emulator mode.</summary>
    public string ConnectionString =>
        $"Endpoint=sb://127.0.0.1:{Port};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";

    private IntegrationHarness(AmqpListenerHost host, TtlExpirationService ttlSweeper, IQueueRegistry queues, IMessageStore store, int port)
    {
        Host = host;
        TtlSweeper = ttlSweeper;
        Queues = queues;
        Store = store;
        Port = port;
    }

    public static async Task<IntegrationHarness> StartAsync()
    {
        var port = GetFreePort();
        var options = new AmqpListenerOptions
        {
            Host = "127.0.0.1",
            Port = port,
            ContainerId = "OpenServiceBus.Integration",
            IdleTimeoutMs = 30_000,
            EnableFrameTracing = Environment.GetEnvironmentVariable("OSB_TRACE_FRAMES") == "1",
        };

        var store = new InMemoryMessageStore();
        IMessageStore storeAsIface = store;
        var queues = new QueueManager(storeAsIface);

        var enableLogs = Environment.GetEnvironmentVariable("OSB_TRACE_FRAMES") == "1";
        ILoggerFactory loggerFactory = enableLogs
            ? LoggerFactory.Create(b => b
                .SetMinimumLevel(LogLevel.Debug)
                .AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss.fff "; }))
            : NullLoggerFactory.Instance;

        var host = new AmqpListenerHost(
            Options.Create(options),
            queues,
            storeAsIface,
            TimeProvider.System,
            loggerFactory);

        var ttlSweeper = new TtlExpirationService(
            storeAsIface,
            queues,
            TimeProvider.System,
            loggerFactory.CreateLogger<TtlExpirationService>());

        await host.StartAsync(CancellationToken.None);
        await ttlSweeper.StartAsync(CancellationToken.None);
        return new IntegrationHarness(host, ttlSweeper, queues, storeAsIface, port);
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    public async ValueTask DisposeAsync()
    {
        await TtlSweeper.StopAsync(CancellationToken.None);
        TtlSweeper.Dispose();
        await Host.StopAsync(CancellationToken.None);
    }
}
