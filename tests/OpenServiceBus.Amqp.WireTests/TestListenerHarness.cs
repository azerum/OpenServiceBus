using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using OpenServiceBus.Abstractions;
using OpenServiceBus.Amqp;
using OpenServiceBus.Broker;

namespace OpenServiceBus.Amqp.WireTests;

/// <summary>
/// Spins up an <see cref="AmqpListenerHost"/> bound to a random free TCP port on 127.0.0.1
/// with an in-memory broker behind it. Intended for wire-level tests until the full
/// <c>OpenServiceBusTestHost</c> lands in M10.
/// </summary>
internal sealed class TestListenerHarness : IAsyncDisposable
{
    public AmqpListenerHost Host { get; }
    public IQueueRegistry Queues { get; }
    public IMessageStore Store { get; }
    public int Port { get; }
    public string AmqpUri => $"amqp://127.0.0.1:{Port}";

    private TestListenerHarness(AmqpListenerHost host, IQueueRegistry queues, IMessageStore store, int port)
    {
        Host = host;
        Queues = queues;
        Store = store;
        Port = port;
    }

    public static async Task<TestListenerHarness> StartAsync(Action<AmqpListenerOptions>? configure = null)
    {
        var port = GetFreePort();
        var options = new AmqpListenerOptions
        {
            Host = "127.0.0.1",
            Port = port,
            ContainerId = "OpenServiceBus.Test",
            IdleTimeoutMs = 30_000,
            MaxFrameSize = 64 * 1024,
        };
        configure?.Invoke(options);

        var store = new InMemoryMessageStore();
        IMessageStore storeAsIface = store;
        var queues = new QueueManager(storeAsIface);

        var host = new AmqpListenerHost(
            Options.Create(options),
            queues,
            storeAsIface,
            NullLoggerFactory.Instance);

        await host.StartAsync(CancellationToken.None);
        return new TestListenerHarness(host, queues, storeAsIface, options.Port);
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync(CancellationToken.None);
    }
}
