using OpenServiceBus.Amqp.Hosting;
using OpenServiceBus.Core.Storage;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Testing;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Thin adapter that lets the existing integration suite keep its old field names while
/// delegating actual broker lifecycle to the public <see cref="OpenServiceBusTestHost"/>.
/// New code should use <see cref="OpenServiceBusTestHost"/> directly.
/// </summary>
internal sealed class IntegrationHarness : IAsyncDisposable
{
    private readonly OpenServiceBusTestHost _host;

    private IntegrationHarness(OpenServiceBusTestHost host) => _host = host;

    public IQueueRegistry Queues => _host.Queues;
    public ITopicRegistry Topics => _host.Topics;
    public IMessageStore Store => _host.Store;
    public int Port => _host.Port;
    public string AmqpUri => _host.AmqpUri;
    public string ConnectionString => _host.ConnectionString;
    public string? WebSocketConnectionString => _host.WebSocketConnectionString;

    public static async Task<IntegrationHarness> StartAsync(
        Action<AmqpListenerOptions>? configure = null,
        bool enableWebSocketBridge = false)
    {
        var host = await OpenServiceBusTestHost.StartAsync(o =>
        {
            o.ContainerId = "OpenServiceBus.Integration";
            o.EnableFrameTracing = Environment.GetEnvironmentVariable("OSB_TRACE_FRAMES") == "1";
            o.EnableWebSocketBridge = enableWebSocketBridge;

            if (configure is not null)
            {
                var listenerOpts = new AmqpListenerOptions { Port = 0 };
                configure(listenerOpts);

                o.RequireSasAuth = listenerOpts.RequireSasAuth;
                foreach (var (name, key) in listenerOpts.SasKeys)
                {
                    o.AdditionalSasKeys[name] = key;
                }
            }
        });
        return new IntegrationHarness(host);
    }

    public ValueTask DisposeAsync() => _host.DisposeAsync();
}
