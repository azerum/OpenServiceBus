using Amqp;
using Amqp.Framing;
using Amqp.Sasl;

namespace OpenServiceBus.Amqp.WireTests;

public class ConnectionLifecycleTests
{
    private static ConnectionFactory CreateClientFactory()
    {
        var factory = new ConnectionFactory();
        factory.SASL.Profile = SaslProfile.Anonymous;
        return factory;
    }

    [Fact]
    public async Task Client_can_open_and_close_a_connection()
    {
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();

        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            conn.ConnectionState.ShouldBe(ConnectionState.Opened);
        }
        finally
        {
            await conn.CloseAsync();
        }

        conn.ConnectionState.ShouldBe(ConnectionState.End);
        conn.Error.ShouldBeNull();
    }

    [Fact]
    public async Task Multiple_connections_in_parallel_all_succeed()
    {
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();

        var openTasks = Enumerable.Range(0, 25).Select(async _ =>
        {
            var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
            try
            {
                conn.ConnectionState.ShouldBe(ConnectionState.Opened);
            }
            finally
            {
                await conn.CloseAsync();
            }
        });

        await Task.WhenAll(openTasks);
    }

    [Fact]
    public async Task Client_observes_broker_container_id_and_idle_timeout()
    {
        var observedContainerId = string.Empty;
        uint observedIdleTimeout = 0;

        await using var harness = await TestListenerHarness.StartAsync(o =>
        {
            o.ContainerId = "OpenServiceBus.Wire";
            o.IdleTimeoutMs = 12_345;
        });

        var factory = CreateClientFactory();
        var open = new Open
        {
            ContainerId = "client-under-test",
            HostName = "127.0.0.1",
        };

        var conn = await factory.CreateAsync(
            new Address(harness.AmqpUri),
            open,
            (c, remoteOpen) =>
            {
                observedContainerId = remoteOpen.ContainerId;
                observedIdleTimeout = remoteOpen.IdleTimeOut;
            });

        try
        {
            observedContainerId.ShouldBe("OpenServiceBus.Wire");
            observedIdleTimeout.ShouldBe(12_345u);
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Session_can_be_opened_and_closed()
    {
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();

        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Listener_stops_cleanly_with_no_clients()
    {
        var harness = await TestListenerHarness.StartAsync();
        await harness.DisposeAsync();

        // Re-binding the same port should succeed after a clean shutdown.
        var harness2 = await TestListenerHarness.StartAsync(o => o.Port = harness.Port);
        await harness2.DisposeAsync();
    }
}
