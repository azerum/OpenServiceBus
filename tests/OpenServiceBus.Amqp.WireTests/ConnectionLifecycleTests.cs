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
    public async Task CreateAsync_HarnessRunning_OpensAndClosesConnectionCleanly()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();

        // Act
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        ConnectionState openedState;
        try
        {
            openedState = conn.ConnectionState;
        }
        finally
        {
            await conn.CloseAsync();
        }

        // Assert
        openedState.ShouldBe(ConnectionState.Opened);
        conn.ConnectionState.ShouldBe(ConnectionState.End);
        conn.Error.ShouldBeNull();
    }

    [Fact]
    public async Task CreateAsync_TwentyFiveParallelConnections_AllReachOpenedState()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();

        // Act
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

        // Assert
        await Task.WhenAll(openTasks);
    }

    [Fact]
    public async Task CreateAsync_BrokerConfiguredContainerIdAndIdleTimeout_ClientObservesBothInRemoteOpen()
    {
        // Arrange
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

        // Act
        var conn = await factory.CreateAsync(
            new Address(harness.AmqpUri),
            open,
            (c, remoteOpen) =>
            {
                observedContainerId = remoteOpen.ContainerId;
                observedIdleTimeout = remoteOpen.IdleTimeOut;
            });

        // Assert
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
    public async Task Session_OpenedAndClosedOnLiveConnection_CompletesWithoutError()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));

        // Act + Assert (no exception means success)
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
    public async Task DisposeAsync_ListenerWithNoClients_ReleasesPortForRebinding()
    {
        // Arrange
        var harness = await TestListenerHarness.StartAsync();
        var port = harness.Port;

        // Act
        await harness.DisposeAsync();
        var harness2 = await TestListenerHarness.StartAsync(o => o.Port = port);
        await harness2.DisposeAsync();

        // Assert: rebinding succeeded — implicit, would have thrown otherwise.
    }
}
