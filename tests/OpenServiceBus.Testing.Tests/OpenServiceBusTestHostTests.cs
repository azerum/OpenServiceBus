using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Testing;

namespace OpenServiceBus.Testing.Tests;

public class OpenServiceBusTestHostTests
{
    [Fact]
    public async Task StartAsync_NoOptions_BindsToFreeLoopbackPortAndReturnsEmulatorConnectionString()
    {
        // Arrange + Act
        await using var host = await OpenServiceBusTestHost.StartAsync();

        // Assert
        host.Port.ShouldBeGreaterThan(0);
        host.AmqpUri.ShouldBe($"amqp://127.0.0.1:{host.Port}");
        host.ConnectionString.ShouldContain("UseDevelopmentEmulator=true");
        host.ConnectionString.ShouldContain($":{host.Port};");
    }

    [Fact]
    public async Task SendAndReceive_AzureSdkAgainstTestHost_RoundTripsAMessage()
    {
        // Arrange
        await using var host = await OpenServiceBusTestHost.StartAsync();
        await host.CreateQueueAsync("smoke");
        await using var client = new ServiceBusClient(host.ConnectionString);
        var sender = client.CreateSender("smoke");
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { MessageId = "id-1" });
        var receiver = client.CreateReceiver("smoke");

        // Act
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        if (msg is not null) await receiver.CompleteMessageAsync(msg);

        // Assert
        msg.ShouldNotBeNull();
        msg.MessageId.ShouldBe("id-1");
        msg.Body.ToString().ShouldBe("hello");
    }

    [Fact]
    public async Task DisposeAsync_AfterStart_ReleasesPortSoItCanBeReboundImmediately()
    {
        // Arrange
        var first = await OpenServiceBusTestHost.StartAsync();
        var port = first.Port;

        // Act
        await first.DisposeAsync();
        await using var second = await OpenServiceBusTestHost.StartAsync(o => o.Port = port);

        // Assert
        second.Port.ShouldBe(port, "the freed port should be immediately bindable");
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsIdempotent()
    {
        // Arrange
        var host = await OpenServiceBusTestHost.StartAsync();

        // Act + Assert (no exception means success)
        await host.DisposeAsync();
        await host.DisposeAsync();
    }

    [Fact]
    public async Task CreateQueueAsync_DescriptorWithCustomFields_PersistsDescriptorAsProvided()
    {
        // Arrange
        await using var host = await OpenServiceBusTestHost.StartAsync();

        // Act
        var descriptor = await host.CreateQueueAsync(new QueueDescriptor
        {
            Name = "configured",
            MaxDeliveryCount = 7,
            LockDuration = TimeSpan.FromSeconds(90),
        });

        // Assert
        descriptor.Name.ShouldBe("configured");
        descriptor.MaxDeliveryCount.ShouldBe(7);
        descriptor.LockDuration.ShouldBe(TimeSpan.FromSeconds(90));
    }

    [Fact]
    public async Task StartAsync_CustomTimeProvider_IsUsedByTheBrokerStore()
    {
        // Arrange
        var fake = new FakeTimeProvider(DateTimeOffset.UtcNow);

        // Act
        await using var host = await OpenServiceBusTestHost.StartAsync(o => o.TimeProvider = fake);

        // Assert
        host.TimeProvider.ShouldBeSameAs(fake);
    }

    [Fact]
    public async Task StartAsync_ConcurrentInstances_GetDistinctPortsAndCanCoexist()
    {
        // Arrange + Act
        await using var a = await OpenServiceBusTestHost.StartAsync();
        await using var b = await OpenServiceBusTestHost.StartAsync();

        // Assert
        a.Port.ShouldNotBe(b.Port, "ephemeral port allocation must give independent fixtures different ports");
    }

    [Fact]
    public async Task StartAsync_RequireSasAuthWithCustomKey_SeedsKeyStoreAndAcceptsMatchingClient()
    {
        // Arrange
        const string keyName = "RootManageSharedAccessKey";
        const string key = "MY-TEST-KEY";
        await using var host = await OpenServiceBusTestHost.StartAsync(o =>
        {
            o.RequireSasAuth = true;
            o.SasKeyName = keyName;
            o.SasKey = key;
        });
        await host.CreateQueueAsync("authz");
        await using var client = new ServiceBusClient(host.ConnectionString);
        var sender = client.CreateSender("authz");

        // Act
        await sender.SendMessageAsync(new ServiceBusMessage("ok"));

        // Assert
        (await host.Store.CountAsync("authz")).ShouldBe(1L);
    }
}
