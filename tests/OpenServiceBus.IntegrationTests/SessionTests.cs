using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// The Azure SDK's session receiver claims a session over AMQP, receives only
/// messages belonging to that session in order, round-trips per-session state, renews the
/// session lock, and releases on dispose so another receiver can take over.
/// </summary>
public class SessionTests
{
    [Fact]
    public async Task CreateSessionReceiverAsync_SpecificSession_OnlyReceivesMessagesWithMatchingSessionId()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "orders", RequiresSession = true });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("orders");

        await sender.SendMessageAsync(new ServiceBusMessage("a-1") { SessionId = "alpha", MessageId = "a-1" });
        await sender.SendMessageAsync(new ServiceBusMessage("b-1") { SessionId = "beta", MessageId = "b-1" });
        await sender.SendMessageAsync(new ServiceBusMessage("a-2") { SessionId = "alpha", MessageId = "a-2" });

        // Act - loop until the session drain returns null.
        var receiver = await client.AcceptSessionAsync("orders", "alpha");
        var ids = new List<string>();
        while (true)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
            if (msg is null) break;
            ids.Add(msg.MessageId);
            await receiver.CompleteMessageAsync(msg);
        }

        // Assert
        ids.ToArray().ShouldBe(new[] { "a-1", "a-2" }, "session 'alpha' messages only, in enqueue order");
    }

    [Fact]
    public async Task SetSessionStateAsync_GetSessionStateAsync_RoundTripsBlobOverAmqp()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "stateful", RequiresSession = true });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("stateful");
        await sender.SendMessageAsync(new ServiceBusMessage("hi") { SessionId = "s1" });
        var receiver = await client.AcceptSessionAsync("stateful", "s1");

        // Act
        var payload = new BinaryData(new byte[] { 1, 2, 3, 4 });
        await receiver.SetSessionStateAsync(payload);
        var read = await receiver.GetSessionStateAsync();

        // Assert
        read.ToArray().ShouldBe(new byte[] { 1, 2, 3, 4 });
    }

    [Fact]
    public async Task AcceptSessionAsync_AlreadyLocked_ThrowsSessionCannotBeLocked()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "exclusive", RequiresSession = true });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("exclusive");
        await sender.SendMessageAsync(new ServiceBusMessage("hi") { SessionId = "only-one" });
        var first = await client.AcceptSessionAsync("exclusive", "only-one");

        // Act + Assert
        var ex = await Should.ThrowAsync<ServiceBusException>(() =>
            client.AcceptSessionAsync("exclusive", "only-one"));
        ex.Reason.ShouldBe(ServiceBusFailureReason.SessionCannotBeLocked);

        // After closing the first, the session frees up.
        await first.DisposeAsync();
        for (var i = 0; i < 20; i++)
        {
            try
            {
                var second = await client.AcceptSessionAsync("exclusive", "only-one");
                await second.DisposeAsync();
                return;
            }
            catch (ServiceBusException) when (i < 19)
            {
                await Task.Delay(50);
            }
        }
        throw new Exception("releasing the first receiver didn't free the session");
    }

    [Fact]
    public async Task AcceptNextSessionAsync_TwoSessions_HandsOutEachOnceUnderConcurrentReceivers()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "fanned", RequiresSession = true });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("fanned");
        await sender.SendMessageAsync(new ServiceBusMessage("x") { SessionId = "s1" });
        await sender.SendMessageAsync(new ServiceBusMessage("y") { SessionId = "s2" });

        // Act
        var r1 = await client.AcceptNextSessionAsync("fanned");
        var r2 = await client.AcceptNextSessionAsync("fanned");

        // Assert
        new[] { r1.SessionId, r2.SessionId }.OrderBy(s => s).ToArray().ShouldBe(new[] { "s1", "s2" });
    }

    [Fact]
    public async Task RenewSessionLockAsync_ExtendsTheSessionLock()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "longwork",
            RequiresSession = true,
            LockDuration = TimeSpan.FromSeconds(30),
        });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("longwork");
        await sender.SendMessageAsync(new ServiceBusMessage("slow") { SessionId = "s" });
        var receiver = await client.AcceptSessionAsync("longwork", "s");
        var before = receiver.SessionLockedUntil;

        // Act
        await receiver.RenewSessionLockAsync();

        // Assert
        receiver.SessionLockedUntil.ShouldBeGreaterThan(before, "renew must push the session lock forward");
    }

    [Fact]
    public async Task SendAndReceive_PlainQueue_StillWorksWithSessionInfrastructurePresent()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "regression" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("regression");
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { MessageId = "id-1" });
        var receiver = client.CreateReceiver("regression");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();
        msg!.MessageId.ShouldBe("id-1");
        await receiver.CompleteMessageAsync(msg);
    }
}
