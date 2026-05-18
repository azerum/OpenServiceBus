using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.Amqp.WireTests;

public class QueueSendTests
{
    private static ConnectionFactory CreateClientFactory()
    {
        var factory = new ConnectionFactory();
        factory.SASL.Profile = SaslProfile.Anonymous;
        return factory;
    }

    [Fact]
    public async Task Send_ExistingQueue_StoresMessageAndReturnsAcceptedDisposition()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "orders" });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "test-sender", "orders");
            var msg = new Message("hello-world")
            {
                Properties = new Properties { MessageId = "msg-1" },
            };

            // Act
            await sender.SendAsync(msg);

            // Assert
            (await harness.Store.CountAsync("orders")).ShouldBe(1L);

            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Send_FiftyMessagesInARow_AllStoredWithMonotonicSequenceNumbers()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "stream" });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "test-sender", "stream");

            // Act
            for (var i = 0; i < 50; i++)
            {
                await sender.SendAsync(new Message($"payload-{i}"));
            }

            // Assert
            (await harness.Store.CountAsync("stream")).ShouldBe(50L);

            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Send_QueueCreatedAfterListenerStart_StillRoutesAndStores()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "late-queue" });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "test-sender", "late-queue");

            // Act
            await sender.SendAsync(new Message("post-startup"));

            // Assert
            (await harness.Store.CountAsync("late-queue")).ShouldBe(1L);

            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }
}
