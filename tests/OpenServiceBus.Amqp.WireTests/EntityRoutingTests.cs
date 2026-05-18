using Amqp;
using Amqp.Sasl;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.Amqp.WireTests;

public class EntityRoutingTests
{
    private static ConnectionFactory CreateClientFactory()
    {
        var factory = new ConnectionFactory();
        factory.SASL.Profile = SaslProfile.Anonymous;
        return factory;
    }

    [Fact]
    public async Task SenderAttach_UnknownQueue_FailsWithAmqpNotFound()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);

            // Act
            var ex = await Should.ThrowAsync<AmqpException>(async () =>
            {
                var sender = new SenderLink(session, "bad-sender", "does-not-exist");
                await sender.SendAsync(new Message("x"));
            });

            // Assert
            ex.Error.ShouldNotBeNull();
            ex.Error.Condition.ToString().ShouldBe(ErrorCode.NotFound);

            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task ReceiverAttach_UnknownQueue_FailsWithAmqpNotFound()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);

            // Act
            var ex = await Should.ThrowAsync<AmqpException>(async () =>
            {
                var receiver = new ReceiverLink(session, "bad-receiver", "does-not-exist");
                await receiver.ReceiveAsync(TimeSpan.FromSeconds(1));
            });

            // Assert
            ex.Error.ShouldNotBeNull();
            ex.Error.Condition.ToString().ShouldBe(ErrorCode.NotFound);

            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task ReceiverAttach_DeadLetterSubresourceOfExistingQueue_ResolvesToDlqSibling()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "orders" });
        (await harness.Queues.GetAsync("orders/$DeadLetterQueue")).ShouldNotBeNull();
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var receiver = new ReceiverLink(session, "dlq-receiver", "orders/$DeadLetterQueue");

            // Act
            var msg = await receiver.ReceiveAsync(TimeSpan.FromSeconds(1));

            // Assert
            msg.ShouldBeNull("empty DLQ returns null, not an attach failure");
            await receiver.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }
}
