using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using OpenServiceBus.Abstractions;

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
    public async Task Sender_attach_to_unknown_queue_is_rejected_with_not_found()
    {
        await using var harness = await TestListenerHarness.StartAsync();

        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);

            var ex = await Should.ThrowAsync<AmqpException>(async () =>
            {
                var sender = new SenderLink(session, "bad-sender", "does-not-exist");
                // Attach is lazy — force it by sending.
                await sender.SendAsync(new Message("x"));
            });

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
    public async Task Receiver_attach_to_unknown_queue_is_rejected_with_not_found()
    {
        await using var harness = await TestListenerHarness.StartAsync();

        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);

            var ex = await Should.ThrowAsync<AmqpException>(async () =>
            {
                var receiver = new ReceiverLink(session, "bad-receiver", "does-not-exist");
                // Force the attach by issuing a receive.
                await receiver.ReceiveAsync(TimeSpan.FromSeconds(1));
            });

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
    public async Task Attach_to_dead_letter_subresource_is_rejected_until_M5()
    {
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "orders" });

        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);

            var ex = await Should.ThrowAsync<AmqpException>(async () =>
            {
                var receiver = new ReceiverLink(session, "dlq-receiver", "orders/$DeadLetterQueue");
                await receiver.ReceiveAsync(TimeSpan.FromSeconds(1));
            });

            ex.Error.ShouldNotBeNull();
            ex.Error.Condition.ToString().ShouldBe(ErrorCode.NotImplemented);

            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }
}
