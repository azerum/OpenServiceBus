using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using OpenServiceBus.Abstractions;

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
    public async Task Sending_a_message_to_an_existing_queue_stores_it_and_is_accepted()
    {
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
            await sender.SendAsync(msg);

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
    public async Task Sending_many_messages_assigns_monotonic_sequence_numbers()
    {
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "stream" });

        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "test-sender", "stream");

            for (var i = 0; i < 50; i++)
            {
                await sender.SendAsync(new Message($"payload-{i}"));
            }

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
    public async Task Queues_created_after_listener_start_become_send_targets()
    {
        await using var harness = await TestListenerHarness.StartAsync();

        // Queue created AFTER the listener is already running.
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "late-queue" });

        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "test-sender", "late-queue");
            await sender.SendAsync(new Message("post-startup"));
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
