using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.WireTests;

public class DeadLetterTests
{
    private static ConnectionFactory CreateClientFactory()
    {
        var factory = new ConnectionFactory();
        factory.SASL.Profile = SaslProfile.Anonymous;
        return factory;
    }

    [Fact]
    public async Task Rejected_disposition_moves_the_message_to_the_DLQ_with_reason_and_description()
    {
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "shop" });

        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "s", "shop");
            await sender.SendAsync(new Message("bad-order") { Properties = new Properties { MessageId = "m-1" } });

            var receiver = new ReceiverLink(session, "r", "shop");
            var first = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
            first.ShouldNotBeNull();

            // Reject with explicit DLQ reason/description (the SDK's DeadLetter shape).
            // Error.Info is a Fields map keyed by Symbol.
            var error = new Error(new Symbol("com.microsoft:dead-letter"))
            {
                Info = [],
            };
            error.Info[new Symbol("DeadLetterReason")] = "FraudCheckFailed";
            error.Info[new Symbol("DeadLetterErrorDescription")] = "Card flagged by external service";
            receiver.Reject(first, error);

            // Message should be gone from the main queue, with one in the DLQ.
            await WaitForCountAsync(harness.Store, "shop", expected: 0, TimeSpan.FromSeconds(2));
            (await harness.Store.CountAsync("shop/$DeadLetterQueue")).ShouldBe(1L);

            var dlqReceiver = new ReceiverLink(session, "dlq-r", "shop/$DeadLetterQueue");
            var dlqMsg = await dlqReceiver.ReceiveAsync(TimeSpan.FromSeconds(5));
            dlqMsg.ShouldNotBeNull();
            dlqMsg.Properties?.MessageId.ShouldBe("m-1");
            (dlqMsg.ApplicationProperties["DeadLetterReason"] as string).ShouldBe("FraudCheckFailed");
            (dlqMsg.ApplicationProperties["DeadLetterErrorDescription"] as string).ShouldBe("Card flagged by external service");
            dlqMsg.MessageAnnotations.ShouldNotBeNull();
            dlqMsg.MessageAnnotations.Map[new Symbol("x-opt-deadletter-source")].ShouldBe("shop");

            dlqReceiver.Accept(dlqMsg);
            await dlqReceiver.CloseAsync();
            await receiver.CloseAsync();
            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Abandon_loop_past_MaxDeliveryCount_moves_message_to_the_DLQ()
    {
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "noisy", MaxDeliveryCount = 3 });

        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "s", "noisy");
            await sender.SendAsync(new Message("flaky") { Properties = new Properties { MessageId = "f-1" } });

            var receiver = new ReceiverLink(session, "r", "noisy");
            for (var i = 0; i < 3; i++)
            {
                var m = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
                m.ShouldNotBeNull($"attempt {i + 1}");
                m.Header?.DeliveryCount.ShouldBe((uint)i, $"attempt {i + 1} should see delivery-count {i}");
                receiver.Release(m);  // increments count
            }

            // After 3 releases, the 4th dequeue should trigger auto-dead-letter (count >= 3).
            await WaitForCountAsync(harness.Store, "noisy", expected: 0, TimeSpan.FromSeconds(2));
            (await harness.Store.CountAsync("noisy/$DeadLetterQueue")).ShouldBe(1L);

            var dlqReceiver = new ReceiverLink(session, "dlq-r", "noisy/$DeadLetterQueue");
            var dlqMsg = await dlqReceiver.ReceiveAsync(TimeSpan.FromSeconds(5));
            dlqMsg.ShouldNotBeNull();
            dlqMsg.Properties?.MessageId.ShouldBe("f-1");
            (dlqMsg.ApplicationProperties["DeadLetterReason"] as string).ShouldBe("MaxDeliveryCountExceeded");
            dlqMsg.MessageAnnotations.Map[new Symbol("x-opt-deadletter-source")].ShouldBe("noisy");

            dlqReceiver.Accept(dlqMsg);
            await dlqReceiver.CloseAsync();
            await receiver.CloseAsync();
            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    private static async Task WaitForCountAsync(IMessageStore store, string queue, long expected, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await store.CountAsync(queue) == expected) return;
            await Task.Delay(20);
        }
        (await store.CountAsync(queue)).ShouldBe(expected);
    }
}
