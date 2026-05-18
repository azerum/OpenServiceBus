using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;
using OpenServiceBus.Core.Entities;
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
    public async Task Reject_WithDeadLetterErrorInfo_MovesMessageToDlqWithReasonAndDescription()
    {
        // Arrange
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
            var error = new Error(new Symbol("com.microsoft:dead-letter"))
            {
                Info = [],
            };
            error.Info[new Symbol("DeadLetterReason")] = "FraudCheckFailed";
            error.Info[new Symbol("DeadLetterErrorDescription")] = "Card flagged by external service";

            // Act
            receiver.Reject(first, error);
            await WaitForCountAsync(harness.Store, "shop", expected: 0, TimeSpan.FromSeconds(2));
            var dlqReceiver = new ReceiverLink(session, "dlq-r", "shop/$DeadLetterQueue");
            var dlqMsg = await dlqReceiver.ReceiveAsync(TimeSpan.FromSeconds(5));

            // Assert
            (await harness.Store.CountAsync("shop/$DeadLetterQueue")).ShouldBe(1L);
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
    public async Task Release_LoopPastMaxDeliveryCount_AutoDeadLettersWithMaxDeliveryCountExceededReason()
    {
        // Arrange
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

            // Act
            for (var i = 0; i < 3; i++)
            {
                var m = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
                m.ShouldNotBeNull($"attempt {i + 1}");
                m.Header?.DeliveryCount.ShouldBe((uint)i, $"attempt {i + 1} should see delivery-count {i}");
                receiver.Release(m);
            }
            await WaitForCountAsync(harness.Store, "noisy", expected: 0, TimeSpan.FromSeconds(2));
            var dlqReceiver = new ReceiverLink(session, "dlq-r", "noisy/$DeadLetterQueue");
            var dlqMsg = await dlqReceiver.ReceiveAsync(TimeSpan.FromSeconds(5));

            // Assert
            (await harness.Store.CountAsync("noisy/$DeadLetterQueue")).ShouldBe(1L);
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
