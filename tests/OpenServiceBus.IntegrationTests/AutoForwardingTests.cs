using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// M16 SDK gate: auto-forwarding is invisible to clients. Sends to a forwarding source land
/// at the configured destination; dead-letters on a forwarding source go to the configured
/// DLQ target; the source entity itself never accumulates messages.
/// </summary>
public class AutoForwardingTests
{
    [Fact]
    public async Task SendMessageAsync_QueueWithForwardTo_LandsAtTargetNotSource()
    {
        // Arrange - source forwards every message to target.
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "fwd-target" });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "fwd-source", ForwardTo = "fwd-target" });
        await using var client = new ServiceBusClient(harness.ConnectionString);

        // Act
        var sender = client.CreateSender("fwd-source");
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { MessageId = "m1" });

        // Assert - source stays empty, target sees the message.
        (await harness.Store.CountAsync("fwd-source")).ShouldBe(0L, "the forwarding source must not hold messages");
        (await harness.Store.CountAsync("fwd-target")).ShouldBe(1L, "the message landed at the forward target");

        var receiver = client.CreateReceiver("fwd-target");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();
        msg.MessageId.ShouldBe("m1");
        msg.Body.ToString().ShouldBe("hello");
    }

    [Fact]
    public async Task DeadLetterMessageAsync_QueueWithForwardDlqTo_LandsAtTargetDlq()
    {
        // Arrange - when source's DLQ would fire, route to a configured DLQ destination instead.
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "dlq-target" });
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "dlq-source",
            ForwardDeadLetteredMessagesTo = "dlq-target",
        });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("dlq-source");
        var receiver = client.CreateReceiver("dlq-source");

        // Act
        await sender.SendMessageAsync(new ServiceBusMessage("bad") { MessageId = "to-die" });
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        received.ShouldNotBeNull();
        await receiver.DeadLetterMessageAsync(received, deadLetterReason: "boom", deadLetterErrorDescription: "manual");

        // Assert - the source's own /$DeadLetterQueue stays empty; the configured target gets it.
        (await harness.Store.CountAsync("dlq-source/$DeadLetterQueue")).ShouldBe(0L,
            "the local DLQ must be bypassed when ForwardDeadLetteredMessagesTo is set");
        (await harness.Store.CountAsync("dlq-target")).ShouldBe(1L,
            "dead-letters land at the configured forward target");

        var dlqRcv = client.CreateReceiver("dlq-target");
        var dlqMsg = await dlqRcv.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        dlqMsg.ShouldNotBeNull();
        dlqMsg.DeadLetterReason.ShouldBe("boom");
        dlqMsg.DeadLetterSource.ShouldBe("dlq-source", "the dead-letter-source annotation records the original entity");
    }

    [Fact]
    public async Task ForwardTo_ChainOfTwoQueues_MessageWalksTheChain()
    {
        // Arrange - A → B → C
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "chain-c" });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "chain-b", ForwardTo = "chain-c" });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "chain-a", ForwardTo = "chain-b" });
        await using var client = new ServiceBusClient(harness.ConnectionString);

        // Act
        await client.CreateSender("chain-a").SendMessageAsync(new ServiceBusMessage("propagated"));

        // Assert
        (await harness.Store.CountAsync("chain-a")).ShouldBe(0L);
        (await harness.Store.CountAsync("chain-b")).ShouldBe(0L);
        (await harness.Store.CountAsync("chain-c")).ShouldBe(1L);
    }

    [Fact]
    public async Task ForwardTo_QueueToTopic_FanOutToSubscriptions()
    {
        // Arrange - queue auto-forwards to a topic; both subscriptions match (TrueFilter $Default).
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Topics.CreateTopicAsync(new TopicDescriptor { Name = "events" });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor { TopicName = "events", Name = "all-1" });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor { TopicName = "events", Name = "all-2" });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "ingress", ForwardTo = "events" });
        await using var client = new ServiceBusClient(harness.ConnectionString);

        // Act - sender attaches to the queue, broker forwards to the topic, topic fans out.
        await client.CreateSender("ingress").SendMessageAsync(new ServiceBusMessage("broadcast"));

        // Assert
        (await harness.Store.CountAsync("ingress")).ShouldBe(0L);
        (await harness.Store.CountAsync("events/Subscriptions/all-1")).ShouldBe(1L);
        (await harness.Store.CountAsync("events/Subscriptions/all-2")).ShouldBe(1L);
    }

    [Fact]
    public async Task SubscriptionForwardTo_TopicFanOut_RoutesToSubsForwardTarget()
    {
        // Arrange - events → sub 'eu' forwards to queue 'aggregate'; sub 'us' lands normally.
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Topics.CreateTopicAsync(new TopicDescriptor { Name = "events2" });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "aggregate" });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor
        {
            TopicName = "events2",
            Name = "eu",
            ForwardTo = "aggregate",
        });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor { TopicName = "events2", Name = "us" });
        await using var client = new ServiceBusClient(harness.ConnectionString);

        // Act
        await client.CreateSender("events2").SendMessageAsync(new ServiceBusMessage("fanned"));

        // Assert - 'eu' sub's backing queue stays empty (was forwarded); 'us' got the regular copy.
        (await harness.Store.CountAsync("events2/Subscriptions/eu")).ShouldBe(0L);
        (await harness.Store.CountAsync("events2/Subscriptions/us")).ShouldBe(1L);
        (await harness.Store.CountAsync("aggregate")).ShouldBe(1L);
    }

    [Fact]
    public async Task CreateAsync_SelfReferencingForwardTo_Rejected()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        var ex = await Should.ThrowAsync<InvalidOperationException>(
            harness.Queues.CreateAsync(new QueueDescriptor { Name = "loop", ForwardTo = "loop" }));
        ex.Message.ShouldContain("cannot forward to itself");
    }

    [Fact]
    public async Task ForwardTo_CycleExceedsDepthCap_MessageDropped()
    {
        // Arrange - a → b, b → a is a runtime cycle. The 4-hop cap drops the message rather
        // than recursing forever. Note: we install b first with no ForwardTo (which lets
        // creation succeed), then mutate it via the registry's idempotent-replace path.
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "ring-a", ForwardTo = "ring-b" });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "ring-b" });
        // Sneak in the back-reference by direct construction - going through CreateAsync
        // would self-loop-protect us, but ring-b → ring-a is allowed (only same-name rejected).
        await harness.Queues.DeleteAsync("ring-b");
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "ring-b", ForwardTo = "ring-a" });
        await using var client = new ServiceBusClient(harness.ConnectionString);

        // Act
        await client.CreateSender("ring-a").SendMessageAsync(new ServiceBusMessage("doomed"));

        // Assert - both ends stay empty: the router drops at MaxForwardDepth.
        (await harness.Store.CountAsync("ring-a")).ShouldBe(0L);
        (await harness.Store.CountAsync("ring-b")).ShouldBe(0L);
    }
}
