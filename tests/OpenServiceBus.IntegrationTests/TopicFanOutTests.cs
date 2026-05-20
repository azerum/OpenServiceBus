using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Filters;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// A Service Bus client publishes to a topic with three subscriptions whose
/// rules use different SQL filters; only the subscriptions whose filter matches receive the
/// message. Exercises the full topic-send → filter-evaluate → fan-out-to-backing-queue path
/// through the actual Azure SDK and real AMQP.
/// </summary>
public class TopicFanOutTests
{
    [Fact]
    public async Task SendMessageAsync_TopicWithThreeFilteredSubscriptions_OnlyMatchingSubsReceive()
    {
        // Arrange - broker, topology, and SDK client.
        await using var harness = await IntegrationHarness.StartAsync();

        await harness.Topics.CreateTopicAsync(new TopicDescriptor { Name = "events" });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor { TopicName = "events", Name = "eu" });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor { TopicName = "events", Name = "us" });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor { TopicName = "events", Name = "everything" });

        // Replace $Default on eu/us with targeted SQL filters; everything keeps the catch-all $Default.
        await harness.Topics.CreateOrReplaceRuleAsync(new RuleDescriptor
        {
            TopicName = "events",
            SubscriptionName = "eu",
            Name = "$Default",
            Filter = new SqlFilter("region = 'eu'"),
        });
        await harness.Topics.CreateOrReplaceRuleAsync(new RuleDescriptor
        {
            TopicName = "events",
            SubscriptionName = "us",
            Name = "$Default",
            Filter = new SqlFilter("region = 'us'"),
        });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("events");

        var euMsg = new ServiceBusMessage("eu-payload") { MessageId = "eu-1" };
        euMsg.ApplicationProperties["region"] = "eu";

        var usMsg = new ServiceBusMessage("us-payload") { MessageId = "us-1" };
        usMsg.ApplicationProperties["region"] = "us";

        var zaMsg = new ServiceBusMessage("za-payload") { MessageId = "za-1" };
        zaMsg.ApplicationProperties["region"] = "za";

        // Act - publish three messages, one per region.
        await sender.SendMessageAsync(euMsg);
        await sender.SendMessageAsync(usMsg);
        await sender.SendMessageAsync(zaMsg);

        // Assert - receive from each subscription and confirm the routing.
        var euIds = await DrainAsync(client, "events", "eu");
        var usIds = await DrainAsync(client, "events", "us");
        var everythingIds = await DrainAsync(client, "events", "everything");

        euIds.ShouldBe(new[] { "eu-1" });
        usIds.ShouldBe(new[] { "us-1" });
        everythingIds.OrderBy(id => id).ShouldBe(new[] { "eu-1", "us-1", "za-1" });
    }

    [Fact]
    public async Task SendMessageAsync_TopicWithNoMatchingSubscriptions_IsSilentlyDropped()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Topics.CreateTopicAsync(new TopicDescriptor { Name = "events" });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor { TopicName = "events", Name = "eu" });
        await harness.Topics.CreateOrReplaceRuleAsync(new RuleDescriptor
        {
            TopicName = "events",
            SubscriptionName = "eu",
            Name = "$Default",
            Filter = new SqlFilter("region = 'eu'"),
        });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("events");
        var noMatch = new ServiceBusMessage("never-delivered") { MessageId = "none-1" };
        noMatch.ApplicationProperties["region"] = "ap";

        // Act
        await sender.SendMessageAsync(noMatch);
        var euIds = await DrainAsync(client, "events", "eu");

        // Assert
        euIds.ShouldBeEmpty("region=ap doesn't match the eu filter and there's no other subscription");
    }

    [Fact]
    public async Task SendMessageAsync_CorrelationFilterOnSubject_RoutesByMessageSubject()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Topics.CreateTopicAsync(new TopicDescriptor { Name = "events" });
        await harness.Topics.CreateSubscriptionAsync(new SubscriptionDescriptor { TopicName = "events", Name = "orders" });
        await harness.Topics.CreateOrReplaceRuleAsync(new RuleDescriptor
        {
            TopicName = "events",
            SubscriptionName = "orders",
            Name = "$Default",
            Filter = new CorrelationFilter { Subject = "order-created" },
        });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("events");

        // Act
        await sender.SendMessageAsync(new ServiceBusMessage("payload") { MessageId = "m-1", Subject = "order-created" });
        await sender.SendMessageAsync(new ServiceBusMessage("payload") { MessageId = "m-2", Subject = "invoice-created" });
        var received = await DrainAsync(client, "events", "orders");

        // Assert
        received.ShouldBe(new[] { "m-1" });
    }

    private static async Task<List<string>> DrainAsync(ServiceBusClient client, string topic, string subscription)
    {
        var receiver = client.CreateReceiver(topic, subscription, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });
        var ids = new List<string>();
        while (true)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
            if (msg is null) break;
            ids.Add(msg.MessageId);
            await receiver.CompleteMessageAsync(msg);
        }
        return ids;
    }
}
