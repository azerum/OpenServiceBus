using OpenServiceBus.Core.Filters;

namespace OpenServiceBus.Core.Entities;

/// <summary>
/// A single rule attached to a subscription. The filter decides whether a message published
/// to the parent topic flows into the subscription. Actions (e.g. property rewriting) are
/// noted for compatibility but not yet evaluated - see M13 follow-ups.
/// </summary>
public sealed record RuleDescriptor
{
    public required string SubscriptionName { get; init; }
    public required string TopicName { get; init; }

    /// <summary>Service Bus's default rule on a fresh subscription is named <c>$Default</c>.</summary>
    public required string Name { get; init; }

    public required RuleFilter Filter { get; init; }

    /// <summary>Backing-queue address of the owning subscription.</summary>
    public string BackingQueueName => EntityNames.SubscriptionAddress(TopicName, SubscriptionName);
}
