using System.Text.Json.Serialization;

namespace OpenServiceBus.Core.Configuration;

/// <summary>
/// Root document matching the Microsoft Azure Service Bus emulator's <c>config.json</c> format.
/// OpenServiceBus parses the same shape so config files can be lifted from existing emulator
/// projects without modification.
/// </summary>
public sealed class EmulatorConfig
{
    [JsonPropertyName("UserConfig")]
    public UserConfigSection UserConfig { get; set; } = new();
}

public sealed class UserConfigSection
{
    [JsonPropertyName("Namespaces")]
    public List<NamespaceConfig> Namespaces { get; set; } = [];

    [JsonPropertyName("Logging")]
    public LoggingConfig? Logging { get; set; }
}

public sealed class NamespaceConfig
{
    [JsonPropertyName("Name")]
    public string Name { get; set; } = string.Empty;

    [JsonPropertyName("Queues")]
    public List<QueueConfig> Queues { get; set; } = [];

    /// <summary>Topics are accepted but not yet honoured (post-MVP — see M13).</summary>
    [JsonPropertyName("Topics")]
    public List<TopicConfig>? Topics { get; set; }
}

public sealed class QueueConfig
{
    [JsonPropertyName("Name")]
    public string Name { get; set; } = string.Empty;

    [JsonPropertyName("Properties")]
    public QueueProperties? Properties { get; set; }
}

public sealed class QueueProperties
{
    /// <summary>ISO 8601 duration (e.g. <c>PT1M</c>) — converted to <see cref="TimeSpan"/>.</summary>
    [JsonPropertyName("LockDuration")]
    public string? LockDuration { get; set; }

    [JsonPropertyName("MaxDeliveryCount")]
    public int? MaxDeliveryCount { get; set; }

    /// <summary>ISO 8601 duration — null means messages never expire.</summary>
    [JsonPropertyName("DefaultMessageTimeToLive")]
    public string? DefaultMessageTimeToLive { get; set; }

    [JsonPropertyName("DeadLetteringOnMessageExpiration")]
    public bool? DeadLetteringOnMessageExpiration { get; set; }

    /// <summary>Accepted for compatibility — sessions not yet supported (post-MVP, M14).</summary>
    [JsonPropertyName("RequiresSession")]
    public bool? RequiresSession { get; set; }

    /// <summary>Accepted for compatibility — duplicate detection not yet supported (post-MVP, M15).</summary>
    [JsonPropertyName("RequiresDuplicateDetection")]
    public bool? RequiresDuplicateDetection { get; set; }

    [JsonPropertyName("DuplicateDetectionHistoryTimeWindow")]
    public string? DuplicateDetectionHistoryTimeWindow { get; set; }

    /// <summary>Accepted for compatibility — auto-forwarding not yet supported (post-MVP, M16).</summary>
    [JsonPropertyName("ForwardTo")]
    public string? ForwardTo { get; set; }

    [JsonPropertyName("ForwardDeadLetteredMessagesTo")]
    public string? ForwardDeadLetteredMessagesTo { get; set; }
}

public sealed class TopicConfig
{
    [JsonPropertyName("Name")]
    public string Name { get; set; } = string.Empty;
}

public sealed class LoggingConfig
{
    [JsonPropertyName("Type")]
    public string? Type { get; set; }
}
