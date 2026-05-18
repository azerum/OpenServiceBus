using System.Text.Json;
using System.Xml;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.Core.Configuration;

/// <summary>
/// Reads a Microsoft-emulator-compatible <c>config.json</c> from disk or a JSON string,
/// projects each declared queue onto a <see cref="QueueDescriptor"/>, and reports any
/// fields whose semantics aren't yet supported by OpenServiceBus.
/// </summary>
public static class EmulatorConfigLoader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    public sealed record LoadResult(IReadOnlyList<QueueDescriptor> Queues, IReadOnlyList<string> Warnings);

    public static LoadResult LoadFromFile(string path)
    {
        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Bootstrap config not found: {path}", path);
        }
        var json = File.ReadAllText(path);
        return LoadFromJson(json);
    }

    public static LoadResult LoadFromJson(string json)
    {
        var config = JsonSerializer.Deserialize<EmulatorConfig>(json, JsonOptions)
            ?? throw new InvalidDataException("Bootstrap config did not deserialize to a valid EmulatorConfig.");

        var queues = new List<QueueDescriptor>();
        var warnings = new List<string>();

        foreach (var ns in config.UserConfig.Namespaces)
        {
            if (ns.Topics is { Count: > 0 })
            {
                warnings.Add($"Namespace '{ns.Name}' declares {ns.Topics.Count} topic(s); topics are not yet supported and will be ignored.");
            }

            foreach (var q in ns.Queues)
            {
                if (string.IsNullOrWhiteSpace(q.Name))
                {
                    warnings.Add("Skipping queue with empty Name.");
                    continue;
                }
                queues.Add(ProjectQueue(q, warnings));
            }
        }

        return new LoadResult(queues, warnings);
    }

    private static QueueDescriptor ProjectQueue(QueueConfig q, List<string> warnings)
    {
        var props = q.Properties ?? new QueueProperties();

        if (props.RequiresSession is true)
            warnings.Add($"Queue '{q.Name}': RequiresSession=true is not yet supported and will be treated as false.");
        if (props.RequiresDuplicateDetection is true)
            warnings.Add($"Queue '{q.Name}': RequiresDuplicateDetection=true is not yet supported and will be treated as false.");
        if (!string.IsNullOrEmpty(props.ForwardTo))
            warnings.Add($"Queue '{q.Name}': ForwardTo='{props.ForwardTo}' is not yet supported.");
        if (!string.IsNullOrEmpty(props.ForwardDeadLetteredMessagesTo))
            warnings.Add($"Queue '{q.Name}': ForwardDeadLetteredMessagesTo='{props.ForwardDeadLetteredMessagesTo}' is not yet supported.");

        var descriptor = new QueueDescriptor
        {
            Name = q.Name,
            MaxDeliveryCount = props.MaxDeliveryCount ?? new QueueDescriptor { Name = q.Name }.MaxDeliveryCount,
            LockDuration = ParseDuration(props.LockDuration, $"Queue '{q.Name}'.LockDuration", warnings)
                           ?? new QueueDescriptor { Name = q.Name }.LockDuration,
            DefaultMessageTimeToLive = ParseDuration(props.DefaultMessageTimeToLive, $"Queue '{q.Name}'.DefaultMessageTimeToLive", warnings),
            DeadLetteringOnMessageExpiration = props.DeadLetteringOnMessageExpiration ?? false,
        };

        return descriptor;
    }

    private static TimeSpan? ParseDuration(string? value, string fieldLabel, List<string> warnings)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        try
        {
            return XmlConvert.ToTimeSpan(value);
        }
        catch (FormatException)
        {
            warnings.Add($"{fieldLabel}: '{value}' is not a valid ISO 8601 duration (e.g. PT1M, PT1H); ignoring.");
            return null;
        }
    }
}
