namespace OpenServiceBus.Amqp;

/// <summary>
/// Parses an AMQP link address into the entity name and the sub-resource it targets.
/// Examples:
///   <c>orders</c>                       → <c>("orders", Main)</c>
///   <c>orders/$DeadLetterQueue</c>      → <c>("orders", DeadLetterQueue)</c>
///   <c>orders/$management</c>           → <c>("orders", Management)</c>
/// </summary>
internal readonly record struct EntityAddress(string Entity, EntitySubResource SubResource)
{
    private const string DeadLetterSuffix = "/$DeadLetterQueue";
    private const string ManagementSuffix = "/$management";

    public static bool TryParse(string? address, out EntityAddress result)
    {
        result = default;
        if (string.IsNullOrWhiteSpace(address)) return false;

        // The Azure SDK prefixes entity addresses with "/" (e.g. "/orders") even though our
        // registry stores them as "orders". Strip the leading slash so both AMQPNetLite-style
        // and SDK-style addresses resolve identically.
        var normalized = address.TrimStart('/');
        if (string.IsNullOrEmpty(normalized)) return false;

        if (normalized.EndsWith(DeadLetterSuffix, StringComparison.Ordinal))
        {
            var entity = normalized[..^DeadLetterSuffix.Length];
            if (string.IsNullOrEmpty(entity)) return false;
            result = new EntityAddress(entity, EntitySubResource.DeadLetterQueue);
            return true;
        }

        if (normalized.EndsWith(ManagementSuffix, StringComparison.Ordinal))
        {
            var entity = normalized[..^ManagementSuffix.Length];
            if (string.IsNullOrEmpty(entity)) return false;
            result = new EntityAddress(entity, EntitySubResource.Management);
            return true;
        }

        result = new EntityAddress(normalized, EntitySubResource.Main);
        return true;
    }
}

internal enum EntitySubResource
{
    Main,
    DeadLetterQueue,
    Management,
}
