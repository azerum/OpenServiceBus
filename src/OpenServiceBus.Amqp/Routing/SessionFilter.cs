using Amqp.Framing;
using Amqp.Types;

namespace OpenServiceBus.Amqp.Routing;

/// <summary>
/// Service Bus's session filter on receiver attaches: a <c>com.microsoft:session-filter</c>
/// key in <see cref="Source.FilterSet"/> whose value carries the requested
/// <see cref="SessionId"/>. A null value means "next available session".
///
/// NOTE (M14.4): The SDK's <c>AmqpReceiver.OpenReceiverLinkAsync</c> reads additional fields
/// on the attach response that aren't currently round-tripped through AMQPNetLite's
/// ListenerLink in a way the SDK accepts; the resulting <c>SessionLockedUntil</c> dereference
/// throws. The storage layer + <c>$management</c> session ops are fully wired and exercised
/// by 12 unit tests; the SDK-level <c>AcceptSessionAsync</c> handshake needs follow-up.
/// </summary>
internal readonly record struct SessionFilter(bool IsSet, string? SessionId)
{
    public const string FilterName = "com.microsoft:session-filter";
    public static readonly Symbol FilterNameSymbol = new(FilterName);

    public static SessionFilter TryReadFromAttach(Attach attach)
    {
        if (attach.Source is not Source source || source.FilterSet is null) return new SessionFilter(false, null);
        if (!source.FilterSet.TryGetValue(FilterNameSymbol, out var raw)) return new SessionFilter(false, null);
        var value = raw switch
        {
            DescribedValue dv => dv.Value,
            _ => raw,
        };
        return new SessionFilter(true, value as string);
    }

    public static void WriteAcceptedSessionFilter(Attach attach, string sessionId, DateTimeOffset lockedUntilUtc)
    {
        if (attach.Source is not Source source) return;
        source.FilterSet ??= new Map();
        source.FilterSet[FilterNameSymbol] = sessionId;

        // CRITICAL: Microsoft.Azure.Amqp's SDK reads this property via
        //   ((RestrictedMap)Settings.Properties).TryGetValue<long>("com.microsoft:locked-until-utc", ...)
        //   ? new DateTime(num2, DateTimeKind.Utc)
        //   : DateTime.MinValue
        // i.e., it expects a *long ticks*, not an AMQP timestamp. Sending a DateTime here makes
        // TryGetValue<long> return false → DateTime.MinValue → implicit DateTimeOffset conversion
        // applies local-tz offset → ArgumentOutOfRange ("year 0 / 10000"). Send raw ticks instead.
        attach.Properties ??= new Fields();
        attach.Properties[(Symbol)"com.microsoft:locked-until-utc"] = lockedUntilUtc.UtcDateTime.Ticks;
    }
}
